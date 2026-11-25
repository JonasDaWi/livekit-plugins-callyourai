"""Give a LiveKit voice agent a face.

    from livekit.plugins import callyourai

    avatar = callyourai.AvatarSession(avatar_id="...")      # CALLYOURAI_API_KEY in the environment
    await avatar.start(session, room=ctx.room)              # before session.start(...)
    await session.start(agent=..., room=ctx.room, room_options=RoomOptions(audio_output=False))

The avatar joins the room as its own participant and publishes video plus the agent's voice, in sync.
The agent's speech goes to it as a data stream instead of being published by the agent itself.

If the server rendering the avatar fails mid-call, the plugin asks the API to put the avatar back on
another worker, under the same participant identity; the agent keeps talking. ``AvatarSession`` emits
``avatar_lost``, ``avatar_recovered`` and ``avatar_recovery_failed`` for whoever wants to know:

    avatar.on("avatar_recovered", lambda: logger.info("avatar is back"))

When every avatar is busy, ``wait_seconds`` lets the session wait in the queue instead of failing. With
``audio_while_waiting=True`` and an agent session that already publishes its own audio (call
``avatar.start`` after ``session.start`` without ``audio_output=False``), the agent talks voice-only while it
waits and its speech moves to the avatar as soon as the avatar is live. Otherwise ``start`` waits for its turn.
Events: ``avatar_queued`` (queue position), ``avatar_joined``, ``avatar_unavailable`` (the wait ran out).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx
from livekit import api, rtc
from livekit.agents import AgentSession
from livekit.agents.types import ATTRIBUTE_PUBLISH_ON_BEHALF
from livekit.agents.voice.avatar import DataStreamAudioOutput

logger = logging.getLogger("livekit.plugins.callyourai")

DEFAULT_API_URL = "https://api.callyour.ai"
# How long the avatar may be out of the room before the plugin asks for another worker (a worker that
# takes the session over on the API's own initiative usually rejoins within this time) ...
RECOVER_GRACE_SECONDS = 3.0
# ... and how long it keeps asking while no worker is free.
RECOVER_GIVE_UP_SECONDS = 90.0
MOOD_ATTRIBUTE = "callyourai.mood"
SAMPLE_RATE = 16000


class CallyouraiError(Exception):
    """The API refused a request. ``code`` is e.g. ``insufficient_credits`` or ``no_capacity``."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{message} ({code}, HTTP {status})")
        self.status, self.code, self.message = status, code, message


class AvatarSession(rtc.EventEmitter[str]):
    def __init__(
        self,
        *,
        avatar_id: str,
        api_key: str | None = None,
        api_url: str | None = None,
        avatar_participant_identity: str = "callyourai-avatar",
        avatar_participant_name: str = "Avatar",
        max_minutes: float | None = None,
        wait_seconds: int = 0,
        audio_while_waiting: bool = True,
        hd: bool = False,
    ) -> None:
        self._avatar_id = avatar_id
        self._api_key = api_key or os.environ.get("CALLYOURAI_API_KEY")
        if not self._api_key:
            raise ValueError("pass api_key or set CALLYOURAI_API_KEY")
        self._api_url = (api_url or os.environ.get("CALLYOURAI_API_URL") or DEFAULT_API_URL).rstrip("/")
        self._identity = avatar_participant_identity
        self._name = avatar_participant_name
        self._max_minutes = max_minutes
        self._wait_seconds = max(0, min(int(wait_seconds), 120))
        self._audio_while_waiting = audio_while_waiting
        self._hd = hd
        super().__init__()
        self._room: rtc.Room | None = None
        self._audio: DataStreamAudioOutput | None = None
        self._livekit: tuple[str, str, str] | None = None
        self._closing = False
        self._recovering: asyncio.Task | None = None
        self._joining: asyncio.Task | None = None
        self.session_id: str | None = None

    async def _call(self, method: str, path: str, body: dict | None = None) -> dict:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.request(
                method, self._api_url + path, json=body, headers={"authorization": f"Bearer {self._api_key}"}
            )
        if response.status_code >= 400:
            try:
                error = response.json()["error"]
                raise CallyouraiError(response.status_code, error["code"], error["message"])
            except (ValueError, KeyError, TypeError):
                raise CallyouraiError(response.status_code, "http_error", response.text[:200]) from None
        return response.json()

    async def start(
        self,
        agent_session: AgentSession,
        room: rtc.Room,
        *,
        livekit_url: str | None = None,
        livekit_api_key: str | None = None,
        livekit_api_secret: str | None = None,
    ) -> None:
        """Bring the avatar into ``room`` and route the agent's speech to it."""
        url = livekit_url or os.environ["LIVEKIT_URL"]
        key = livekit_api_key or os.environ["LIVEKIT_API_KEY"]
        secret = livekit_api_secret or os.environ["LIVEKIT_API_SECRET"]
        self._livekit = (url, key, secret)
        self._room = room
        body = {"avatar_id": self._avatar_id, "livekit_url": url, "livekit_token": self._token()}
        if self._max_minutes:
            body["max_minutes"] = self._max_minutes
        if self._wait_seconds:
            body["wait_seconds"] = self._wait_seconds
        if self._hd:
            body["hd"] = True
        answer = await self._call("POST", "/v1/sessions", body)
        self.session_id = answer["session_id"]
        room.on("participant_disconnected", self._on_participant_disconnected)
        if answer.get("status") != "queued":
            self._attach(agent_session)
            return
        logger.info("all avatars are busy; waiting in the queue",
                    extra={"session_id": self.session_id, "position": answer.get("queue_position")})  # fmt: skip
        self.emit("avatar_queued", answer.get("queue_position"))
        if self._audio_while_waiting and agent_session.output.audio is not None:
            # The agent already speaks into the room itself: keep that until the avatar is live.
            self._joining = asyncio.create_task(self._join_when_ready(agent_session))
            return
        if not await self._wait_until_started():
            raise CallyouraiError(503, "no_capacity", "No avatar came free within wait_seconds.")
        self._attach(agent_session)

    def _attach(self, agent_session: AgentSession) -> None:
        """Route the agent's speech to the avatar participant."""
        self._audio = DataStreamAudioOutput(
            room=self._room,
            destination_identity=self._identity,
            sample_rate=SAMPLE_RATE,
            wait_remote_track=rtc.TrackKind.KIND_VIDEO,
        )
        if agent_session.output.audio is None:
            agent_session.output.audio = self._audio
        else:
            # Keep wrappers such as the transcript synchronizer; swap only the sink at the end of the chain.
            agent_session.output.replace_audio_tail(self._audio)

    async def _wait_until_started(self, live: bool = False) -> bool:
        """Poll the queued session until a worker has it (or, with ``live``, until its video is live)."""
        deadline = time.monotonic() + self._wait_seconds + 30
        wanted = ("live",) if live else ("starting", "live")
        while not self._closing and self.session_id is not None and time.monotonic() < deadline:
            state = await self._call("GET", f"/v1/sessions/{self.session_id}")
            if state.get("status") in wanted:
                return True
            if state.get("status") == "ended":
                return False
            await asyncio.sleep(2)
        return False

    async def _join_when_ready(self, agent_session: AgentSession) -> None:
        try:
            if await self._wait_until_started(live=True):
                self._attach(agent_session)
                logger.info("avatar joined; the agent's speech goes to it now", extra={"session_id": self.session_id})
                self.emit("avatar_joined")
            else:
                logger.warning("no avatar came free; the agent stays voice-only", extra={"session_id": self.session_id})
                self.emit("avatar_unavailable")
        except Exception as error:  # noqa: BLE001 - the call goes on voice-only
            logger.error("waiting for the avatar failed", extra={"error": str(error)})
            self.emit("avatar_unavailable")

    def _token(self) -> str:
        """A join token for the avatar participant: fresh for every worker that takes the session."""
        assert self._livekit is not None and self._room is not None
        _, key, secret = self._livekit
        return (
            api.AccessToken(key, secret)
            .with_kind("agent")
            .with_identity(self._identity)
            .with_name(self._name)
            .with_grants(api.VideoGrants(room_join=True, room=self._room.name))
            # The avatar's tracks count as the agent's own for the user's client.
            .with_attributes({ATTRIBUTE_PUBLISH_ON_BEHALF: self._room.local_participant.identity})
            .to_jwt()
        )

    def _on_participant_disconnected(self, participant: rtc.RemoteParticipant) -> None:
        if participant.identity != self._identity or self._closing or self.session_id is None:
            return
        if self._room is None or not self._room.isconnected():
            return
        if self._recovering is None or self._recovering.done():
            self._recovering = asyncio.create_task(self._recover())

    async def _recover(self) -> None:
        """The avatar left the room on its own: get it back on another worker."""
        logger.warning("avatar left the room unexpectedly; asking for another worker",
                       extra={"session_id": self.session_id})  # fmt: skip
        self.emit("avatar_lost")
        if self._audio is not None:
            # What was sent to the old avatar will never be played: close the segment and let the agent's
            # playout finish, so its turn does not hang.
            self._audio.flush()
            self._audio.clear_buffer()
        await asyncio.sleep(RECOVER_GRACE_SECONDS)
        give_up = time.monotonic() + RECOVER_GIVE_UP_SECONDS
        while not self._closing and self.session_id is not None and self._room is not None:
            if self._identity in self._room.remote_participants:
                break  # the API already moved it
            url = self._livekit[0] if self._livekit else ""
            try:
                answer = await self._call("POST", f"/v1/sessions/{self.session_id}/recover",
                                          {"livekit_url": url, "livekit_token": self._token()})  # fmt: skip
            except CallyouraiError as error:
                if error.code == "no_capacity" and time.monotonic() < give_up:
                    await asyncio.sleep(5)
                    continue
                logger.error("avatar could not be recovered", extra={"session_id": self.session_id, "error": str(error)})
                self.emit("avatar_recovery_failed", error)
                return
            except httpx.HTTPError as error:
                if time.monotonic() < give_up:
                    await asyncio.sleep(2)
                    continue
                self.emit("avatar_recovery_failed", error)
                return
            if answer.get("status") == "ended":
                logger.info("avatar session is over; not recovering", extra={"session_id": self.session_id})
                self.emit("avatar_recovery_failed", None)
                return
            break
        else:
            return
        logger.info("avatar is being put back on another worker", extra={"session_id": self.session_id})
        self.emit("avatar_recovered")

    async def set_mood(self, mood: str) -> None:
        """The face for what the agent says next (premium avatars): neutral, friendly, happy, laughing,
        thoughtful, empathetic, surprised or serious."""
        if self._room is not None:
            await self._room.local_participant.set_attributes({MOOD_ATTRIBUTE: mood})

    async def aclose(self) -> None:
        """End the avatar session now. It also ends by itself when the agent leaves the room."""
        self._closing = True
        for task in (self._recovering, self._joining):
            if task is not None and not task.done():
                task.cancel()
        if self.session_id is not None:
            session_id, self.session_id = self.session_id, None
            await self._call("DELETE", f"/v1/sessions/{session_id}")
