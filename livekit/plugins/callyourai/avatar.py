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

With ``upgrade_to_hd=True`` the call starts at once on the normal avatar and moves to Live HD as soon as an HD
rendering server is ready: the HD avatar joins as a second participant (``<identity>-hd``), and once its video
is live the agent's speech switches to it between two sentences and the normal avatar leaves. Whoever shows
"the avatar" should follow the agent's participant attribute ``callyourai.avatar`` (the identity of the
avatar that speaks now). Events: ``avatar_upgrading``, ``avatar_upgraded``, ``avatar_upgrade_failed``.
"""

from __future__ import annotations

import asyncio
import contextlib
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
# The agent's attribute naming the avatar participant that speaks now (changes on an upgrade to HD).
ACTIVE_AVATAR_ATTRIBUTE = "callyourai.avatar"
# Before the switch to the HD avatar: time for viewers to subscribe to its video and decode a first frame ...
UPGRADE_SETTLE_SECONDS = 1.0
# ... and the longest wait for a pause in the agent's speech (after that it switches mid-sentence).
UPGRADE_PAUSE_WAIT_SECONDS = 30.0
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
        upgrade_to_hd: bool = False,
        upgrade_wait_seconds: int = 120,
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
        self._upgrade_to_hd = upgrade_to_hd and not hd
        self._upgrade_wait_seconds = max(0, min(int(upgrade_wait_seconds), 120))
        super().__init__()
        self._room: rtc.Room | None = None
        self._audio: DataStreamAudioOutput | None = None
        self._livekit: tuple[str, str, str] | None = None
        self._closing = False
        self._recovering: asyncio.Task | None = None
        self._joining: asyncio.Task | None = None
        self._upgrading: asyncio.Task | None = None
        self._agent_session: AgentSession | None = None
        # The HD session of an upgrade in progress (None before it is created and after the switch).
        self._upgrade_session_id: str | None = None
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
        self._agent_session = agent_session
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
        if self._upgrade_to_hd:
            self._upgrading = asyncio.create_task(self._upgrade())
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
        """Route the agent's speech to the avatar participant (``self._identity``)."""
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
        self._announce_active()

    def _announce_active(self) -> None:
        """Tell the room which avatar participant speaks now (the agent's ``callyourai.avatar`` attribute)."""
        room = self._room
        if room is None or not hasattr(room.local_participant, "set_attributes"):
            return

        async def run() -> None:
            try:
                await room.local_participant.set_attributes({ACTIVE_AVATAR_ATTRIBUTE: self._identity})
            except Exception as error:  # noqa: BLE001 - only a hint for frontends
                logger.debug("could not set the active avatar attribute", extra={"error": str(error)})

        asyncio.create_task(run())

    async def _wait_until_started(self, live: bool = False, session_id: str | None = None,
                                  wait_seconds: int | None = None) -> bool:  # fmt: skip
        """Poll the queued session until a worker has it (or, with ``live``, until its video is live)."""
        session_id = session_id or self.session_id
        deadline = time.monotonic() + (self._wait_seconds if wait_seconds is None else wait_seconds) + 30
        wanted = ("live",) if live else ("starting", "live")
        while not self._closing and session_id is not None and time.monotonic() < deadline:
            state = await self._call("GET", f"/v1/sessions/{session_id}")
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

    async def _upgrade(self) -> None:
        """Bring in the HD avatar next to the normal one, then switch the agent's speech over to it."""
        assert self._room is not None and self._agent_session is not None
        hd_identity = f"{self._identity}-hd"
        old_id = self.session_id
        self.emit("avatar_upgrading")
        body = {"avatar_id": self._avatar_id, "livekit_url": self._livekit[0] if self._livekit else "",
                "livekit_token": self._token(hd_identity), "upgrade_from": old_id,
                "wait_seconds": self._upgrade_wait_seconds}  # fmt: skip
        if self._max_minutes:
            body["max_minutes"] = self._max_minutes
        try:
            answer = await self._call("POST", "/v1/sessions", body)
            self._upgrade_session_id = answer["session_id"]
            live = await self._wait_until_started(live=True, session_id=self._upgrade_session_id,
                                                  wait_seconds=self._upgrade_wait_seconds)  # fmt: skip
            if live:
                live = await self._wait_for_video(hd_identity, 20)
            if not live:
                raise CallyouraiError(503, "no_capacity", "No Live HD server came free in time.")
            # The normal avatar must be the one speaking by now (a queued start may still be waiting for it).
            while self._audio is None and self._joining is not None and not self._joining.done():
                await asyncio.sleep(0.2)
            await asyncio.sleep(UPGRADE_SETTLE_SECONDS)
            # Switch between two sentences: the old avatar has played out everything it was sent.
            deadline = time.monotonic() + UPGRADE_PAUSE_WAIT_SECONDS
            while getattr(self._agent_session, "agent_state", None) == "speaking" and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - the call goes on with the normal avatar
            logger.warning("upgrade to Live HD failed; staying on the normal avatar",
                           extra={"session_id": old_id, "error": str(error)})  # fmt: skip
            pending, self._upgrade_session_id = self._upgrade_session_id, None
            if pending is not None:
                with contextlib.suppress(Exception):
                    await self._call("DELETE", f"/v1/sessions/{pending}")
            self.emit("avatar_upgrade_failed", error)
            return
        old_audio = self._audio
        self._identity, self.session_id, self._upgrade_session_id = hd_identity, self._upgrade_session_id, None
        if old_audio is not None and getattr(self._agent_session, "agent_state", None) == "speaking":
            old_audio.clear_buffer()  # no pause came: the rest of this sentence is cut
        self._attach(self._agent_session)
        logger.info("upgraded to Live HD; the agent's speech goes to the HD avatar now",
                    extra={"session_id": self.session_id, "replaced": old_id})  # fmt: skip
        try:
            await self._call("DELETE", f"/v1/sessions/{old_id}")
        except Exception as error:  # noqa: BLE001 - it is billed no further either way
            logger.warning("could not end the replaced session", extra={"session_id": old_id, "error": str(error)})
        self.emit("avatar_upgraded")

    async def _wait_for_video(self, identity: str, timeout: float) -> bool:
        """Wait until ``identity`` is in the room and publishes video."""
        deadline = time.monotonic() + timeout
        while not self._closing and self._room is not None and time.monotonic() < deadline:
            participant = self._room.remote_participants.get(identity)
            if participant is not None and any(
                pub.kind == rtc.TrackKind.KIND_VIDEO for pub in participant.track_publications.values()
            ):
                return True
            await asyncio.sleep(0.1)
        return False

    def _token(self, identity: str | None = None) -> str:
        """A join token for the avatar participant: fresh for every worker that takes the session."""
        assert self._livekit is not None and self._room is not None
        _, key, secret = self._livekit
        return (
            api.AccessToken(key, secret)
            .with_kind("agent")
            .with_identity(identity or self._identity)
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
        for task in (self._recovering, self._joining, self._upgrading):
            if task is not None and not task.done():
                task.cancel()
        if self._upgrade_session_id is not None:
            pending, self._upgrade_session_id = self._upgrade_session_id, None
            with contextlib.suppress(Exception):
                await self._call("DELETE", f"/v1/sessions/{pending}")
        if self.session_id is not None:
            session_id, self.session_id = self.session_id, None
            await self._call("DELETE", f"/v1/sessions/{session_id}")
