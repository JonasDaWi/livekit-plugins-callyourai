"""Give a LiveKit voice agent a face.

    from livekit.plugins import callyourai

    avatar = callyourai.AvatarSession(avatar_id="...")      # CALLYOURAI_API_KEY in the environment
    await avatar.start(session, room=ctx.room)              # before session.start(...)
    await session.start(agent=..., room=ctx.room, room_options=RoomOptions(audio_output=False))

The avatar joins the room as its own participant and publishes video plus the agent's voice, in sync.
The agent's speech goes to it as a data stream instead of being published by the agent itself.
"""

from __future__ import annotations

import os

import httpx
from livekit import api, rtc
from livekit.agents import AgentSession
from livekit.agents.types import ATTRIBUTE_PUBLISH_ON_BEHALF
from livekit.agents.voice.avatar import DataStreamAudioOutput

DEFAULT_API_URL = "https://api.callyour.ai"
MOOD_ATTRIBUTE = "callyourai.mood"
SAMPLE_RATE = 16000


class CallyouraiError(Exception):
    """The API refused a request. ``code`` is e.g. ``insufficient_credits`` or ``no_capacity``."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{message} ({code}, HTTP {status})")
        self.status, self.code, self.message = status, code, message


class AvatarSession:
    def __init__(
        self,
        *,
        avatar_id: str,
        api_key: str | None = None,
        api_url: str | None = None,
        avatar_participant_identity: str = "callyourai-avatar",
        avatar_participant_name: str = "Avatar",
        max_minutes: float | None = None,
    ) -> None:
        self._avatar_id = avatar_id
        self._api_key = api_key or os.environ.get("CALLYOURAI_API_KEY")
        if not self._api_key:
            raise ValueError("pass api_key or set CALLYOURAI_API_KEY")
        self._api_url = (api_url or os.environ.get("CALLYOURAI_API_URL") or DEFAULT_API_URL).rstrip("/")
        self._identity = avatar_participant_identity
        self._name = avatar_participant_name
        self._max_minutes = max_minutes
        self._room: rtc.Room | None = None
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
        token = (
            api.AccessToken(key, secret)
            .with_kind("agent")
            .with_identity(self._identity)
            .with_name(self._name)
            .with_grants(api.VideoGrants(room_join=True, room=room.name))
            # The avatar's tracks count as the agent's own for the user's client.
            .with_attributes({ATTRIBUTE_PUBLISH_ON_BEHALF: room.local_participant.identity})
            .to_jwt()
        )
        body = {"avatar_id": self._avatar_id, "livekit_url": url, "livekit_token": token}
        if self._max_minutes:
            body["max_minutes"] = self._max_minutes
        self.session_id = (await self._call("POST", "/v1/sessions", body))["session_id"]
        self._room = room
        agent_session.output.audio = DataStreamAudioOutput(
            room=room,
            destination_identity=self._identity,
            sample_rate=SAMPLE_RATE,
            wait_remote_track=rtc.TrackKind.KIND_VIDEO,
        )

    async def set_mood(self, mood: str) -> None:
        """The face for what the agent says next (premium avatars): neutral, friendly, happy, laughing,
        thoughtful, empathetic, surprised or serious."""
        if self._room is not None:
            await self._room.local_participant.set_attributes({MOOD_ATTRIBUTE: mood})

    async def aclose(self) -> None:
        """End the avatar session now. It also ends by itself when the agent leaves the room."""
        if self.session_id is not None:
            session_id, self.session_id = self.session_id, None
            await self._call("DELETE", f"/v1/sessions/{session_id}")
