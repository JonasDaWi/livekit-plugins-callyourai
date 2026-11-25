"""Busy service: the plugin waits in the queue, voice-only if the agent already speaks into the room."""

import asyncio

import pytest

from livekit.plugins import callyourai
from livekit.plugins.callyourai import avatar as module


class FakeLocal:
    identity = "agent-1"


class FakeRoom:
    name = "room-1"
    local_participant = FakeLocal()

    def __init__(self) -> None:
        self.remote_participants: dict = {}
        self.handlers: dict = {}

    def on(self, event, handler):
        self.handlers[event] = handler

    def isconnected(self) -> bool:
        return True


class FakeOutput:
    def __init__(self, audio=None) -> None:
        self.audio = audio
        self.tail = None

    def replace_audio_tail(self, sink) -> None:
        self.tail = sink


class FakeSession:
    def __init__(self, audio=None) -> None:
        self.output = FakeOutput(audio)


def _avatar(answers: list, **kw) -> tuple[callyourai.AvatarSession, list]:
    avatar = callyourai.AvatarSession(avatar_id="a1", api_key="k", **kw)
    calls: list = []

    async def call(method, path, body=None):
        calls.append((method, path, body))
        return answers.pop(0)

    avatar._call = call
    avatar._token = lambda: "tok"
    return avatar, calls


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(module, "DataStreamAudioOutput", lambda **kw: ("sink", kw["destination_identity"]))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(module.asyncio, "sleep", lambda s: real_sleep(0))


def test_a_queued_session_keeps_the_agents_own_voice_until_the_avatar_is_live():
    avatar, calls = _avatar(
        [{"session_id": "s1", "status": "queued", "queue_position": 2},
         {"status": "queued"}, {"status": "starting"}, {"status": "live"}],
        wait_seconds=60,
    )
    events = []
    avatar.on("avatar_queued", lambda pos: events.append(("queued", pos)))
    avatar.on("avatar_joined", lambda: events.append(("joined",)))
    session = FakeSession(audio="room-audio")

    async def run():
        await avatar.start(session, FakeRoom(), livekit_url="wss://lk", livekit_api_key="k", livekit_api_secret="s")
        assert session.output.tail is None  # still speaking into the room itself
        await avatar._joining

    asyncio.run(run())
    assert calls[0][2]["wait_seconds"] == 60
    assert session.output.tail == ("sink", "callyourai-avatar")
    assert events == [("queued", 2), ("joined",)]


def test_without_own_audio_start_waits_and_fails_cleanly_when_nothing_comes_free():
    avatar, _ = _avatar([{"session_id": "s1", "status": "queued", "queue_position": 1}, {"status": "ended"}],
                        wait_seconds=30)
    session = FakeSession(audio=None)

    async def run():
        await avatar.start(session, FakeRoom(), livekit_url="wss://lk", livekit_api_key="k", livekit_api_secret="s")

    with pytest.raises(callyourai.CallyouraiError) as error:
        asyncio.run(run())
    assert error.value.code == "no_capacity" and session.output.audio is None


def test_a_free_avatar_attaches_at_once_and_hd_is_asked_for():
    avatar, calls = _avatar([{"session_id": "s1", "status": "starting"}], hd=True)
    session = FakeSession(audio=None)
    asyncio.run(avatar.start(session, FakeRoom(), livekit_url="wss://lk", livekit_api_key="k", livekit_api_secret="s"))
    assert calls[0][2]["hd"] is True and "wait_seconds" not in calls[0][2]
    assert session.output.audio == ("sink", "callyourai-avatar")
