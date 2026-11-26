"""upgrade_to_hd: the call starts on the normal avatar and moves to the HD avatar once that one is live."""

import asyncio

import pytest

from livekit import rtc
from livekit.plugins import callyourai
from livekit.plugins.callyourai import avatar as module


class FakeLocal:
    identity = "agent-1"

    def __init__(self) -> None:
        self.attributes: dict = {}

    async def set_attributes(self, attributes: dict) -> None:
        self.attributes.update(attributes)


class FakePub:
    kind = rtc.TrackKind.KIND_VIDEO


class FakeParticipant:
    def __init__(self) -> None:
        self.track_publications = {"v": FakePub()}


class FakeRoom:
    name = "room-1"

    def __init__(self) -> None:
        self.local_participant = FakeLocal()
        self.remote_participants: dict = {}

    def on(self, event, handler):
        pass

    def isconnected(self) -> bool:
        return True


class FakeSink:
    def __init__(self, identity: str) -> None:
        self.identity, self.cleared = identity, False

    def clear_buffer(self) -> None:
        self.cleared = True


class FakeOutput:
    def __init__(self) -> None:
        self.audio = None
        self.tail = None

    def replace_audio_tail(self, sink) -> None:
        self.tail = sink


class FakeSession:
    def __init__(self) -> None:
        self.output = FakeOutput()
        self.agent_state = "speaking"


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(module, "DataStreamAudioOutput", lambda **kw: FakeSink(kw["destination_identity"]))
    monkeypatch.setattr(module, "UPGRADE_SETTLE_SECONDS", 0)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(module.asyncio, "sleep", lambda s: real_sleep(0))


def _avatar(answers: dict, **kw):
    avatar = callyourai.AvatarSession(avatar_id="a1", api_key="k", upgrade_to_hd=True, **kw)
    calls: list = []

    async def call(method, path, body=None):
        calls.append((method, path, body))
        key = (method, path)
        value = answers[key]
        return value.pop(0) if isinstance(value, list) else value

    avatar._call = call
    avatar._token = lambda identity=None: f"tok:{identity or avatar._identity}"
    return avatar, calls


def test_the_call_moves_to_the_hd_avatar_between_two_sentences_and_the_old_session_ends():
    avatar, calls = _avatar({
        ("POST", "/v1/sessions"): [{"session_id": "s1", "status": "starting"},
                                   {"session_id": "s2", "status": "queued", "queue_position": 1}],
        ("GET", "/v1/sessions/s2"): [{"status": "queued"}, {"status": "live"}],
        ("DELETE", "/v1/sessions/s1"): {"status": "ended"},
    })
    events = []
    for name in ("avatar_upgrading", "avatar_upgraded", "avatar_upgrade_failed"):
        avatar.on(name, lambda *a, name=name: events.append(name))
    session, room = FakeSession(), FakeRoom()

    async def run():
        await avatar.start(session, room, livekit_url="wss://lk", livekit_api_key="k", livekit_api_secret="s")
        assert session.output.audio.identity == "callyourai-avatar"
        room.remote_participants["callyourai-avatar-hd"] = FakeParticipant()
        for _ in range(50):
            await asyncio.sleep(0)
        assert not avatar._upgrading.done()  # the agent is still speaking: no switch yet
        session.agent_state = "listening"
        await avatar._upgrading
        await asyncio.sleep(0)

    asyncio.run(run())
    upgrade = calls[1][2]
    assert upgrade["upgrade_from"] == "s1" and upgrade["livekit_token"] == "tok:callyourai-avatar-hd"
    assert session.output.tail.identity == "callyourai-avatar-hd" and not session.output.audio.cleared
    assert avatar.session_id == "s2" and ("DELETE", "/v1/sessions/s1", None) in calls
    assert room.local_participant.attributes["callyourai.avatar"] == "callyourai-avatar-hd"
    assert events == ["avatar_upgrading", "avatar_upgraded"]


def test_when_no_hd_server_comes_the_call_stays_on_the_normal_avatar():
    avatar, calls = _avatar({
        ("POST", "/v1/sessions"): [{"session_id": "s1", "status": "starting"},
                                   {"session_id": "s2", "status": "queued", "queue_position": 1}],
        ("GET", "/v1/sessions/s2"): {"status": "ended"},
        ("DELETE", "/v1/sessions/s2"): {"status": "ended"},
    })
    failed = []
    avatar.on("avatar_upgrade_failed", lambda error: failed.append(error))
    session, room = FakeSession(), FakeRoom()

    async def run():
        await avatar.start(session, room, livekit_url="wss://lk", livekit_api_key="k", livekit_api_secret="s")
        await avatar._upgrading

    asyncio.run(run())
    assert avatar.session_id == "s1" and session.output.audio.identity == "callyourai-avatar"
    assert ("DELETE", "/v1/sessions/s2", None) in calls and len(failed) == 1


def test_an_hd_session_is_not_upgraded_again():
    avatar = callyourai.AvatarSession(avatar_id="a1", api_key="k", hd=True, upgrade_to_hd=True)
    assert avatar._upgrade_to_hd is False
