"""The plugin asks for another worker when the avatar leaves the room on its own."""

import asyncio

from livekit.plugins import callyourai
from livekit.plugins.callyourai import avatar as module


class FakeParticipant:
    def __init__(self, identity: str) -> None:
        self.identity = identity


class FakeLocal:
    identity = "agent-1"


class FakeRoom:
    name = "room-1"
    local_participant = FakeLocal()

    def __init__(self) -> None:
        self.remote_participants: dict = {}

    def isconnected(self) -> bool:
        return True


def _avatar(calls: list, answers: list) -> callyourai.AvatarSession:
    avatar = callyourai.AvatarSession(avatar_id="a1", api_key="k")
    avatar.session_id = "s1"
    avatar._room = FakeRoom()
    avatar._livekit = ("wss://lk", "key", "secret-of-at-least-32-characters-xx")

    async def call(method, path, body=None):
        calls.append((method, path, body))
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    avatar._call = call
    return avatar


def test_a_lost_avatar_is_recovered_with_a_fresh_token_and_retries_while_no_worker_is_free(monkeypatch):
    monkeypatch.setattr(module, "RECOVER_GRACE_SECONDS", 0)
    monkeypatch.setattr(module.asyncio, "sleep", _no_sleep)
    calls: list = []
    busy = callyourai.CallyouraiError(503, "no_capacity", "busy")
    avatar = _avatar(calls, [busy, {"status": "live", "recovered": True}])
    events = []
    avatar.on("avatar_lost", lambda: events.append("lost"))
    avatar.on("avatar_recovered", lambda: events.append("recovered"))

    async def run():
        avatar._on_participant_disconnected(FakeParticipant("callyourai-avatar"))
        await avatar._recovering

    asyncio.run(run())
    assert [c[1] for c in calls] == ["/v1/sessions/s1/recover"] * 2
    assert calls[-1][2]["livekit_url"] == "wss://lk" and calls[-1][2]["livekit_token"]
    assert events == ["lost", "recovered"]


def test_other_participants_leaving_and_a_closing_session_change_nothing(monkeypatch):
    calls: list = []
    avatar = _avatar(calls, [])
    avatar._on_participant_disconnected(FakeParticipant("someone-else"))
    avatar._closing = True
    avatar._on_participant_disconnected(FakeParticipant("callyourai-avatar"))
    assert avatar._recovering is None and calls == []


def test_an_avatar_already_back_in_the_room_is_not_moved_again(monkeypatch):
    monkeypatch.setattr(module, "RECOVER_GRACE_SECONDS", 0)
    calls: list = []
    avatar = _avatar(calls, [])
    avatar._room.remote_participants["callyourai-avatar"] = FakeParticipant("callyourai-avatar")

    async def run():
        avatar._on_participant_disconnected(FakeParticipant("callyourai-avatar"))
        await avatar._recovering

    asyncio.run(run())
    assert calls == []


_real_sleep = asyncio.sleep


async def _no_sleep(_seconds):
    await _real_sleep(0)
