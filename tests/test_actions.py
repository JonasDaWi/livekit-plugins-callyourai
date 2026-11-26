"""set_action: a one-shot gesture is the agent's participant attribute ``callyourai.action`` = "<name>:<counter>"."""

import asyncio

import pytest

from livekit.plugins import callyourai


class FakeLocal:
    def __init__(self) -> None:
        self.sets: list[dict] = []

    async def set_attributes(self, attributes: dict) -> None:
        self.sets.append(attributes)


class FakeRoom:
    def __init__(self) -> None:
        self.local_participant = FakeLocal()


def session() -> callyourai.AvatarSession:
    return callyourai.AvatarSession(avatar_id="a", api_key="k")


def test_a_repeated_action_is_a_new_attribute_value():
    avatar, room = session(), FakeRoom()
    avatar._room = room

    async def run():
        await avatar.set_action("wave")
        await avatar.set_action("wave")
        await avatar.set_action("thumbs_up")

    asyncio.run(run())
    assert room.local_participant.sets == [
        {"callyourai.action": "wave:1"}, {"callyourai.action": "wave:2"}, {"callyourai.action": "thumbs_up:3"},
    ]  # fmt: skip


def test_an_unknown_action_is_refused():
    avatar = session()
    avatar._room = FakeRoom()
    with pytest.raises(ValueError, match="one of: wave"):
        asyncio.run(avatar.set_action("nod"))
    assert avatar._room.local_participant.sets == []


def test_before_start_nothing_is_sent():
    asyncio.run(session().set_action("wave"))  # no room yet: no error


def test_the_vocabulary():
    assert set(callyourai.ACTIONS) == {"wave", "thumbs_up", "explain_hands", "look_side", "tilt_smile", "laugh",
                                       "surprised", "angry"}  # fmt: skip
