"""A voice agent with a callyourai face.

    export LIVEKIT_URL=... LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=...
    export CALLYOURAI_API_KEY=... CALLYOURAI_AVATAR_ID=...
    python examples/agent.py dev

Speech-to-text, the language model and the voice run on LiveKit Inference, so no other keys are needed.
The agent is dispatched by name (``AGENT_NAME``, default ``callyourai-example``):

    lk dispatch create --room my-room --agent-name callyourai-example
"""

from __future__ import annotations

import logging
import os

from livekit.agents import Agent, AgentServer, AgentSession, JobContext, cli, inference
from livekit.agents.voice import room_io
from livekit.plugins import callyourai, silero

log = logging.getLogger("callyourai-example")
server = AgentServer()


@server.rtc_session(agent_name=os.environ.get("AGENT_NAME", "callyourai-example"))
async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()
    session = AgentSession(
        stt=inference.STT(model="deepgram/flux-general-multi"),
        llm=inference.LLM(model="google/gemma-4-31b-it"),
        tts=inference.TTS(model="cartesia/sonic-3", voice="9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"),
        vad=silero.VAD.load(),
    )
    avatar = callyourai.AvatarSession(avatar_id=os.environ["CALLYOURAI_AVATAR_ID"])
    try:
        # Before session.start: the avatar takes over the agent's audio output.
        await avatar.start(session, room=ctx.room)
    except callyourai.CallyouraiError as error:
        # error.code: insufficient_credits, no_capacity, avatar_not_ready, ...
        log.error("no avatar: %s", error)
        ctx.shutdown(reason=error.code)
        return
    ctx.add_shutdown_callback(avatar.aclose)

    await session.start(
        agent=Agent(instructions="You are a friendly assistant in a video call. Answer in one or two short sentences."),
        room=ctx.room,
        # The avatar publishes the voice; the agent must not publish it a second time.
        room_options=room_io.RoomOptions(audio_output=False),
    )
    session.generate_reply(instructions="Greet the user in one short sentence.")


if __name__ == "__main__":
    cli.run_app(server)
