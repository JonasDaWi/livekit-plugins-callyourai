# livekit-plugins-callyourai

Give your LiveKit voice agent a face. The avatar joins the room as its own participant and speaks with the
agent's voice, lips in sync.

```bash
pip install livekit-plugins-callyourai
```

```python
from livekit.agents import AgentSession, room_io
from livekit.plugins import callyourai

async def entrypoint(ctx):
    await ctx.connect()
    session = AgentSession(stt=..., llm=..., tts=...)
    avatar = callyourai.AvatarSession(avatar_id="YOUR_AVATAR_ID")
    await avatar.start(session, room=ctx.room)
    await session.start(agent=MyAgent(), room=ctx.room,
                        room_options=room_io.RoomOptions(audio_output=False))
```

A complete agent is in `examples/agent.py`; `examples/talk_check.py` calls it without a browser and prints
what arrived and what was billed.

Environment: `CALLYOURAI_API_KEY`, plus the usual `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`.

No account yet? Three calls (full guide for people and coding agents: https://api.callyour.ai/llms.txt):

```bash
API=https://api.callyour.ai
curl -X POST $API/v1/signup -d '{"email":"you@example.com"}' -H 'content-type: application/json'   # -> api_key
curl -X POST $API/v1/credits/checkout -H "authorization: Bearer $KEY" -H 'content-type: application/json' -d '{"amount_usd":10}'
curl -X POST $API/v1/avatars -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
     -d '{"name":"Me","image_url":"https://example.com/me.jpg"}'                                    # -> id
```

Sessions are billed per second from your credits while the avatar's video is live. `CallyouraiError.code`
tells you why a start was refused (`insufficient_credits`, `no_capacity`, ...).

Questions: support@callyour.ai · Terms and pricing: https://callyour.ai
