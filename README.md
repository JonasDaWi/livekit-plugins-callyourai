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

No account yet? Three calls. The full guide for people and coding agents is at https://api.callyour.ai/llms.txt

```bash
API=https://api.callyour.ai
curl -X POST $API/v1/signup -d '{"email":"you@example.com"}' -H 'content-type: application/json'   # -> api_key
curl -X POST $API/v1/credits/checkout -H "authorization: Bearer $KEY" -H 'content-type: application/json' -d '{"amount":10}'   # euros
curl -X POST $API/v1/avatars -H "authorization: Bearer $KEY" -H 'content-type: application/json' \
     -d '{"name":"Me","image_url":"https://example.com/me.jpg"}'                                    # -> id
```

## When every avatar is busy

By default `start` raises `CallyouraiError` with code `no_capacity` when no avatar is free. Pass `wait_seconds`
(up to 120) to wait in the queue instead:

```python
avatar = callyourai.AvatarSession(avatar_id="YOUR_AVATAR_ID", wait_seconds=60)
await session.start(agent=MyAgent(), room=ctx.room)      # the agent speaks into the room itself
await avatar.start(session, room=ctx.room)                # returns at once if queued
```

Started after `session.start` like this, the agent talks voice-only while the avatar waits, and its speech moves
to the avatar as soon as the avatar is live. Started before `session.start` (with `audio_output=False`), `start`
waits for the avatar instead. Waiting is free; billing starts when the avatar's video is live. Events:
`avatar_queued`, `avatar_joined`, `avatar_unavailable`.

## Live HD

`hd=True` renders the avatar sharper (Live HD Priority, €0.30 per minute instead of €0.10) and puts the session
ahead of the queue. An HD server may take up to a minute or two to come free, so pass `wait_seconds` with it.

To start at once and still get HD, upgrade within the call:

```python
avatar = callyourai.AvatarSession(avatar_id="YOUR_AVATAR_ID", upgrade_to_hd=True)
```

The call starts on the normal avatar. As soon as an HD server is ready, the HD avatar joins the room as a second
participant (`callyourai-avatar-hd`), and once its video is live the agent's speech switches to it between two
sentences; then the normal avatar leaves. There is no gap in video or voice: for a moment both avatars are in the
room. The normal session is billed until the HD avatar is live, the HD session from then on (one row each in
`GET /v1/usage`, linked by `upgraded_from`). Events: `avatar_upgrading`, `avatar_upgraded`,
`avatar_upgrade_failed` (no HD server came free in time; the call stays on the normal avatar).

If your frontend shows the avatar's video itself, show the participant named by the agent's attribute
`callyourai.avatar` (it changes from `callyourai-avatar` to `callyourai-avatar-hd` at the switch). Frontends that
pick the avatar with LiveKit's `useVoiceAssistant` switch when the normal avatar leaves.

Sessions are billed per second from your prepaid euro credits while the avatar's video is live. `CallyouraiError.code`
tells you why a start was refused (`insufficient_credits`, `no_capacity`, ...).

Questions: support@callyour.ai · Terms and pricing: https://callyour.ai
