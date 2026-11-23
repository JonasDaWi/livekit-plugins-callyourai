"""End-to-end check without an LLM: plays the agent (sends a WAV as its speech through the plugin's audio
output) and the user (receives the avatar), then prints what arrived and what was billed.

    CALLYOURAI_API_URL=http://127.0.0.1:8700 CALLYOURAI_API_KEY=... LIVEKIT_URL=... LIVEKIT_API_KEY=... \\
    LIVEKIT_API_SECRET=... python examples/e2e_check.py --avatar AVATAR_ID --wav speech_16k.wav
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import time
import types
import wave

import httpx
import numpy as np
from livekit import api, rtc

from livekit.plugins import callyourai


def token(room: str, identity: str, kind: str | None = None) -> str:
    t = api.AccessToken(os.environ["LIVEKIT_API_KEY"], os.environ["LIVEKIT_API_SECRET"]).with_identity(identity)
    if kind:
        t = t.with_kind(kind)
    return t.with_grants(api.VideoGrants(room_join=True, room=room)).to_jwt()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--avatar", required=True)
    ap.add_argument("--wav", required=True, help="16 kHz mono")
    ap.add_argument("--idle", type=float, default=4.0, help="seconds before and after the speech")
    args = ap.parse_args()
    url = os.environ["LIVEKIT_URL"]
    room_name = f"local-a2f-e2e-{secrets.token_hex(3)}"
    agent, user = rtc.Room(), rtc.Room()
    await agent.connect(url, token(room_name, "agent-e2e", "agent"), rtc.RoomOptions(auto_subscribe=False))
    video = {"frames": 0, "size": None, "first": None}
    audio = {"loud": 0, "frames": 0}
    started = time.monotonic()

    async def watch_video(track: rtc.Track) -> None:
        async for event in rtc.VideoStream(track):
            video["frames"] += 1
            video["size"] = (event.frame.width, event.frame.height)
            video["first"] = video["first"] or round(time.monotonic() - started, 1)

    async def watch_audio(track: rtc.Track) -> None:
        async for event in rtc.AudioStream(track, sample_rate=16000, num_channels=1):
            audio["frames"] += 1
            audio["loud"] += int(np.abs(np.frombuffer(event.frame.data, dtype=np.int16)).max() > 600)

    tasks: list[asyncio.Task] = []

    def on_track(track: rtc.Track, _pub, participant: rtc.RemoteParticipant) -> None:
        tasks.append(asyncio.create_task((watch_video if track.kind == rtc.TrackKind.KIND_VIDEO else watch_audio)(track)))

    user.on("track_subscribed", on_track)
    await user.connect(url, token(room_name, "user-e2e"))

    avatar = callyourai.AvatarSession(avatar_id=args.avatar)
    fake_session = types.SimpleNamespace(output=types.SimpleNamespace(audio=None))
    t0 = time.monotonic()
    await avatar.start(fake_session, room=agent)  # type: ignore[arg-type]
    out = fake_session.output.audio
    print(f"session {avatar.session_id} accepted after {time.monotonic() - t0:.1f} s", flush=True)
    while video["frames"] == 0 and time.monotonic() - t0 < 90:
        await asyncio.sleep(0.2)
    print(f"first video frame after {time.monotonic() - t0:.1f} s, size {video['size']}", flush=True)
    await asyncio.sleep(args.idle)
    with wave.open(args.wav) as w:
        assert w.getframerate() == 16000 and w.getnchannels() == 1
        pcm = w.readframes(w.getnframes())
    step = 16000 // 10 * 2
    for i in range(0, len(pcm), step):
        chunk = pcm[i : i + step]
        await out.capture_frame(rtc.AudioFrame(data=chunk, sample_rate=16000, num_channels=1,
                                               samples_per_channel=len(chunk) // 2))  # fmt: skip
    out.flush()
    speech = len(pcm) / 2 / 16000
    await asyncio.sleep(speech + args.idle + 3)
    seen = time.monotonic() - t0
    await agent.disconnect()  # the agent leaves: the avatar session ends by itself
    await asyncio.sleep(6)
    async with httpx.AsyncClient() as http:
        headers = {"authorization": f"Bearer {os.environ['CALLYOURAI_API_KEY']}"}
        base = os.environ["CALLYOURAI_API_URL"]
        session = (await http.get(f"{base}/v1/sessions/{avatar.session_id}", headers=headers)).json()
        balance = (await http.get(f"{base}/v1/balance", headers=headers)).json()
    await user.disconnect()
    print(json.dumps({
        "video_frames": video["frames"], "video_fps": round(video["frames"] / max(seen - (video["first"] or 0), 1), 1),
        "size": video["size"], "speech_seconds": round(speech, 1), "loud_audio_seconds": round(audio["loud"] / 100, 1),
        "session": session, "balance_usd": balance["balance_usd"], "available_usd": balance["available_usd"],
    }, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
