"""Headless check of examples/agent.py: plays the user in a call with the running agent.

Dispatches the agent into a new room, joins with a microphone track, waits for the avatar's video and the
greeting, asks a question from a WAV, interrupts the answer with a second WAV, closes the room and prints
what arrived and what was billed.

    python examples/talk_check.py --agent callyourai-example --question q.wav --interrupt i.wav
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import subprocess
import time

import httpx
import numpy as np
from livekit import api, rtc

SR = 48_000
LOUD = 600


def load(path: str) -> np.ndarray:
    out = subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-i", path, "-f", "s16le", "-ac", "1", "-ar", str(SR), "-"],
        capture_output=True, check=True,
    ).stdout  # fmt: skip
    return np.frombuffer(out, dtype=np.int16)


async def play(source: rtc.AudioSource, pcm: np.ndarray | None, seconds: float = 0.0) -> None:
    """Send ``pcm`` (or ``seconds`` of silence) as 10 ms frames in real time."""
    n = SR // 100
    if pcm is None:
        pcm = np.zeros(int(seconds * SR), np.int16)
    start = time.monotonic()
    for i in range(0, len(pcm), n):
        chunk = pcm[i : i + n]
        if len(chunk) < n:
            chunk = np.concatenate([chunk, np.zeros(n - len(chunk), np.int16)])
        await source.capture_frame(rtc.AudioFrame(chunk.tobytes(), SR, 1, n))
        delay = start + (i + n) / SR - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agent", default="callyourai-example")
    ap.add_argument("--room-prefix", default="callyourai-check-")
    ap.add_argument("--question", required=True)
    ap.add_argument("--interrupt", required=True)
    args = ap.parse_args()
    url, key, secret = (os.environ[k] for k in ("LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"))
    base = os.environ.get("CALLYOURAI_API_URL", "https://api.callyour.ai")
    headers = {"authorization": f"Bearer {os.environ['CALLYOURAI_API_KEY']}"}
    room_name = args.room_prefix + secrets.token_hex(3)
    async with httpx.AsyncClient() as http:
        before = (await http.get(f"{base}/v1/balance", headers=headers)).json()

    lk = api.LiveKitAPI(url, key, secret)
    await lk.agent_dispatch.create_dispatch(api.CreateAgentDispatchRequest(agent_name=args.agent, room=room_name))
    token = (
        api.AccessToken(key, secret).with_identity("user-check")
        .with_grants(api.VideoGrants(room_join=True, room=room_name)).to_jwt()
    )  # fmt: skip

    room = rtc.Room()
    frames: list[float] = []
    size: list[tuple[int, int]] = []
    audio: list[tuple[float, int]] = []  # arrival time, peak
    tracks: list[dict] = []
    t0 = time.monotonic()

    async def watch_video(track: rtc.Track) -> None:
        async for event in rtc.VideoStream(track):
            frames.append(time.monotonic() - t0)
            size[:] = [(event.frame.width, event.frame.height)]

    async def watch_audio(track: rtc.Track) -> None:
        async for event in rtc.AudioStream(track, sample_rate=16000, num_channels=1):
            audio.append((time.monotonic() - t0, int(np.abs(np.frombuffer(event.frame.data, np.int16)).max())))

    tasks: list[asyncio.Task] = []

    def on_track(track: rtc.Track, _pub, participant: rtc.RemoteParticipant) -> None:
        video = track.kind == rtc.TrackKind.KIND_VIDEO
        tracks.append({
            "kind": "video" if video else "audio", "from": participant.identity,
            "on_behalf_of": participant.attributes.get("lk.publish_on_behalf"),
        })  # fmt: skip
        tasks.append(asyncio.create_task((watch_video if video else watch_audio)(track)))

    room.on("track_subscribed", on_track)
    await room.connect(url, token)
    source = rtc.AudioSource(SR, 1)
    await room.local_participant.publish_track(
        rtc.LocalAudioTrack.create_audio_track("mic", source),
        rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE),
    )
    mic = asyncio.create_task(play(source, None, 600))  # silence until we speak

    def loud_since(t: float) -> float | None:
        return next((at for at, peak in audio if at >= t and peak > LOUD), None)

    def quiet_for(seconds: float) -> bool:
        now = time.monotonic() - t0
        recent = [peak for at, peak in audio if at > now - seconds]
        return bool(recent) and max(recent) < LOUD

    report: dict = {"room": room_name}
    deadline = time.monotonic() + 180
    while not frames and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    if not frames:
        report["error"] = "no video"
    else:
        report["video_after_s"] = round(frames[0], 2)
        greet = None
        while time.monotonic() < deadline and greet is None:
            greet = loud_since(0)
            await asyncio.sleep(0.1)
        report["greeting_after_s"] = None if greet is None else round(greet, 2)
        while time.monotonic() < deadline and not quiet_for(1.5):
            await asyncio.sleep(0.1)

        mic.cancel()
        await play(source, load(args.question))
        asked = time.monotonic() - t0
        mic = asyncio.create_task(play(source, None, 600))
        answer = None
        while time.monotonic() < deadline and answer is None:
            answer = loud_since(asked)
            await asyncio.sleep(0.05)
        report["response_s"] = None if answer is None else round(answer - asked, 2)
        await asyncio.sleep(2.5)  # let it talk, then interrupt
        mic.cancel()
        interrupt_at = time.monotonic() - t0
        await play(source, load(args.interrupt))
        said = time.monotonic() - t0
        mic = asyncio.create_task(play(source, None, 600))
        # When did the first answer stop? The first 0.6 s quiet window after the interruption began.
        stopped = None
        while time.monotonic() - t0 < said + 15 and stopped is None:
            await asyncio.sleep(0.1)
            times = [(at, peak) for at, peak in audio if at >= interrupt_at]
            for at, _ in times:
                window = [peak for a, peak in times if at <= a < at + 0.6]
                if window and max(window) < LOUD and times[-1][0] > at + 0.6:
                    stopped = at
                    break
        report["kept_talking_after_interrupt_s"] = None if stopped is None else round(stopped - interrupt_at, 2)
        second = None
        while time.monotonic() - t0 < said + 30 and second is None:
            second = loud_since(said + 0.3)
            await asyncio.sleep(0.05)
        report["second_response_s"] = None if second is None else round(second - said, 2)
        await asyncio.sleep(6)
        steady = [at for at in frames if at > frames[0] + 2]
        gaps = np.diff(frames)
        report["fps_received"] = round((len(steady) - 1) / (steady[-1] - steady[0]), 1) if len(steady) > 2 else None
        report["max_frame_gap_ms"] = round(float(gaps.max()) * 1000) if len(gaps) else None
        report["size"] = size[0] if size else None
        report["video_seconds"] = round(frames[-1] - frames[0], 1)
    report["tracks"] = tracks
    mic.cancel()
    await room.disconnect()
    for task in tasks:
        task.cancel()
    try:
        await lk.room.delete_room(api.DeleteRoomRequest(room=room_name))  # the agent leaves, the avatar session ends
    except api.TwirpError:
        pass
    await lk.aclose()

    await asyncio.sleep(8)
    async with httpx.AsyncClient() as http:
        after = (await http.get(f"{base}/v1/balance", headers=headers)).json()
        usage = (await http.get(f"{base}/v1/usage", headers=headers)).json()
    report["balance_before"] = before.get("balance", before.get("balance_usd"))
    report["balance_after"] = after.get("balance", after.get("balance_usd"))
    report["available_after"] = after.get("available", after.get("available_usd"))
    report["usage"] = usage
    print(json.dumps(report, indent=1), flush=True)
    return 0 if frames else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
