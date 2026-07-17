"""
نسخة رصد فقط — تكتشف أول مينت يبدأ اليوم وترسل البيانات الخام الكاملة
لـ /drops/{slug} عبر تيليجرام، بدون أي شراء فعلي.
الهدف: التأكد من أسماء الحقول الحقيقية قبل بناء كود الشراء.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
TARGET_CHAIN = "robinhood"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor-only")

send_queue: "asyncio.Queue[str]" = asyncio.Queue()


def enqueue_message(text: str):
    send_queue.put_nowait(text)


async def telegram_sender():
    while True:
        text = await send_queue.get()
        try:
            # تيليجرام يحدد طول الرسالة، فنقسمها لو طويلة
            chunks = [text[i:i + 3500] for i in range(0, len(text), 3500)] or [text]
            for chunk in chunks:
                await asyncio.to_thread(
                    requests.post,
                    f"{TELEGRAM_API}/sendMessage",
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": f"<pre>{chunk}</pre>",
                        "parse_mode": "HTML",
                    },
                    timeout=10,
                )
                await asyncio.sleep(1.05)
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()


def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"خطأ شبكة: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def find_active_stage(stages: list, now):
    for st in stages:
        start = parse_iso(st.get("start_time", ""))
        end = parse_iso(st.get("end_time", ""))
        if start and end and start <= now <= end:
            return st
    return None


def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


async def inspect_and_report(slug: str, checking: set, already_reported: set):
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail:
            return
        if not detail.get("is_minting"):
            return

        stages = detail.get("stages") or []
        now = datetime.now(timezone.utc)
        stage = find_active_stage(stages, now)
        if not stage or not started_today_local(stage):
            return

        if slug in already_reported:
            return
        already_reported.add(slug)

        log.info(f"✅ '{slug}': مينت بدأ اليوم — إرسال البيانات الخام للفحص.")
        raw = json.dumps(detail, ensure_ascii=False, indent=2)
        enqueue_message(f"📦 بيانات خام لـ '{slug}':\n\n{raw}")

    except Exception as e:
        log.error(f"خطأ فحص '{slug}': {e}")
    finally:
        checking.discard(slug)


async def listen_opensea():
    msg_ref = 0
    checking: set[str] = set()
    already_reported: set[str] = set()

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("متصل بـ OpenSea Stream (وضع الرصد فقط)...")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain = (item.get("chain", {}) or {}).get("name", "")
                    if chain != TARGET_CHAIN:
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug or slug in checking or slug in already_reported:
                        continue

                    checking.add(slug)
                    asyncio.create_task(inspect_and_report(slug, checking, already_reported))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
            await asyncio.sleep(5)


async def run():
    enqueue_message("✅ وضع الرصد اشتغل — بينتظر أول مينت يبدأ اليوم لإرسال بياناته الخام.")
    await asyncio.gather(listen_opensea(), telegram_sender())


if __name__ == "__main__":
    asyncio.run(run())
