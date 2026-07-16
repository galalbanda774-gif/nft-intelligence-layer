"""
نظام تحليل مينتات Robinhood Chain — طبقة الذكاء (Intelligence Layer).
مبني على نفس منطق اكتشاف المشروع الأول، مع إضافتين:
  1. فلترة المينتات التي بدأت اليوم فقط (بتوقيت +3).
  2. تحليل on-chain عبر Alchemy لحساب مؤشر wash trading أولي.

هذا مشروع مستقل تمامًا (repo وRailway منفصلين) عن بوت الاكتشاف الأول.
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

from onchain import analyze_contract

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
ALCHEMY_API_KEY = os.environ["ALCHEMY_API_KEY"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
COLLECTIONS_API_BASE = "https://api.opensea.io/api/v2/collections"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
TARGET_CHAIN = "robinhood"
LOCAL_TZ = timezone(timedelta(hours=3))  # توقيتك (+3)

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
MIN_SEND_INTERVAL = 1.05
RESTART_BACKOFF_MAX = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("intel-bot")


# ---------------------------------------------------------------------------
# استعلامات OpenSea
# ---------------------------------------------------------------------------

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
        log.warning(f"[Drops API] رد غير متوقع لـ '{slug}': HTTP {resp.status_code}")
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ شبكة أثناء التحقق من '{slug}': {e}")
        return None, None


def fetch_contract_address(slug: str):
    """
    يجيب عنوان العقد من endpoint المجموعة. لو الحقل مختلف عن المتوقع،
    نسجل الاستجابة الخام عشان نصلح بسرعة بدل التخمين.
    """
    try:
        resp = requests.get(
            f"{COLLECTIONS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning(f"[Collections API] HTTP {resp.status_code} لـ '{slug}'")
            return None
        data = resp.json()
        contracts = data.get("contracts") or []
        for c in contracts:
            if (c.get("chain") or "").lower() == TARGET_CHAIN:
                addr = c.get("address")
                if addr:
                    return addr
        log.warning(f"[Collections API] ما لقيت عنوان عقد لـ '{slug}'. الاستجابة: {json.dumps(data)[:500]}")
        return None
    except Exception as e:
        log.warning(f"[Collections API] خطأ شبكة: {e}")
        return None


def has_supply_remaining(detail: dict) -> bool:
    max_supply = detail.get("max_supply")
    total_supply = detail.get("total_supply")
    if max_supply is None or total_supply is None:
        return True
    try:
        return int(total_supply) < int(max_supply)
    except (TypeError, ValueError):
        return True


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
    """يتحقق أن المرحلة الحالية بدأت اليوم بتوقيتك (+3)."""
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    start_local = start.astimezone(LOCAL_TZ)
    today_local = datetime.now(LOCAL_TZ).date()
    return start_local.date() == today_local


def wei_to_eth(value) -> float:
    try:
        return int(value) / 1e18
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# بناء رسالة التقرير
# ---------------------------------------------------------------------------

def build_report(detail: dict, stage: dict, onchain: dict) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "بدون اسم")
    slug = detail.get("collection_slug", "")
    opensea_url = detail.get("opensea_url") or f"https://opensea.io/collection/{slug}"

    price_eth = wei_to_eth(stage.get("price", "0")) if stage else 0.0
    price_line = "مجانية (Free Mint)" if price_eth == 0 else f"{price_eth:.4f} ETH"

    if onchain["confidence"] == "insufficient_data":
        risk_line = "⚪ بيانات غير كافية بعد للتحليل"
    elif onchain["wash_flag"]:
        risk_line = f"🔴 تحذير: مؤشرات wash trading (أعلى حصة محفظة: {onchain['top_wallet_share']*100:.0f}%)"
    else:
        risk_line = f"🟢 لا توجد مؤشرات تلاعب واضحة (تنوع المحافظ: {onchain['unique_ratio']*100:.0f}%)"

    sample_note = " ⚠️ عينة صغيرة، النتيجة قد تتغير" if onchain["confidence"] == "low_sample" else ""

    return (
        f"📊 <b>تحليل مينت جديد بدأ اليوم</b>\n\n"
        f"الاسم: <b>{name}</b>\n"
        f"السعر: {price_line}\n"
        f"إجمالي الصكوك حتى الآن: {onchain['total_mints']}\n"
        f"محافظ فريدة: {onchain['unique_minters']}\n"
        f"{risk_line}{sample_note}\n\n"
        f"🔗 {opensea_url}"
    )


# ---------------------------------------------------------------------------
# طابور تليجرام (نفس منطق المشروع الأول)
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[str]" = asyncio.Queue()


async def telegram_sender():
    while True:
        text = await send_queue.get()
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = await asyncio.to_thread(
                    requests.post,
                    f"{TELEGRAM_API}/sendMessage",
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    log.info("تم إرسال تقرير.")
                    break
                elif resp.status_code == 429:
                    retry_after = (resp.json().get("parameters") or {}).get("retry_after", 3)
                    await asyncio.sleep(retry_after + 1)
                else:
                    log.error(f"فشل إرسال تليجرام ({resp.status_code}): {resp.text[:200]}")
                    if attempt >= 3:
                        break
                    await asyncio.sleep(2 * attempt)
            except Exception as e:
                log.error(f"خطأ اتصال بتليجرام (محاولة {attempt}): {e}")
                if attempt >= 3:
                    break
                await asyncio.sleep(2 * attempt)

        send_queue.task_done()
        await asyncio.sleep(MIN_SEND_INTERVAL)


def enqueue_message(text: str):
    send_queue.put_nowait(text)


# ---------------------------------------------------------------------------
# التحقق + الفلترة الزمنية + التحليل on-chain (بالخلفية)
# ---------------------------------------------------------------------------

async def verify_and_analyze(slug: str, notified: set, known_external: set, checking: set):
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)

        if found is None:
            log.info(f"↺ '{slug}': خطأ مؤقت، سيُعاد المحاولة لاحقًا.")
            return
        if found is False:
            known_external.add(slug)
            log.info(f"⚡ '{slug}': ليس دروب رسمي — تم تجاهله.")
            return
        if not detail.get("is_minting"):
            known_external.add(slug)
            return
        if not has_supply_remaining(detail):
            known_external.add(slug)
            return

        stages = detail.get("stages") or []
        now = datetime.now(timezone.utc)
        stage = find_active_stage(stages, now)

        if not stage or not started_today_local(stage):
            known_external.add(slug)
            log.info(f"⏭️ '{slug}': المرحلة لم تبدأ اليوم بتوقيتك — تم تجاهله.")
            return

        contract_address = await asyncio.to_thread(fetch_contract_address, slug)
        if not contract_address:
            log.warning(f"⏭️ '{slug}': تعذّر تحديد عنوان العقد — تم تخطي التحليل on-chain.")
            return

        onchain = await asyncio.to_thread(analyze_contract, contract_address, ALCHEMY_API_KEY)

        notified.add(slug)
        enqueue_message(build_report(detail, stage, onchain))
        log.info(f"✅ '{slug}': تم التحليل والإرسال.")

    except Exception as e:
        log.error(f"خطأ غير متوقع أثناء تحليل '{slug}': {e}")
    finally:
        checking.discard(slug)


# ---------------------------------------------------------------------------
# الاتصال بـ OpenSea Stream
# ---------------------------------------------------------------------------

async def listen_opensea():
    msg_ref = 0
    notified: set[str] = set()
    known_external: set[str] = set()
    checking: set[str] = set()

    while True:
        try:
            async with websockets.connect(
                STREAM_URL, ping_interval=None, open_timeout=15
            ) as ws:
                log.info("متصل بـ OpenSea Stream — الاشتراك في كل المجموعات...")
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
                    elif isinstance(parsed, dict):
                        event_name = parsed.get("event")
                        payload_wrapper = parsed.get("payload")
                    else:
                        continue

                    if event_name == "phx_reply":
                        status = (payload_wrapper or {}).get("status")
                        if status == "error":
                            log.error(f"[تشخيص] خطأ اشتراك: {payload_wrapper}")
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain = (item.get("chain", {}) or {}).get("name", "")
                    if chain != TARGET_CHAIN:
                        continue

                    from_address = ((payload.get("from_account") or {})
                                     .get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug or slug in notified or slug in known_external or slug in checking:
                        continue

                    checking.add(slug)
                    asyncio.create_task(verify_and_analyze(slug, notified, known_external, checking))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
            await asyncio.sleep(5)


async def run():
    enqueue_message("✅ نظام تحليل المينتات (Intelligence Layer) اشتغل الآن.")
    await asyncio.gather(listen_opensea(), telegram_sender())


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}. إعادة التشغيل خلال {backoff} ثانية...")
            time.sleep(backoff)
            backoff = min(backoff * 2, RESTART_BACKOFF_MAX)
            continue
        else:
            break


if __name__ == "__main__":
    main()
