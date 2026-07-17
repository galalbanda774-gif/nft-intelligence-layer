"""
النظام الكامل: اكتشاف مينت مجاني بدأ اليوم على Robinhood Chain،
التحقق من كل الضوابط عبر buyer.py، تنفيذ الشراء، وإرسال إشعار تيليجرام.
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

from buyer import get_web3, attempt_purchase

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
ALCHEMY_API_KEY = os.environ["ALCHEMY_API_KEY"]
PRIVATE_KEY = os.environ["PRIVATE_KEY"]
WALLET_ADDRESS = os.environ["WALLET_ADDRESS"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
RPC_URL = f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
TARGET_CHAIN = "robinhood"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01  # أقل من هذا = "مجاني عمليًا"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

w3 = get_web3(RPC_URL)
buy_lock = asyncio.Lock()  # يمنع تضارب nonce لو اكتشف أكتر من مينت بنفس اللحظة

_eth_price_cache = {"value": None, "ts": 0}


def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


# ---------------------------------------------------------------------------
# OpenSea
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
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


# ---------------------------------------------------------------------------
# تيليجرام
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[str]" = asyncio.Queue()


def enqueue_message(text: str):
    send_queue.put_nowait(text)


async def telegram_sender():
    while True:
        text = await send_queue.get()
        try:
            await asyncio.to_thread(
                requests.post,
                f"{TELEGRAM_API}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(1.05)


REASON_MESSAGES = {
    "balance_too_low": "الرصيد بالمحفظة منخفض جدًا — توقف النظام عن الشراء",
    "gas_too_high": "رسوم الغاز التقديرية تجاوزت الحد المسموح",
    "gas_too_high_precise": "رسوم الغاز الفعلية (بعد التقدير الدقيق) تجاوزت الحد",
    "no_fee_recipient": "تعذر تحديد عنوان الرسوم من العقد",
    "simulation_failed": "محاكاة المعاملة فشلت — على الأغلب المينت غير متاح فعليًا",
    "insufficient_funds_for_total_cost": "الرصيد لا يكفي سعر المينت + الغاز معًا",
    "tx_error": "خطأ أثناء إرسال المعاملة",
}


def build_result_message(detail: dict, result: dict, quantity: int | None) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")

    if result["success"]:
        return (
            f"✅ <b>تم الشراء بنجاح!</b>\n\n"
            f"المجموعة: <b>{name}</b>\n"
            f"الكمية: {result['quantity']}\n"
            f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
            f"معاملة: {result['tx_hash']}\n"
            f"🔗 {url}"
        )

    reason_text = REASON_MESSAGES.get(result["reason"], result["reason"])
    extra = ""
    if result["reason"] == "balance_too_low":
        extra = f"\nالرصيد الحالي: ${result.get('balance_usd', 0):.4f}"
    elif "gas_too_high" in result["reason"]:
        extra = f"\nالرسوم المقدّرة: ${result.get('gas_fee_usd', 0):.4f}"

    return f"⏭️ <b>تم تجاهل الشراء</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason_text}{extra}"


# ---------------------------------------------------------------------------
# منطق التحقق والشراء
# ---------------------------------------------------------------------------

async def evaluate_and_buy(slug: str, notified: set, known_external: set, checking: set):
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail:
            known_external.add(slug)
            return
        if not detail.get("is_minting"):
            known_external.add(slug)
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            known_external.add(slug)
            log.info(f"⏭️ '{slug}': ليس مرحلة اليوم — تم تجاهله.")
            return

        max_supply = int(detail.get("max_supply") or 0)
        total_supply = int(detail.get("total_supply") or 0)
        remaining = max_supply - total_supply
        if remaining <= 0:
            known_external.add(slug)
            return

        price_wei = int(stage.get("price", "0"))
        eth_price_usd = get_eth_price_usd()

        if not is_free_or_negligible(price_wei, eth_price_usd):
            known_external.add(slug)
            log.info(f"⏭️ '{slug}': ليس مينت مجاني ({price_wei} wei) — تم تجاهله.")
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            log.warning(f"⏭️ '{slug}': لا يوجد contract_address بالبيانات.")
            known_external.add(slug)
            return

        max_per_wallet_raw = stage.get("max_per_wallet")
        max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None

        notified.add(slug)  # نمنع محاولات مكررة على نفس المجموعة حتى لو فشل الشراء

        async with buy_lock:
            result = await asyncio.to_thread(
                attempt_purchase,
                w3, PRIVATE_KEY, WALLET_ADDRESS,
                contract_address, price_wei, max_per_wallet, remaining, eth_price_usd,
            )

        enqueue_message(build_result_message(detail, result, result.get("quantity")))
        log.info(f"{'✅' if result['success'] else '⏭️'} '{slug}': {result}")

    except Exception as e:
        log.error(f"خطأ غير متوقع بمعالجة '{slug}': {e}")
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
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("متصل بـ OpenSea Stream — النظام يراقب ويشتري تلقائيًا.")
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
                    if not slug or slug in notified or slug in known_external or slug in checking:
                        continue

                    checking.add(slug)
                    asyncio.create_task(evaluate_and_buy(slug, notified, known_external, checking))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
            await asyncio.sleep(5)


async def run():
    enqueue_message("✅ نظام الشراء التلقائي اشتغل الآن ويراقب Robinhood Chain.")
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
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
