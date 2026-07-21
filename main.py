"""
النظام الكامل: مراقبة Robinhood Chain + Ethereum Mainnet بالتوازي،
شراء تلقائي للمينتات المجانية اللي بدأت اليوم، مع إعادة محاولة
تلقائية لو الغاز كان مرتفع لحظة الاكتشاف (كل 10 ثواني لمدة دقيقتين).
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
PRIVATE_KEY = os.environ["PRIVATE_KEY"]
WALLET_ADDRESS = os.environ["WALLET_ADDRESS"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01

RETRY_INTERVAL_SECONDS = 10
RETRY_DURATION_SECONDS = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ---------------------------------------------------------------------------
# إعدادات كل شبكة — لكل شبكة RPC مستقل وحد غاز مستقل
# ---------------------------------------------------------------------------

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.05,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
    },
}

# نبني اتصال Web3 واحد لكل شبكة، جاهز بالذاكرة
W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}

# خريطة عكسية: اسم الشبكة كما يظهر بـ Stream API -> مفتاح الشبكة عندنا
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

buy_lock = asyncio.Lock()

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
    "no_fee_recipient": "تعذر تحديد عنوان الرسوم من العقد",
    "simulation_failed": "محاكاة المعاملة فشلت — على الأغلب المينت غير متاح فعليًا",
    "insufficient_funds_for_total_cost": "الرصيد لا يكفي سعر المينت + الغاز معًا",
    "tx_error": "خطأ أثناء إرسال المعاملة",
    "retry_timeout": "رسوم الغاز بقيت مرتفعة لمدة دقيقتين — تم التخلي عن المحاولة",
}


def build_result_message(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"

    if result["success"]:
        return (
            f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})\n\n"
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
    return f"⏭️ <b>تم تجاهل الشراء</b> ({chain_label})\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason_text}{extra}"


# ---------------------------------------------------------------------------
# قائمة إعادة المحاولة (لحالة الغاز المرتفع فقط)
# ---------------------------------------------------------------------------

pending_retries: dict[str, dict] = {}  # slug -> {"chain_key":..., "deadline":..., "detail":...}


async def retry_loop():
    while True:
        await asyncio.sleep(RETRY_INTERVAL_SECONDS)
        if not pending_retries:
            continue

        for slug in list(pending_retries.keys()):
            entry = pending_retries.get(slug)
            if not entry:
                continue

            chain_key = entry["chain_key"]

            # إعادة جلب بيانات الدروب (الكمية المتبقية ممكن تكون تغيرت)
            found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)
            if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                pending_retries.pop(slug, None)
                continue

            stage = fresh_detail.get("active_stage")
            if not stage:
                pending_retries.pop(slug, None)
                continue

            max_supply = int(fresh_detail.get("max_supply") or 0)
            total_supply = int(fresh_detail.get("total_supply") or 0)
            remaining = max_supply - total_supply
            if remaining <= 0:
                pending_retries.pop(slug, None)
                continue

            price_wei = int(stage.get("price", "0"))
            max_per_wallet_raw = stage.get("max_per_wallet")
            max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
            contract_address = fresh_detail.get("contract_address")

            eth_price_usd = get_eth_price_usd()
            w3 = W3_INSTANCES[chain_key]
            max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

            async with buy_lock:
                result = await asyncio.to_thread(
                    attempt_purchase,
                    w3, PRIVATE_KEY, WALLET_ADDRESS,
                    contract_address, price_wei, max_per_wallet, remaining,
                    eth_price_usd, max_gas_fee_usd,
                )

            if result["success"]:
                pending_retries.pop(slug, None)
                enqueue_message(build_result_message(fresh_detail, result, chain_key))
                log.info(f"✅ '{slug}': نجحت إعادة المحاولة بعد انخفاض الغاز.")
                continue

            if result["reason"] != "gas_too_high":
                pending_retries.pop(slug, None)
                enqueue_message(build_result_message(fresh_detail, result, chain_key))
                continue

            if time.time() > entry["deadline"]:
                pending_retries.pop(slug, None)
                enqueue_message(
                    build_result_message(
                        fresh_detail, {"success": False, "reason": "retry_timeout"}, chain_key
                    )
                )
                log.info(f"⏱️ '{slug}': انتهت مهلة إعادة المحاولة (دقيقتين) — تم التخلي.")


# ---------------------------------------------------------------------------
# منطق التحقق والشراء الأولي
# ---------------------------------------------------------------------------

async def evaluate_and_buy(slug: str, chain_key: str, notified: set, known_external: set, checking: set):
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

        notified.add(slug)

        w3 = W3_INSTANCES[chain_key]
        max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

        async with buy_lock:
            result = await asyncio.to_thread(
                attempt_purchase,
                w3, PRIVATE_KEY, WALLET_ADDRESS,
                contract_address, price_wei, max_per_wallet, remaining,
                eth_price_usd, max_gas_fee_usd,
            )

        if not result["success"] and result["reason"] == "gas_too_high":
            pending_retries[slug] = {
                "chain_key": chain_key,
                "deadline": time.time() + RETRY_DURATION_SECONDS,
                "detail": detail,
            }
            enqueue_message(
                f"⏳ <b>تأجيل شراء مؤقت</b>\n\n"
                f"المجموعة: <b>{detail.get('collection_name', slug)}</b>\n"
                f"السبب: رسوم الغاز مرتفعة حاليًا — بيعيد المحاولة كل 10 ثواني لمدة دقيقتين."
            )
            log.info(f"⏳ '{slug}': غاز مرتفع — أُضيف لقائمة إعادة المحاولة.")
            checking.discard(slug)
            return

        enqueue_message(build_result_message(detail, result, chain_key))
        log.info(f"{'✅' if result['success'] else '⏭️'} '{slug}': {result}")

    except Exception as e:
        log.error(f"خطأ غير متوقع بمعالجة '{slug}': {e}")
    finally:
        checking.discard(slug)


# ---------------------------------------------------------------------------
# الاتصال بـ OpenSea Stream — يراقب كل الشبكات المفعّلة بـ CHAIN_CONFIGS
# ---------------------------------------------------------------------------

async def listen_opensea():
    msg_ref = 0
    notified: set[str] = set()
    known_external: set[str] = set()
    checking: set[str] = set()

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب: {list(CHAIN_CONFIGS.keys())}")
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
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue  # شبكة مو مفعّلة عندنا

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug or slug in notified or slug in known_external or slug in checking:
                        continue

                    checking.add(slug)
                    asyncio.create_task(evaluate_and_buy(slug, chain_key, notified, known_external, checking))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
            await asyncio.sleep(5)


async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false — النظام متوقف عمدًا (وضع الأمان).")
        enqueue_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false) — ما رح يشتري لين تفعّله.")
        await telegram_sender()
        return

    enqueue_message(
        f"✅ نظام الشراء التلقائي اشتغل الآن — يراقب: {', '.join(CHAIN_CONFIGS.keys())}"
    )
    await asyncio.gather(listen_opensea(), retry_loop(), telegram_sender())


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
