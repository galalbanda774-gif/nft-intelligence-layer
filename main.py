"""
النظام الكامل — محافظ متعددة + رسالة مجمّعة واحدة لكل مينت:
  - يكتشف مينتات بدأت اليوم على Robinhood + Ethereum
  - كل محفظة تحاول تشتري نسختها الخاصة من نفس المينت، بشكل مستقل تمامًا
  - نتائج كل المحافظ اللي نجحت بنفس الجولة تُجمع برسالة تيليجرام واحدة
  - أي مينت لسا معلّق (غاز مرتفع/مدفوع) يُضاف لمراقبة دائمة لكل محفظة لسا ما اشترت
  - كل محفظة لا تشتري نفس المجموعة مرتين أبدًا
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

from buyer import get_web3, attempt_purchase, get_onchain_public_price_wei

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

_wallet_addresses = [a.strip() for a in os.environ["WALLET_ADDRESSES"].split(",") if a.strip()]
_private_keys = [k.strip() for k in os.environ["PRIVATE_KEYS"].split(",") if k.strip()]

if len(_wallet_addresses) != len(_private_keys):
    raise ValueError(
        f"عدد WALLET_ADDRESSES ({len(_wallet_addresses)}) لا يطابق عدد PRIVATE_KEYS ({len(_private_keys)})"
    )

WALLETS = [{"address": a, "private_key": k} for a, k in zip(_wallet_addresses, _private_keys)]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

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

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

buy_lock = asyncio.Lock()

notified: set[tuple[str, str]] = set()  # (wallet_address, slug)
watchlist: dict[str, dict] = {}  # slug -> {"chain_key":, "detail":, "pending_wallets": set}
in_flight: set[str] = set()

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


def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


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


def short_addr(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}"


def build_consolidated_success_message(detail: dict, chain_key: str, successes: list[tuple[str, dict]]) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"

    lines = [f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})", "", f"المجموعة: <b>{name}</b>"]
    for wallet_address, result in successes:
        lines.append("")
        lines.append(f"👛 المحفظة: <code>{short_addr(wallet_address)}</code>")
        lines.append(f"الكمية: {result['quantity']}")
        lines.append(f"رسوم الغاز: ${result['gas_fee_usd']:.4f}")
        lines.append(f"معاملة: {result['tx_hash']}")
    lines.append("")
    lines.append(f"🔗 {url}")
    return "\n".join(lines)


def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


def build_reverted_message(detail: dict, chain_key: str, failures: list[tuple[str, dict]]) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    lines = [f"⚠️ <b>معاملة فشلت فعليًا (استهلكت غاز)</b> ({chain_label})", "", f"المجموعة: <b>{name}</b>"]
    for wallet_address, result in failures:
        lines.append("")
        lines.append(f"👛 المحفظة: <code>{short_addr(wallet_address)}</code>")
        lines.append(f"رسوم مدفوعة: ${result.get('gas_fee_usd', 0):.4f}")
        lines.append(f"معاملة: {result.get('tx_hash', '')}")
        lines.append("السبب: على الأغلب نفدت الكمية قبل تأكيد معاملتك")
    return "\n".join(lines)


async def try_buy_now(slug: str, chain_key: str, detail: dict, wallet: dict) -> dict | None:
    stage = detail.get("active_stage")
    if not stage:
        return None

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return {"success": False, "reason": "sold_out"}

    contract_address = detail.get("contract_address")
    if not contract_address:
        return {"success": False, "reason": "no_contract_address"}

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    if not is_free_or_negligible(price_wei, eth_price_usd):
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    key = (wallet["address"], slug)
    async with buy_lock:
        if key in notified:
            return {"success": False, "reason": "already_bought"}
        result = await asyncio.to_thread(
            attempt_purchase,
            w3, wallet["private_key"], wallet["address"],
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )
        if result["success"]:
            notified.add(key)

    return result


async def evaluate_new_mint(slug: str, chain_key: str):
    if slug in watchlist or slug in in_flight:
        return
    in_flight.add(slug)
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        pending_wallets: set[str] = set()
        successes: list[tuple[str, dict]] = []
        reverted: list[tuple[str, dict]] = []
        sold_out_hit = False

        for wallet in WALLETS:
            if (wallet["address"], slug) in notified:
                continue

            result = await try_buy_now(slug, chain_key, detail, wallet)

            if result is None:
                pending_wallets.add(wallet["address"])
                continue

            if result["success"]:
                successes.append((wallet["address"], result))
                continue

            if result["reason"] == "sold_out":
                sold_out_hit = True
                pending_wallets.clear()
                break

            if result["reason"] == "tx_reverted":
                reverted.append((wallet["address"], result))
                continue

            if result["reason"] == "balance_too_low":
                enqueue_message(
                    f"🔴 <b>تنبيه: رصيد منخفض!</b>\n\nالمحفظة: <code>{short_addr(wallet['address'])}</code>\n"
                    f"الرصيد الحالي: ${result.get('balance_usd', 0):.4f}"
                )
                continue

            pending_wallets.add(wallet["address"])

        if successes:
            enqueue_message(build_consolidated_success_message(detail, chain_key, successes))
            log.info(f"✅ '{slug}': نجح الشراء لـ {len(successes)} محفظة عند أول اكتشاف.")

        if reverted:
            enqueue_message(build_reverted_message(detail, chain_key, reverted))

        if sold_out_hit and not successes:
            return  # ما فيه داعي نراقب مجموعة خلصت كميتها

        if pending_wallets:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail, "pending_wallets": pending_wallets}
            log.info(f"👀 '{slug}': أُضيف لقائمة المراقبة لـ {len(pending_wallets)} محفظة.")

    except Exception as e:
        log.error(f"خطأ غير متوقع بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight:
                continue
            entry = watchlist.get(slug)
            if not entry:
                continue

            in_flight.add(slug)
            try:
                chain_key = entry["chain_key"]

                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    log.info(f"🔕 '{slug}': المينت لم يعد نشطًا — إزالة من المراقبة بصمت (بدون إشعار).")
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage:
                    if fresh_detail.get("next_stage"):
                        entry["detail"] = fresh_detail
                        watchlist[slug] = entry
                        continue
                    watchlist.pop(slug, None)
                    enqueue_message(build_gaveup_message(fresh_detail, "لا توجد مرحلة نشطة أو قادمة."))
                    continue

                if stage_has_ended(stage) and not fresh_detail.get("next_stage"):
                    watchlist.pop(slug, None)
                    enqueue_message(build_gaveup_message(fresh_detail, "انتهت المرحلة نهائيًا بدون فرصة شراء مناسبة."))
                    log.info(f"⏱️ '{slug}': انتهى وقت المرحلة — تم إيقاف المراقبة.")
                    continue

                still_pending: set[str] = set()
                successes: list[tuple[str, dict]] = []
                reverted: list[tuple[str, dict]] = []
                sold_out_hit = False

                for wallet_address in list(entry["pending_wallets"]):
                    if (wallet_address, slug) in notified:
                        continue
                    wallet = next((w for w in WALLETS if w["address"] == wallet_address), None)
                    if not wallet:
                        continue

                    result = await try_buy_now(slug, chain_key, fresh_detail, wallet)

                    if result is None:
                        still_pending.add(wallet_address)
                        continue

                    if result["success"]:
                        successes.append((wallet_address, result))
                        continue

                    if result["reason"] == "sold_out":
                        sold_out_hit = True
                        still_pending.clear()
                        break

                    if result["reason"] == "tx_reverted":
                        reverted.append((wallet_address, result))
                        continue

                    still_pending.add(wallet_address)

                if successes:
                    enqueue_message(build_consolidated_success_message(fresh_detail, chain_key, successes))
                    log.info(f"✅ '{slug}': نجح الشراء لـ {len(successes)} محفظة أثناء المراقبة.")

                if reverted:
                    enqueue_message(build_reverted_message(fresh_detail, chain_key, reverted))

                if sold_out_hit:
                    watchlist.pop(slug, None)
                    enqueue_message(build_gaveup_message(fresh_detail, "نفدت الكمية قبل ما نشتري."))
                    continue

                if still_pending:
                    entry["detail"] = fresh_detail
                    entry["pending_wallets"] = still_pending
                    watchlist[slug] = entry
                else:
                    watchlist.pop(slug, None)

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)


async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب: {list(CHAIN_CONFIGS.keys())} لـ {len(WALLETS)} محفظة.")
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
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

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
        f"✅ نظام الشراء التلقائي (متعدد المحافظ) اشتغل — {len(WALLETS)} محفظة، يراقب: {', '.join(CHAIN_CONFIGS.keys())}"
    )
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())


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
