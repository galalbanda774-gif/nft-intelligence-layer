"""
تحليل on-chain لعقد مينت محدد عبر Alchemy (Robinhood Chain).
يحسب مؤشرات: عدد الصكوك، المحافظ الفريدة، وتركّز الملكية (concentration)
كمقياس أولي لاحتمال الـ wash trading.
"""

import logging
import requests

log = logging.getLogger("onchain")

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def alchemy_base_url(api_key: str) -> str:
    return f"https://robinhood-mainnet.g.alchemy.com/v2/{api_key}"


def fetch_mint_transfers(contract_address: str, api_key: str, max_count_hex: str = "0x3e8"):
    """
    يجيب كل تحويلات المينت (from = zero address) لعقد معيّن عبر Transfers API.
    يرجع قائمة transfers أو [] عند الفشل (لا يوقف البرنامج).
    """
    url = alchemy_base_url(api_key)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [{
            "fromAddress": ZERO_ADDRESS,
            "contractAddresses": [contract_address],
            "category": ["erc721", "erc1155"],
            "withMetadata": False,
            "excludeZeroValue": False,
            "maxCount": max_count_hex,
            "order": "asc",
        }],
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            log.warning(f"[Alchemy] رد غير متوقع: HTTP {resp.status_code} - {resp.text[:200]}")
            return []
        data = resp.json()
        if "error" in data:
            log.warning(f"[Alchemy] خطأ بالاستعلام: {data['error']}")
            return []
        return data.get("result", {}).get("transfers", [])
    except Exception as e:
        log.warning(f"[Alchemy] خطأ اتصال: {e}")
        return []


def analyze_concentration(transfers: list) -> dict:
    """
    يحسب مؤشرات بسيطة من قائمة تحويلات المينت:
    - total_mints: عدد عمليات الصك الكلي
    - unique_minters: عدد المحافظ الفريدة اللي صكّت
    - top_wallet_share: أعلى نسبة صكوك لمحفظة واحدة
    - wash_flag: تحذير أولي لو التركّز مرتفع أو تنوع المحافظ منخفض
    """
    total_mints = len(transfers)
    if total_mints == 0:
        return {
            "total_mints": 0, "unique_minters": 0,
            "top_wallet_share": 0.0, "unique_ratio": 0.0,
            "wash_flag": False, "confidence": "insufficient_data",
        }

    wallet_counts = {}
    for t in transfers:
        to_addr = (t.get("to") or "").lower()
        if not to_addr:
            continue
        wallet_counts[to_addr] = wallet_counts.get(to_addr, 0) + 1

    unique_minters = len(wallet_counts)
    top_wallet_count = max(wallet_counts.values()) if wallet_counts else 0
    top_wallet_share = top_wallet_count / total_mints
    unique_ratio = unique_minters / total_mints

    # عتبات أولية (قابلة للتعديل بعد ما نجمع بيانات حقيقية من نظامك)
    wash_flag = (top_wallet_share > 0.15) or (unique_ratio < 0.5)

    return {
        "total_mints": total_mints,
        "unique_minters": unique_minters,
        "top_wallet_share": round(top_wallet_share, 3),
        "unique_ratio": round(unique_ratio, 3),
        "wash_flag": wash_flag,
        "confidence": "ok" if total_mints >= 10 else "low_sample",
    }


def analyze_contract(contract_address: str, api_key: str) -> dict:
    transfers = fetch_mint_transfers(contract_address, api_key)
    result = analyze_concentration(transfers)
    log.info(
        f"[تحليل on-chain] {contract_address}: "
        f"صكوك={result['total_mints']} فريدة={result['unique_minters']} "
        f"أعلى_حصة={result['top_wallet_share']} تحذير={result['wash_flag']}"
    )
    return result
