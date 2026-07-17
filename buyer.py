"""
محرك الشراء التلقائي عبر عقد SeaDrop على Robinhood Chain.

يحتوي كل ضوابط الأمان بمكان واحد، بالترتيب الصحيح للتحقق:
  1. الرصيد الحالي بالمحفظة (توقف لو منخفض جدًا)
  2. رسوم الغاز الحالية (إلغاء لو مرتفعة)
  3. عنوان الرسوم المسموح (يُقرأ من العقد نفسه، بدون تخمين)
  4. تنفيذ المعاملة
"""

import logging
from web3 import Web3

log = logging.getLogger("buyer")

# ---------------------------------------------------------------------------
# ثوابت العقد
# ---------------------------------------------------------------------------

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ---------------------------------------------------------------------------
# ضوابط قابلة للتعديل
# ---------------------------------------------------------------------------

MAX_GAS_FEE_USD = 0.05          # ألغِ الشراء لو الرسوم أعلى من هذا
MIN_BALANCE_RESERVE_USD = 0.30  # توقف عن الشراء لو الرصيد أقل من هذا
FEW_THRESHOLD = 20              # حتى 20 قطعة/محفظة = اشترِ الكل
LIMITED_BUY_QTY = 5             # فوق 20 = اشترِ هذا العدد بس
GAS_ESTIMATE_FALLBACK = 300_000 # لو فشل estimate_gas، استخدم هذا كاحتياط
GAS_LIMIT_SAFETY_MARGIN = 1.2   # هامش أمان 20% فوق التقدير الفعلي


def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url))


# ---------------------------------------------------------------------------
# فحوصات ما قبل الشراء
# ---------------------------------------------------------------------------

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """يرجع رصيد المحفظة بالدولار. عند أي خطأ، يرجع 0.0 (أمان: يمنع الشراء)."""
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير أولي سريع لرسوم معاملة عادية، قبل بناء المعاملة الفعلية."""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")  # عند الشك، اعتبرها عالية جدًا ولا تشترِ


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    """يسأل عقد SeaDrop مباشرة عن عنوان الرسوم المسموح لهذا العقد تحديدًا."""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            log.warning(f"[عنوان الرسوم] لا يوجد عنوان مسموح لـ {nft_contract}")
            return None
        return recipients[0]
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """
    - max_per_wallet <= 20  => اشترِ الحد الأقصى المسموح
    - max_per_wallet > 20   => اشترِ 5 فقط (تفادي مينتات ذات كمية ضخمة)
    - max_per_wallet مجهول  => اشترِ قطعة واحدة فقط (أمان)
    """
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))


# ---------------------------------------------------------------------------
# الدالة الرئيسية: تنفذ كل الفحوصات بالترتيب، ثم الشراء إن نجحت كلها
# ---------------------------------------------------------------------------

def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
) -> dict:
    """
    ينفذ كل ضوابط الأمان بالترتيب، ثم المعاملة إن نجحت كلها.
    يرجع دائمًا dict فيه success (bool) و reason (str) لتوضيح أي فحص فشل.
    """

    # --- الفحص 1: الرصيد ---
    balance_usd = get_wallet_balance_usd(w3, wallet_address, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"[توقف] الرصيد ${balance_usd:.4f} أقل من الحد ${MIN_BALANCE_RESERVE_USD}.")
        return {
            "success": False, "reason": "balance_too_low",
            "balance_usd": balance_usd,
        }

    # --- الفحص 2: رسوم الغاز (تقدير أولي سريع) ---
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > MAX_GAS_FEE_USD:
        log.info(f"[إلغاء] رسوم الغاز ${gas_fee_usd:.4f} > الحد ${MAX_GAS_FEE_USD}.")
        return {
            "success": False, "reason": "gas_too_high",
            "gas_fee_usd": gas_fee_usd,
        }

    # --- الفحص 3: عنوان الرسوم المسموح ---
    fee_recipient = get_fee_recipient(w3, nft_contract)
    if not fee_recipient:
        return {"success": False, "reason": "no_fee_recipient"}

    # --- تحديد الكمية ---
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    # --- بناء وإرسال المعاملة ---
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        tx = contract.functions.mintPublic(
            Web3.to_checksum_address(nft_contract),
            Web3.to_checksum_address(fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS),
            quantity,
        ).build_transaction({
            "from": Web3.to_checksum_address(wallet_address),
            "value": total_value,
            "nonce": w3.eth.get_transaction_count(wallet_address, "pending"),
            "chainId": w3.eth.chain_id,
        })

        # --- الفحص 4: تقدير غاز دقيق للمعاملة الفعلية (يكتشف الأخطاء قبل الإرسال) ---
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            # لو فشل estimate_gas، غالبًا المعاملة رح تفشل أصلاً (شروط غير محققة) — ألغِ بدل التخمين
            log.error(f"[إلغاء] فشل estimate_gas — المعاملة على الأغلب رح ترفض: {e}")
            return {"success": False, "reason": "simulation_failed", "error": str(e)}

        # --- الفحص 5: إعادة حساب التكلفة الفعلية بدقة أكبر بعد معرفة الـ gas الحقيقي ---
        actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
        if actual_gas_fee_usd > MAX_GAS_FEE_USD:
            log.info(f"[إلغاء] التكلفة الفعلية ${actual_gas_fee_usd:.4f} > الحد بعد التقدير الدقيق.")
            return {
                "success": False, "reason": "gas_too_high_precise",
                "gas_fee_usd": actual_gas_fee_usd,
            }

        # --- الفحص 6: التأكد إن الرصيد يكفي فعليًا (سعر المينت + الغاز) ---
        total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
        wallet_balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        if wallet_balance_wei < total_cost_wei:
            log.warning("[إلغاء] الرصيد لا يكفي لتغطية سعر المينت + الغاز معًا.")
            return {"success": False, "reason": "insufficient_funds_for_total_cost"}

        # --- التوقيع والإرسال ---
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[شراء ناجح] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
        }

    except Exception as e:
        log.error(f"[خطأ إرسال] {e}")
        return {"success": False, "reason": "tx_error", "error": str(e)}
