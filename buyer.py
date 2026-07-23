"""
محرك الشراء التلقائي عبر عقد SeaDrop — يدعم أكتر من شبكة (Robinhood + Ethereum).
كل الضوابط الأمنية مركزة هنا بدالة واحدة.
يدعم تقليص الكمية تدريجيًا (10 -> 5 -> 2 -> 1) لو رسوم الغاز مرتفعة على الكمية الكاملة.
"""

import logging
from web3 import Web3

log = logging.getLogger("buyer")

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
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

MIN_BALANCE_RESERVE_USD = 0.30
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 5
GAS_LIMIT_SAFETY_MARGIN = 1.2


def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
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
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))


def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])  # mintPrice هو أول عنصر بالـ tuple
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة، سنعتمد بيانات OpenSea: {e}")
        return None


def build_quantity_candidates(initial_qty: int) -> list[int]:
    """
    يبني قائمة كميات تنازلية للتجربة: الكمية الكاملة، ثم نصفها، ثم نصف النصف... لحد 1.
    مثال: 10 -> [10, 5, 2, 1]
    """
    candidates = []
    q = initial_qty
    while q > 1:
        candidates.append(q)
        q = q // 2
    candidates.append(1)
    return candidates


def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """
    max_gas_fee_usd يُمرَّر من main.py حسب الشبكة (كل شبكة لها حدها الخاص).
    يجرب كميات متناقصة (10 -> 5 -> 2 -> 1) لحد ما يلقى كمية تنجح ضمن حد الغاز.
    """

    balance_usd = get_wallet_balance_usd(w3, wallet_address, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"[توقف] الرصيد ${balance_usd:.4f} أقل من الحد ${MIN_BALANCE_RESERVE_USD}.")
        return {"success": False, "reason": "balance_too_low", "balance_usd": balance_usd}

    fee_recipient = get_fee_recipient(w3, nft_contract)
    if not fee_recipient:
        return {"success": False, "reason": "no_fee_recipient"}

    initial_quantity = decide_quantity(max_per_wallet, remaining_supply)
    candidates = build_quantity_candidates(initial_quantity)

    last_gas_fee_usd = None

    for quantity in candidates:
        total_value = price_wei_per_token * quantity

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

            try:
                estimated_gas = w3.eth.estimate_gas(tx)
                tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            except Exception as e:
                log.error(f"[إلغاء] فشل estimate_gas بكمية {quantity}: {e}")
                continue  # المحاكاة فشلت لهذي الكمية — جرب الأصغر

            actual_gas_fee_usd = (tx["gas"] * w3.eth.gas_price / 1e18) * eth_price_usd
            last_gas_fee_usd = actual_gas_fee_usd

            if actual_gas_fee_usd > max_gas_fee_usd:
                log.info(
                    f"[تقليص] كمية {quantity}: رسوم ${actual_gas_fee_usd:.4f} > الحد ${max_gas_fee_usd} "
                    f"— تجربة كمية أصغر..."
                )
                continue  # جرب الكمية الأصغر التالية بالقائمة

            total_cost_wei = total_value + (tx["gas"] * w3.eth.gas_price)
            wallet_balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
            if wallet_balance_wei < total_cost_wei:
                log.warning(f"[إلغاء] الرصيد لا يكفي لكمية {quantity} (سعر + غاز).")
                continue  # جرب كمية أصغر، ممكن تكفي

            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

            log.info(f"[شراء ناجح] {tx_hash.hex()} — كمية نهائية: {quantity} (الأصلية: {initial_quantity})")
            return {
                "success": True,
                "tx_hash": tx_hash.hex(),
                "quantity": quantity,
                "gas_fee_usd": actual_gas_fee_usd,
                "total_value_wei": total_value,
            }

        except Exception as e:
            log.error(f"[خطأ إرسال] بكمية {quantity}: {e}")
            continue

    # جربنا كل الكميات ولا وحدة نجحت
    return {
        "success": False,
        "reason": "gas_too_high",
        "gas_fee_usd": last_gas_fee_usd if last_gas_fee_usd is not None else float("inf"),
    }
