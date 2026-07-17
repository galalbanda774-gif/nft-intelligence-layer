"""
محرك الشراء التلقائي عبر عقد SeaDrop على Robinhood Chain.
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
]

MAX_GAS_FEE_USD = 0.05
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 5


def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url))


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    """يسأل عقد SeaDrop مباشرة عن عنوان الرسوم المسموح — لا تخمين."""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            log.warning(f"[FeeRecipient] لا يوجد عنوان مسموح لـ {nft_contract}")
            return None
        return recipients[0]
    except Exception as e:
        log.error(f"[FeeRecipient] خطأ استعلام: {e}")
        return None


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float) -> float:
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * 150_000) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[Gas] تعذر تقدير الرسوم: {e}")
        return float("inf")


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))


def buy_mint(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    quantity: int,
    eth_price_usd: float,
) -> dict:
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > MAX_GAS_FEE_USD:
        log.info(f"[إلغاء] رسوم الغاز ${gas_fee_usd:.4f} > الحد ${MAX_GAS_FEE_USD}.")
        return {"success": False, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    fee_recipient = get_fee_recipient(w3, nft_contract)
    if not fee_recipient:
        return {"success": False, "reason": "no_fee_recipient"}

    total_value = price_wei_per_token * quantity

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        tx = contract.functions.mintPublic(
            Web3.to_checksum_address(nft_contract),
            Web3.to_checksum_address(fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS),  # المينت للدافع نفسه
            quantity,
        ).build_transaction({
            "from": Web3.to_checksum_address(wallet_address),
            "value": total_value,
            "nonce": w3.eth.get_transaction_count(wallet_address, "pending"),
            "chainId": w3.eth.chain_id,
        })

        try:
            tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.2)
        except Exception as e:
            log.warning(f"[Gas] فشل estimate_gas: {e}")
            tx["gas"] = 300_000

        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[شراء] تم إرسال معاملة: {tx_hash.hex()}")
        return {
            "success": True, "tx_hash": tx_hash.hex(),
            "quantity": quantity, "gas_fee_usd": gas_fee_usd,
            "total_value_wei": total_value,
        }

    except Exception as e:
        log.error(f"[خطأ شراء] {e}")
        return {"success": False, "reason": "tx_error", "error": str(e)}
