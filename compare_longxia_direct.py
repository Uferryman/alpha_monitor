import sqlite3
from collections import Counter
from datetime import timedelta

from demand2_realtime_v2 import (
    query_interval,
    parse_iso,
    normalize_wallet,
)

DB = "alpha_monitor.db"
ADDRESS = "0xeccbb861c0dda7efd964010085488b69317e4444"

conn = sqlite3.connect(DB)

# 读取当前checkpoint
checkpoint_text = conn.execute(
    """
    SELECT meta_value
    FROM demand2_v2_meta
    WHERE meta_key='last_success_end'
    """
).fetchone()[0]

end = parse_iso(checkpoint_text)
start = end - timedelta(days=15)

# 找到龙虾
token_key, token_id, symbol = conn.execute(
    """
    SELECT token_key, token_id, symbol
    FROM alpha_token_registry
    WHERE lower(contract_address)=lower(?)
    """,
    (ADDRESS,),
).fetchone()

print("=" * 72)
print("龙虾 单币15天直接对比")
print("=" * 72)
print("Token：", symbol)
print("窗口：", start, "→", end)
print()
print("发送1次 Bitquery 单币查询...")

# 只查询龙虾一个Token
first_rows, _flow_rows = query_interval(
    [token_id],
    start,
    end,
)

print("Bitquery返回：", len(first_rows), "行")

if len(first_rows) >= 20000:
    raise RuntimeError("达到20000行上限，本次结果不能用于比较")


# ------------------------------------------------------------
# Bitquery直接结果：
# 同一个钱包如果意外重复，只保留窗口内最早时间
# ------------------------------------------------------------

direct_wallets = {}

for row in first_rows:

    wallet = normalize_wallet(
        token_id,
        row.get("Trader", {}).get("Address", ""),
    )

    first_time = (
        row.get("Block", {})
        .get("first_buy_time")
    )

    if not wallet or not first_time:
        continue

    old = direct_wallets.get(wallet)

    if old is None or first_time < old:
        direct_wallets[wallet] = first_time


direct_daily = Counter(
    first_time[:10]
    for first_time in direct_wallets.values()
)


# ------------------------------------------------------------
# 本地V2结果：
# 只取同一个15天窗口内保存的最早first_buy_time
# ------------------------------------------------------------

local_rows = conn.execute(
    """
    SELECT first_buy_time
    FROM wallet_token_first_buy_v2
    WHERE
        token_key=?
        AND first_buy_time>=?
        AND first_buy_time<?
    """,
    (
        token_key,
        start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    ),
).fetchall()

local_daily = Counter(
    row[0][:10]
    for row in local_rows
)


# ------------------------------------------------------------
# 逐日对比
# ------------------------------------------------------------

print()
print("日期          Bitquery直接    本地V2     差值")
print("-" * 52)

days = sorted(
    set(direct_daily)
    |
    set(local_daily)
)

for day in days:

    direct = direct_daily.get(day, 0)
    local = local_daily.get(day, 0)

    print(
        f"{day}    "
        f"{direct:>8}    "
        f"{local:>8}    "
        f"{direct-local:+8}"
    )


print("-" * 52)

print(
    "Bitquery窗口唯一钱包：",
    len(direct_wallets)
)

print(
    "本地窗口钱包：",
    len(local_rows)
)

print()
print("Bitquery HTTP请求：1次")
print("数据库修改：0")
print("checkpoint修改：0")

conn.close()
