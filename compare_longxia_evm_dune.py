# -*- coding: utf-8 -*-

import os
import sqlite3
from collections import Counter

import requests
from dotenv import load_dotenv


# ============================================================
# 1. 基础参数
# ============================================================

DB = "alpha_monitor.db"

# 龙虾合约地址
TOKEN = "0xeccbb861c0dda7efd964010085488b69317e4444"

# Bitquery V2 官方 GraphQL 地址
URL = "https://streaming.bitquery.io/graphql"

# Dune截图显示的窗口从 8月14日 00:00 UTC 开始
START = "2026-08-14T00:00:00Z"


# ============================================================
# 2. 读取当前需求二 checkpoint
#    用它作为查询截止时间
# ============================================================

conn = sqlite3.connect(DB)

row = conn.execute(
    """
    SELECT meta_value
    FROM demand2_v2_meta
    WHERE meta_key = 'last_success_end'
    """
).fetchone()

conn.close()

if not row:
    raise RuntimeError("找不到需求二 checkpoint")

END = row[0]


# ============================================================
# 3. 你刚才截图中的 Dune 数据
# ============================================================

DUNE = {
    "2026-08-14": 117,
    "2026-08-15": 132,
    "2026-08-16": 90,
    "2026-08-17": 66,
    "2026-08-18": 33,
    "2026-08-19": 44,
    "2026-08-20": 31,
    "2026-08-21": 34,
    "2026-08-22": 30,
    "2026-08-23": 36,
    "2026-08-24": 38,
    "2026-08-25": 95,
    "2026-08-26": 179,
    "2026-08-27": 110,
    "2026-08-28": 223,
    "2026-08-29": 36,
}


# ============================================================
# 4. Bitquery官方 BSC First Buyers 口径
#
#    关键：
#    - EVM.DEXTrades
#    - Trade.Sell.Currency = 龙虾
#    - 钱包 = Trade.Sell.Buyer
#    - 每个Buyer只保留窗口内第一次
# ============================================================

QUERY = """
query FirstBuyers(
  $token: String!
  $start: DateTime!
  $end: DateTime!
) {
  EVM(network: bsc, dataset: combined) {
    DEXTrades(
      limit: {count: 20000}
      orderBy: {ascending: Block_Time}
      limitBy: {
        count: 1
        by: Trade_Sell_Buyer
      }
      where: {
        Block: {
          Time: {
            after: $start
            before: $end
          }
        }
        Trade: {
          Sell: {
            Currency: {
              SmartContract: {
                is: $token
              }
            }
          }
        }
      }
    ) {
      Block {
        Time
      }
      Trade {
        Sell {
          Buyer
        }
      }
    }
  }
}
"""


# ============================================================
# 5. 读取Token
# ============================================================

load_dotenv()

token = os.getenv("BITQUERY_TOKEN")

if not token:
    raise RuntimeError("没有读取到 BITQUERY_TOKEN")


# ============================================================
# 6. 只发送一次HTTP请求
#    故意不做任何自动重试
# ============================================================

print("=" * 72)
print("龙虾：Bitquery EVM.DEXTrades vs Dune")
print("=" * 72)

print("查询窗口：")
print(START)
print("→")
print(END)

print()
print("发送 1 次 Bitquery 请求……")


response = requests.post(
    URL,
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    },
    json={
        "query": QUERY,
        "variables": {
            "token": TOKEN,
            "start": START,
            "end": END,
        },
    },
    timeout=120,
)


# ============================================================
# 7. 请求失败直接停止
#    不重试，防止额外消耗Points
# ============================================================

if response.status_code != 200:
    print("HTTP状态：", response.status_code)
    print(response.text[:1000])
    raise SystemExit(1)


result = response.json()

if result.get("errors"):
    print("GraphQL错误：")
    print(result["errors"])
    raise SystemExit(1)


# ============================================================
# 8. 读取结果
# ============================================================

rows = (
    result
    .get("data", {})
    .get("EVM", {})
    .get("DEXTrades", [])
)

print("返回唯一Buyer：", len(rows))


if len(rows) >= 20000:
    raise RuntimeError(
        "结果达到20000保护线，不能用于判断"
    )


# ============================================================
# 9. 按首次买入日期统计
# ============================================================

daily = Counter()

for row in rows:

    block_time = (
        row
        .get("Block", {})
        .get("Time")
    )

    buyer = (
        row
        .get("Trade", {})
        .get("Sell", {})
        .get("Buyer")
    )

    if block_time and buyer:
        day = block_time[:10]
        daily[day] += 1


# ============================================================
# 10. 和Dune逐日对比
# ============================================================

print()
print(
    "日期          "
    "Dune     "
    "EVM.DEXTrades     "
    "差值"
)

print("-" * 58)


for day in sorted(DUNE):

    dune_count = DUNE[day]

    bitquery_count = daily.get(
        day,
        0,
    )

    diff = (
        bitquery_count
        -
        dune_count
    )

    print(
        f"{day}    "
        f"{dune_count:>5}    "
        f"{bitquery_count:>13}    "
        f"{diff:+6}"
    )


print("-" * 58)

print(
    "Dune合计：",
    sum(DUNE.values())
)

print(
    "Bitquery合计：",
    sum(daily.values())
)

print()
print("Bitquery HTTP请求：1次")
print("数据库修改：0")
print("checkpoint修改：0")
print("自动重试：0")
