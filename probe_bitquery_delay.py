# -*- coding: utf-8 -*-

# ============================================================
# Bitquery 15分钟数据时延测试
#
# 用法：
# 同一个15分钟区间结束后：
#
# +2分钟运行一次
# +5分钟再运行一次
#
# 程序自动识别同一个最近完整15分钟区间，
# 并比较两次查询结果。
#
# 不修改：
# - first_buy
# - 资金累计
# - checkpoint
#
# 只保存一个很小的临时JSON测试文件。
# 测试结束后可以直接删除。
# ============================================================

import os
import json
import hashlib
import sqlite3
import requests

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv


DATABASE_FILE = "alpha_monitor.db"
RESULT_FILE = Path("bitquery_delay_probe.json")

BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"


QUERY = """
query DelayProbe(
  $tokens: [String!]!
  $since: DateTime!
  $before: DateTime!
) {
  Trading {

    FirstBuys: Trades(
      limit: { count: 20000 }
      where: {
        Block: {
          Time: {
            since: $since
            before: $before
          }
        }
        Pair: {
          Token: {
            Id: { in: $tokens }
          }
        }
        Side: { is: "Buy" }
      }
    ) {
      Pair {
        Token {
          Id
        }
      }
      Trader {
        Address
      }
      Block {
        first_buy_time: Time(
          minimum: Block_Time
        )
      }
    }

    Flow: Trades(
      limit: { count: 1000 }
      where: {
        Block: {
          Time: {
            since: $since
            before: $before
          }
        }
        Pair: {
          Token: {
            Id: { in: $tokens }
          }
        }
      }
    ) {
      Pair {
        Token {
          Id
        }
      }

      buy_usd: sum(
        of: AmountsInUsd_Quote
        if: {
          Side: { is: "Buy" }
        }
      )

      sell_usd: sum(
        of: AmountsInUsd_Quote
        if: {
          Side: { is: "Sell" }
        }
      )
    }
  }
}
"""


# ============================================================
# 最近一个完整15分钟区间
# ============================================================

def floor_15m(dt):

    dt = dt.astimezone(
        timezone.utc
    ).replace(
        second=0,
        microsecond=0
    )

    minute = (
        dt.minute // 15
    ) * 15

    return dt.replace(
        minute=minute
    )


def iso(dt):

    return dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


now = datetime.now(
    timezone.utc
)

interval_end = floor_15m(
    now
)

interval_start = (
    interval_end
    -
    timedelta(
        minutes=15
    )
)

delay_seconds = int(
    (
        now
        -
        interval_end
    ).total_seconds()
)


# ============================================================
# 读取当前Alpha Token
# ============================================================

conn = sqlite3.connect(
    DATABASE_FILE
)

cur = conn.cursor()

cur.execute("""
SELECT token_id
FROM alpha_token_registry
ORDER BY token_key
""")

token_ids = [
    row[0]
    for row in cur.fetchall()
    if row[0]
]

conn.close()


# ============================================================
# Bitquery Token
# ============================================================

load_dotenv()

BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

if not BITQUERY_TOKEN:

    raise RuntimeError(
        "没有读取到 BITQUERY_TOKEN"
    )


print("=" * 80)

print(
    "Bitquery 15分钟时延测试"
)

print("=" * 80)

print(
    "区间：",
    iso(interval_start)
)

print(
    "   →",
    iso(interval_end)
)

print(
    "本次查询延迟：",
    f"{delay_seconds} 秒",
    f"≈ {delay_seconds / 60:.1f} 分钟"
)

print(
    "Alpha Token：",
    len(token_ids)
)

print()

print(
    "发送1次Bitquery查询..."
)


# ============================================================
# 查询
# ============================================================

response = requests.post(
    BITQUERY_URL,

    headers={
        "Content-Type":
            "application/json",

        "Authorization":
            f"Bearer {BITQUERY_TOKEN}",
    },

    json={
        "query":
            QUERY,

        "variables": {
            "tokens":
                token_ids,

            "since":
                iso(interval_start),

            "before":
                iso(interval_end),
        },
    },

    timeout=180,
)


if response.status_code != 200:

    raise RuntimeError(
        f"HTTP {response.status_code}"
    )


result = response.json()


if result.get(
    "errors"
):

    raise RuntimeError(
        str(
            result["errors"]
        )
    )


trading = (
    result
    .get("data", {})
    .get("Trading", {})
)


first_rows = trading.get(
    "FirstBuys",
    []
)

flow_rows = trading.get(
    "Flow",
    []
)


# ============================================================
# first-buy完整内容指纹
#
# 不只比较数量。
# 如果数量一样但钱包发生变化，也能发现。
# ============================================================

first_items = []


for row in first_rows:

    token_id = (
        row
        .get("Pair", {})
        .get("Token", {})
        .get("Id", "")
    )

    wallet = (
        row
        .get("Trader", {})
        .get("Address", "")
    )

    first_time = (
        row
        .get("Block", {})
        .get("first_buy_time", "")
    )

    first_items.append(
        f"{token_id}|{wallet}|{first_time}"
    )


first_items.sort()


first_hash = hashlib.sha256(
    "\n".join(
        first_items
    ).encode("utf-8")
).hexdigest()


# ============================================================
# 资金完整内容指纹
# ============================================================

total_buy = 0.0
total_sell = 0.0

flow_items = []


for row in flow_rows:

    token_id = (
        row
        .get("Pair", {})
        .get("Token", {})
        .get("Id", "")
    )

    buy_usd = float(
        row.get("buy_usd")
        or 0
    )

    sell_usd = float(
        row.get("sell_usd")
        or 0
    )

    total_buy += buy_usd
    total_sell += sell_usd

    flow_items.append(
        (
            token_id,
            buy_usd,
            sell_usd,
        )
    )


flow_items.sort(
    key=lambda x: x[0]
)


flow_hash_text = "\n".join(
    f"{token_id}|{buy_usd:.8f}|{sell_usd:.8f}"

    for (
        token_id,
        buy_usd,
        sell_usd
    ) in flow_items
)


flow_hash = hashlib.sha256(
    flow_hash_text.encode(
        "utf-8"
    )
).hexdigest()


total_net = (
    total_buy
    -
    total_sell
)


# ============================================================
# 本次结果
# ============================================================

sample = {
    "queried_at":
        iso(now),

    "delay_seconds":
        delay_seconds,

    "first_buy_count":
        len(first_rows),

    "first_buy_hash":
        first_hash,

    "flow_token_count":
        len(flow_rows),

    "total_buy_usd":
        total_buy,

    "total_sell_usd":
        total_sell,

    "total_netflow_usd":
        total_net,

    "flow_hash":
        flow_hash,
}


interval_key = (
    f"{iso(interval_start)}"
    f"__"
    f"{iso(interval_end)}"
)


# ============================================================
# 读取旧测试
# ============================================================

if RESULT_FILE.exists():

    history = json.loads(
        RESULT_FILE.read_text(
            encoding="utf-8"
        )
    )

else:

    history = {}


samples = history.setdefault(
    interval_key,
    []
)


# ============================================================
# 与同一区间上一条样本比较
# ============================================================

previous = (
    samples[-1]
    if samples
    else None
)


samples.append(
    sample
)


RESULT_FILE.write_text(
    json.dumps(
        history,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)


# ============================================================
# 输出
# ============================================================

print()

print(
    "first-buy：",
    f"{len(first_rows):,} 行"
)

print(
    "资金Token：",
    len(flow_rows)
)

print(
    "买入：",
    f"${total_buy:,.2f}"
)

print(
    "卖出：",
    f"${total_sell:,.2f}"
)

print(
    "净流入：",
    f"${total_net:,.2f}"
)


print()

if previous is None:

    print(
        "这是该区间第一次测试。"
    )

    print(
        "请在同一区间稍后再运行一次进行比较。"
    )

else:

    first_same = (
        previous[
            "first_buy_hash"
        ]
        ==
        sample[
            "first_buy_hash"
        ]
    )

    flow_same = (
        previous[
            "flow_hash"
        ]
        ==
        sample[
            "flow_hash"
        ]
    )


    print(
        "=" * 80
    )

    print(
        "与同一区间上一轮比较"
    )

    print(
        "=" * 80
    )

    print(
        "上次延迟：",
        f"{previous['delay_seconds'] / 60:.1f} 分钟"
    )

    print(
        "本次延迟：",
        f"{sample['delay_seconds'] / 60:.1f} 分钟"
    )

    print()

    print(
        "first-buy：",
        "✅ 完全一致"
        if first_same
        else
        "⚠️ 有变化"
    )

    # 如果 first-buy 有变化，
    # 直接显示上一轮、本轮以及差值。
    if not first_same:

        first_delta = (
            sample["first_buy_count"]
            -
            previous["first_buy_count"]
        )

        print(
            "  上次：",
            f"{previous['first_buy_count']:,}"
        )

        print(
            "  本次：",
            f"{sample['first_buy_count']:,}"
        )

        print(
            "  差值：",
            f"{first_delta:+,}"
        )


    print(
        "资金数据：",
        "✅ 完全一致"
        if flow_same
        else
        "⚠️ 有变化"
    )

    # 如果资金数据变化，
    # 显示买入、卖出、净流入的实际差值。
    if not flow_same:

        buy_delta = (
            sample["total_buy_usd"]
            -
            previous["total_buy_usd"]
        )

        sell_delta = (
            sample["total_sell_usd"]
            -
            previous["total_sell_usd"]
        )

        net_delta = (
            sample["total_netflow_usd"]
            -
            previous["total_netflow_usd"]
        )

        print(
            "  买入变化：",
            f"${buy_delta:+,.8f}"
        )

        print(
            "  卖出变化：",
            f"${sell_delta:+,.8f}"
        )

        print(
            "  净流入变化：",
            f"${net_delta:+,.8f}"
        )


    if (
        first_same
        and
        flow_same
    ):

        print()

        print(
            "✅ 这两个时间点之间数据稳定"
        )

    else:

        print()

        print(
            "⚠️ 这两个时间点之间仍有数据补录"
        )


print()

print(
    "Bitquery HTTP请求：1次"
)

print(
    "未修改业务数据库"
)

print(
    "未修改checkpoint"
)
