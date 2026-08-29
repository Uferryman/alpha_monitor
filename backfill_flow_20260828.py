# -*- coding: utf-8 -*-

# ============================================================
# 一次性补齐 2026-08-28 全天资金数据
#
# 时间范围：
# 2026-08-28 00:00 UTC
# →
# 2026-08-29 00:00 UTC
#
# 只做一件事：
# 为当前305个Alpha Token统计全天：
#
# buy_usd
# sell_usd
# netflow_usd
#
# 然后写入现有 daily_fund_flow。
#
# 正常情况下：
# Bitquery HTTP请求 = 1次
# ============================================================

import os
import sqlite3
import requests

from datetime import datetime
from dotenv import load_dotenv


DATABASE_FILE = "alpha_monitor.db"

BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)

DATE_TEXT = "2026-08-28"

SINCE = "2026-08-28T00:00:00Z"

BEFORE = "2026-08-29T00:00:00Z"


# ============================================================
# 1. 读取Token
# ============================================================

conn = sqlite3.connect(
    DATABASE_FILE
)

conn.execute(
    "PRAGMA busy_timeout = 30000"
)

cur = conn.cursor()


cur.execute("""
SELECT
    token_key,
    token_id,
    symbol,
    chain,
    contract_address
FROM alpha_token_registry
ORDER BY token_key
""")


token_rows = cur.fetchall()


if len(token_rows) != 305:

    raise RuntimeError(
        f"当前Alpha Token数量={len(token_rows)}，"
        "预期305，为安全起见停止。"
    )


token_ids = [
    row[1]
    for row in token_rows
]


token_by_id = {
    row[1]: row
    for row in token_rows
}


# ============================================================
# 2. 防止重复补
# ============================================================

cur.execute("""
SELECT COUNT(*)
FROM daily_fund_flow
WHERE date = ?
""", (
    DATE_TEXT,
))


existing_count = cur.fetchone()[0]


if existing_count > 0:

    print(
        f"⚠️ {DATE_TEXT} 已经存在 "
        f"{existing_count} 行。"
    )

    print(
        "为避免重复覆盖，本次停止。"
    )

    conn.close()

    raise SystemExit


# ============================================================
# 3. Bitquery查询
#
# 一次请求：
# 305个Token整天资金汇总。
# ============================================================

QUERY = """
query BackfillDailyFlow(
  $tokens: [String!]!
  $since: DateTime!
  $before: DateTime!
) {

  Trading {

    Flow: Trades(

      limit: {
        count: 1000
      }

      where: {

        Block: {
          Time: {
            since: $since
            before: $before
          }
        }

        Pair: {
          Token: {
            Id: {
              in: $tokens
            }
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
          Side: {
            is: "Buy"
          }
        }
      )

      sell_usd: sum(
        of: AmountsInUsd_Quote
        if: {
          Side: {
            is: "Sell"
          }
        }
      )
    }
  }
}
"""


# ============================================================
# 4. Token只从本地.env读取
# ============================================================

load_dotenv()

BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)


if not BITQUERY_TOKEN:

    raise RuntimeError(
        "没有读取到 BITQUERY_TOKEN"
    )


print("=" * 72)

print(
    "一次性补齐 2026-08-28 全天资金"
)

print("=" * 72)

print(
    "Token：305"
)

print(
    f"时间：{SINCE}"
)

print(
    f"   → {BEFORE}"
)

print()

print(
    "即将发送1次Bitquery请求..."
)


# ============================================================
# 5. 只请求一次，不自动重试
#
# 防止测试阶段无意产生额外Points。
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
                SINCE,

            "before":
                BEFORE,
        },
    },

    timeout=180,
)


if response.status_code != 200:

    print(
        f"❌ HTTP状态："
        f"{response.status_code}"
    )

    print(
        "数据库没有写入。"
    )

    conn.close()

    raise SystemExit


result = response.json()


if result.get(
    "errors"
):

    print(
        "❌ GraphQL错误："
    )

    print(
        result[
            "errors"
        ]
    )

    print(
        "数据库没有写入。"
    )

    conn.close()

    raise SystemExit


flow_rows = (
    result
    .get(
        "data",
        {}
    )
    .get(
        "Trading",
        {}
    )
    .get(
        "Flow",
        []
    )
)


print(
    f"Bitquery返回资金Token："
    f"{len(flow_rows)}"
)


# ============================================================
# 6. 先把返回结果整理成字典
# ============================================================

flow_by_token_id = {}


for row in flow_rows:

    token_id = (
        row
        .get(
            "Pair",
            {}
        )
        .get(
            "Token",
            {}
        )
        .get(
            "Id",
            ""
        )
    )


    if token_id not in token_by_id:
        continue


    buy_usd = float(
        row.get(
            "buy_usd"
        )
        or 0
    )


    sell_usd = float(
        row.get(
            "sell_usd"
        )
        or 0
    )


    flow_by_token_id[
        token_id
    ] = (
        buy_usd,
        sell_usd,
    )


# ============================================================
# 7. 生成完整305行
#
# 没交易的Token：
#
# buy = 0
# sell = 0
# net = 0
#
# 这样daily_fund_flow每天固定305行。
# ============================================================

updated_at = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)


insert_rows = []


for (
    token_key,
    token_id,
    symbol,
    chain,
    contract_address
) in token_rows:


    buy_usd, sell_usd = (
        flow_by_token_id.get(
            token_id,
            (
                0.0,
                0.0,
            )
        )
    )


    netflow_usd = (
        buy_usd
        -
        sell_usd
    )


    insert_rows.append(
        (
            DATE_TEXT,
            symbol,
            chain,
            contract_address,
            buy_usd,
            sell_usd,
            netflow_usd,
            updated_at,
        )
    )


# ============================================================
# 8. 一次事务写入
# ============================================================

try:

    conn.execute(
        "BEGIN"
    )


    cur.executemany(
        """
        INSERT INTO daily_fund_flow (

            date,
            symbol,
            chain,
            contract_address,
            buy_usd,
            sell_usd,
            netflow_usd,
            updated_at

        )

        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )

        ON CONFLICT(
            date,
            chain,
            contract_address
        )

        DO UPDATE SET

            symbol =
                excluded.symbol,

            buy_usd =
                excluded.buy_usd,

            sell_usd =
                excluded.sell_usd,

            netflow_usd =
                excluded.netflow_usd,

            updated_at =
                excluded.updated_at
        """,
        insert_rows,
    )


    conn.commit()


except Exception:

    conn.rollback()

    conn.close()

    raise


# ============================================================
# 9. 本地验证
# ============================================================

cur.execute("""
SELECT
    COUNT(*),
    COALESCE(
        SUM(buy_usd),
        0
    ),
    COALESCE(
        SUM(sell_usd),
        0
    ),
    COALESCE(
        SUM(netflow_usd),
        0
    )
FROM daily_fund_flow
WHERE date = ?
""", (
    DATE_TEXT,
))


(
    count,
    total_buy,
    total_sell,
    total_net
) = cur.fetchone()


print()

print("=" * 72)

print(
    "✅ 2026-08-28 补齐完成"
)

print("=" * 72)

print(
    "Token行数：",
    count
)

print(
    "全天买入：",
    f"${total_buy:,.2f}"
)

print(
    "全天卖出：",
    f"${total_sell:,.2f}"
)

print(
    "全天净流入：",
    f"${total_net:,.2f}"
)

print()

print(
    "Bitquery HTTP请求：1次"
)

print(
    "以后这一天不需要再查询。"
)


conn.close()
