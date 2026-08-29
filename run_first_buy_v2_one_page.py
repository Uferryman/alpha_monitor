# ============================================================
# first_buy V2 单页安全执行器
#
# 每运行一次：
#
# 只允许发送 1 次 Bitquery HTTP请求
#
# 请求成功后：
# - 数据正式写入 wallet_token_first_buy_v2
# - checkpoint正式推进
# - 下次全量V2会从下一页继续
#
# 不会浪费这次测试查询。
# ============================================================

import os
import json
import sqlite3
import requests

from datetime import datetime
from dotenv import load_dotenv


# ============================================================
# 1. 配置
# ============================================================

DATABASE_FILE = "alpha_monitor.db"

BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)

PAGE_SIZE = 20000


# ============================================================
# 2. 环境变量
# ============================================================

load_dotenv()

BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

if not BITQUERY_TOKEN:
    raise RuntimeError(
        "没有读取到 BITQUERY_TOKEN"
    )


# ============================================================
# 3. 数据库
# ============================================================

connection = sqlite3.connect(
    DATABASE_FILE
)

connection.execute(
    "PRAGMA busy_timeout = 30000"
)

cursor = connection.cursor()


# ============================================================
# 4. 读取冻结窗口
# ============================================================

cursor.execute(
    """
    SELECT meta_key, meta_value
    FROM wallet_first_buy_v2_meta
    WHERE meta_key IN (
        'window_start',
        'window_end'
    )
    """
)

meta = dict(
    cursor.fetchall()
)

window_start = meta.get(
    "window_start"
)

window_end = meta.get(
    "window_end"
)

if not window_start or not window_end:
    raise RuntimeError(
        "没有找到V2冻结窗口"
    )


# ============================================================
# 5. 找到第一个尚未完成的大Token任务
#
# 与正式V2顺序保持一致。
# ============================================================

cursor.execute(
    """
    SELECT
        job_id,
        token_keys_json,
        estimated_wallets,
        status,
        next_offset

    FROM wallet_first_buy_v2_plan

    WHERE
        job_type = 'large'
        AND status IN (
            'pending',
            'running'
        )

    ORDER BY job_id

    LIMIT 1
    """
)

job = cursor.fetchone()

if not job:

    print(
        "没有尚未完成的大Token任务。"
    )

    connection.close()

    raise SystemExit


(
    job_id,
    token_keys_json,
    estimated_wallets,
    job_status,
    next_offset
) = job


token_keys = [
    int(value)
    for value in json.loads(
        token_keys_json
    )
]

if len(token_keys) != 1:
    raise RuntimeError(
        "大Token任务结构异常"
    )


token_key = token_keys[0]

offset = int(
    next_offset or 0
)


# ============================================================
# 6. Token信息
# ============================================================

cursor.execute(
    """
    SELECT
        token_id,
        symbol

    FROM alpha_token_registry

    WHERE token_key = ?
    """,
    (
        token_key,
    )
)

token_row = cursor.fetchone()

if not token_row:
    raise RuntimeError(
        "找不到Token注册信息"
    )


token_id = token_row[0]

symbol = token_row[1]


# ============================================================
# 7. 地址标准化
# ============================================================

def normalize_wallet(
    current_token_id,
    wallet,
):

    wallet = str(
        wallet or ""
    ).strip()

    if (
        current_token_id.startswith("bid:bsc:")
        or
        current_token_id.startswith("bid:base:")
        or
        current_token_id.startswith("bid:eth:")
        or
        current_token_id.startswith("bid:arbitrum:")
    ):
        return wallet.lower()

    return wallet


# ============================================================
# 8. 查询
#
# 固定历史窗口。
#
# 单Token。
#
# Token + Trader 聚合。
#
# first_buy_time取minimum。
#
# 按Trader_Address稳定排序。
# ============================================================

QUERY = """
query FirstBuyV2OnePage(
  $tokens: [String!]!
  $since: DateTime!
  $till: DateTime!
  $limit: Int!
  $offset: Int!
) {

  Trading {

    Trades(

      limit: {
        count: $limit
        offset: $offset
      }

      orderBy: [
        {
          ascending: Trader_Address
        }
      ]

      where: {

        Block: {
          Time: {
            since: $since
            till: $till
          }
        }

        Pair: {
          Token: {
            Id: {
              in: $tokens
            }
          }
        }

        Side: {
          is: "Buy"
        }
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
  }
}
"""


# ============================================================
# 9. 显示本次即将请求什么
# ============================================================

print("=" * 72)

print(
    "first_buy V2 单页安全测试"
)

print("=" * 72)

print(
    f"Job：{job_id}"
)

print(
    f"Token：{symbol}"
)

print(
    f"预计钱包：{estimated_wallets:,}"
)

print(
    f"窗口：{window_start}"
)

print(
    f"   → {window_end}"
)

print(
    f"offset：{offset:,}"
)

print(
    f"limit：{PAGE_SIZE:,}"
)

print(
    "\n本程序最多发送1次Bitquery请求。"
)


# ============================================================
# 10. 唯一一次HTTP请求
#
# 不自动重试。
#
# 如果429、402、网络错误：
# 直接退出。
#
# 避免测试阶段产生额外请求。
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
                [
                    token_id
                ],

            "since":
                window_start,

            "till":
                window_end,

            "limit":
                PAGE_SIZE,

            "offset":
                offset,
        },
    },

    timeout=180,
)


# ============================================================
# 11. HTTP检查
# ============================================================

if response.status_code != 200:

    print(
        f"\n❌ HTTP状态："
        f"{response.status_code}"
    )

    print(
        "数据库没有推进。"
    )

    connection.close()

    raise SystemExit


result = response.json()


if "errors" in result:

    print(
        "\n❌ GraphQL错误："
    )

    print(
        result["errors"]
    )

    print(
        "数据库没有推进。"
    )

    connection.close()

    raise SystemExit


# ============================================================
# 12. 获取结果
# ============================================================

rows = (
    result[
        "data"
    ][
        "Trading"
    ][
        "Trades"
    ]
)


row_count = len(
    rows
)


print(
    f"\n返回：{row_count:,} 条"
)


# ============================================================
# 13. 整理正式写入数据
# ============================================================

updated_at = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

insert_rows = []


for row in rows:

    returned_token_id = (
        row.get(
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

    if returned_token_id != token_id:
        continue


    wallet = (
        row.get(
            "Trader",
            {}
        )
        .get(
            "Address",
            ""
        )
    )


    wallet = normalize_wallet(
        token_id,
        wallet,
    )


    first_buy_time = (
        row.get(
            "Block",
            {}
        )
        .get(
            "first_buy_time",
            ""
        )
    )


    if not wallet or not first_buy_time:
        continue


    insert_rows.append(
        (
            token_key,
            wallet,
            first_buy_time,
            updated_at,
        )
    )


# ============================================================
# 14. 一次事务正式保存
# ============================================================

try:

    connection.execute(
        "BEGIN"
    )


    cursor.executemany(
        """
        INSERT INTO wallet_token_first_buy_v2 (

            token_key,
            wallet,
            first_buy_time,
            updated_at

        )

        VALUES (?, ?, ?, ?)

        ON CONFLICT(
            token_key,
            wallet
        )

        DO UPDATE SET

            first_buy_time =

                CASE

                    WHEN
                        excluded.first_buy_time
                        <
                        wallet_token_first_buy_v2.first_buy_time

                    THEN
                        excluded.first_buy_time

                    ELSE
                        wallet_token_first_buy_v2.first_buy_time

                END,

            updated_at =
                excluded.updated_at
        """,
        insert_rows,
    )


    # ========================================================
    # 15. checkpoint推进
    # ========================================================

    if row_count < PAGE_SIZE:

        # 最后一页
        cursor.execute(
            """
            UPDATE wallet_first_buy_v2_plan

            SET
                status = 'complete',
                next_offset = ?,
                last_row_count = ?,
                completed_at = ?

            WHERE job_id = ?
            """,
            (
                offset,
                row_count,
                updated_at,
                job_id,
            )
        )

    else:

        # 还有下一页
        cursor.execute(
            """
            UPDATE wallet_first_buy_v2_plan

            SET
                status = 'running',
                next_offset = ?,
                last_row_count = ?

            WHERE job_id = ?
            """,
            (
                offset + PAGE_SIZE,
                row_count,
                job_id,
            )
        )


    # ========================================================
    # 16. 成功请求数量 +1
    # ========================================================

    cursor.execute(
        """
        SELECT meta_value

        FROM wallet_first_buy_v2_meta

        WHERE meta_key =
            'successful_query_count'
        """
    )

    count_row = cursor.fetchone()

    old_count = int(
        count_row[0]
        if count_row
        else 0
    )


    cursor.execute(
        """
        INSERT INTO wallet_first_buy_v2_meta (
            meta_key,
            meta_value
        )

        VALUES (
            'successful_query_count',
            ?
        )

        ON CONFLICT(meta_key)

        DO UPDATE SET
            meta_value =
                excluded.meta_value
        """,
        (
            str(
                old_count + 1
            ),
        )
    )


    connection.commit()


except Exception:

    connection.rollback()

    connection.close()

    raise


# ============================================================
# 17. 最终结果
# ============================================================

print(
    f"有效写入："
    f"{len(insert_rows):,} 条"
)


if row_count < PAGE_SIZE:

    print(
        f"✅ {symbol} 已经完成"
    )

else:

    print(
        f"✅ 第1页已保存"
    )

    print(
        f"下一offset："
        f"{offset + PAGE_SIZE:,}"
    )


print(
    "\n本次Bitquery HTTP请求：1次"
)

print(
    "本次结果已经正式计入V2，"
    "后面不会重复查询这一页。"
)


connection.close()
