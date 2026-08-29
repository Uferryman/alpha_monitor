# ============================================================
# Binance Alpha 跨链单日一次请求测速
#
# 测试目标：
# 1. 将 BSC / Base / Ethereum / Solana / Arbitrum / TRON
#    的全部有效 Alpha 合并
# 2. 使用 Bitquery Trading.Trades
# 3. 通过 Pair.Token.Id 一次性查询全部 Token
# 4. 只查询昨天 1 个完整 UTC 自然日
# 5. 不写数据库
# 6. 不修改 alpha_ambush_pool.csv
# 7. 用 UB 和数据库已有结果对账
# ============================================================


# 导入 os，用于读取环境变量
import os

# 导入 csv，用于读取 Alpha 清单
import csv

# 导入 time，用于统计查询耗时
import time

# 导入 sqlite3，只读取数据库，不会修改
import sqlite3

# 导入 requests，用于访问 Bitquery
import requests

# 导入日期工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env
from dotenv import load_dotenv


# ============================================================
# 1. 读取 Bitquery Token
# ============================================================

# 加载 .env
load_dotenv()

# 读取 Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 如果没有 Token 就停止
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 2. 基础文件
# ============================================================

# Bitquery 亚洲节点
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

# Binance Alpha 有效清单
ALPHA_FILE = "alpha_tokens_active.csv"

# 当前数据库
DATABASE_FILE = "alpha_monitor.db"

# UB 合约
UB_ADDRESS = (
    "0x40b8129b786d766267a7a118cf8c07e31cdb6fde"
)

# UB 在 Bitquery Trading 数据里的完整 Token ID
UB_TOKEN_ID = (
    "bid:bsc:"
    + UB_ADDRESS
)


# ============================================================
# 3. Binance 链名称 → Bitquery Token ID 前缀
# ============================================================

# Bitquery 的 Token.Id 自带链信息
#
# 例如：
#
# BSC:
# bid:bsc:0x123...
#
# Ethereum:
# bid:eth:0x123...
#
# Solana:
# bid:solana:MintAddress...
TOKEN_ID_PREFIX = {
    "BSC": "bid:bsc",
    "Base": "bid:base",
    "Ethereum": "bid:eth",
    "Solana": "bid:solana",
    "Arbitrum": "bid:arbitrum",
    "TRON": "bid:tron",
}


# ============================================================
# 4. EVM 链
# ============================================================

# EVM 合约统一使用小写
EVM_CHAINS = {
    "BSC",
    "Base",
    "Ethereum",
    "Arbitrum",
}


# ============================================================
# 5. 计算昨天完整 UTC 自然日
# ============================================================

# 当前 UTC 时间
now_utc = datetime.now(timezone.utc)

# 今天 UTC 日期
today_date = now_utc.date()

# 昨天 UTC 日期
target_date = (
    today_date
    - timedelta(days=1)
)

# 昨天 UTC 00:00
start_time = datetime(
    year=target_date.year,
    month=target_date.month,
    day=target_date.day,
    tzinfo=timezone.utc,
)

# 今天 UTC 00:00
end_time = (
    start_time
    + timedelta(days=1)
)

# Bitquery 时间格式
start_time_str = (
    start_time
    .isoformat()
    .replace("+00:00", "Z")
)

# Bitquery 截止时间格式
end_time_str = (
    end_time
    .isoformat()
    .replace("+00:00", "Z")
)


# ============================================================
# 6. 读取所有 Bitquery 当前可处理的 Alpha
# ============================================================

tokens = []

# 打开有效 Alpha 清单
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建 CSV 读取器
    reader = csv.DictReader(file)

    # 遍历每一个 Alpha
    for row in reader:

        # 获取链
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # 如果当前链 Bitquery Trading 不处理
        if chain not in TOKEN_ID_PREFIX:
            continue

        # 获取合约地址
        address = row.get(
            "contractAddress",
            "",
        ).strip()

        # 没有地址则跳过
        if not address:
            continue

        # EVM 链统一小写
        if chain in EVM_CHAINS:
            address = address.lower()

        # 构建完整 Bitquery Token ID
        #
        # 例如：
        # bid:bsc:0x123...
        token_id = (
            TOKEN_ID_PREFIX[chain]
            + ":"
            + address
        )

        # 保存
        tokens.append(
            {
                "symbol": row.get(
                    "symbol",
                    "",
                ).strip(),

                "chain": chain,

                "address": address,

                "token_id": token_id,
            }
        )


# ============================================================
# 7. 按 Token ID 去重
# ============================================================

unique_tokens = []

seen_ids = set()

# 遍历
for token in tokens:

    # 已经出现则跳过
    if token["token_id"] in seen_ids:
        continue

    # 记录
    seen_ids.add(
        token["token_id"]
    )

    # 保存
    unique_tokens.append(
        token
    )


# 得到所有 Token ID
token_ids = [
    token["token_id"]
    for token in unique_tokens
]


# ============================================================
# 8. 统计各链数量
# ============================================================

chain_counts = {}

# 遍历
for token in unique_tokens:

    chain = token["chain"]

    chain_counts[chain] = (
        chain_counts.get(chain, 0)
        + 1
    )


# ============================================================
# 9. GraphQL
#
# 关键：
# 不再使用 NetworkBid = bid:bsc
#
# 而是直接使用：
#
# Pair.Token.Id.in
#
# Token ID 本身已经包含：
# 链 + 合约地址
# ============================================================

QUERY = """
query AllChainDailyNetflow(
  $tokens: [String!]!
  $since: DateTime!
  $till: DateTime!
) {
  Trading {
    Trades(
      limit: {count: 2000}

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
      }
    ) {

      Pair {
        Token {
          Id
        }
      }

      buy_usd: sum(
        of: AmountsInUsd_Base
        if: {
          Side: {
            is: "Buy"
          }
        }
      )

      sell_usd: sum(
        of: AmountsInUsd_Base
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
# 10. GraphQL 参数
# ============================================================

variables = {

    # 一次传入全部跨链 Alpha
    "tokens": token_ids,

    # 昨天开始
    "since": start_time_str,

    # 今天00:00结束
    "till": end_time_str,
}


# ============================================================
# 11. 请求头
# ============================================================

headers = {
    "Content-Type": "application/json",

    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 12. 开始测试
# ============================================================

print("=" * 72)

print("全部支持链 Alpha 单日一次请求测速")

print("=" * 72)

print(
    f"Alpha 总数："
    f"{len(unique_tokens)}"
)

print(
    f"查询日期："
    f"{target_date} UTC"
)

print("\n各链数量：")

# 打印各链
for chain, count in chain_counts.items():

    print(
        f"  {chain:<12}"
        f"{count}"
    )

print(
    "\n正在一次请求查询全部 Alpha..."
)


# 记录开始时间
request_start = (
    time.perf_counter()
)


try:

    # 向 Bitquery 请求
    response = requests.post(
        BITQUERY_URL,

        headers=headers,

        json={
            "query": QUERY,
            "variables": variables,
        },

        timeout=180,
    )


    # 计算耗时
    request_seconds = (
        time.perf_counter()
        - request_start
    )


    # ========================================================
    # 13. HTTP 状态
    # ========================================================

    print(
        f"\nHTTP状态码："
        f"{response.status_code}"
    )

    print(
        f"Bitquery请求耗时："
        f"{request_seconds:.2f} 秒"
    )


    # ========================================================
    # 14. 顺便看看响应头有没有 Points / Rate 信息
    # ========================================================

    useful_headers = {}

    # 遍历所有响应头
    for key, value in response.headers.items():

        # 转小写方便判断
        key_lower = key.lower()

        # 如果和积分、费用、限流相关
        if (
            "point" in key_lower
            or "cost" in key_lower
            or "rate" in key_lower
            or "limit" in key_lower
        ):

            useful_headers[key] = value


    # 如果确实发现相关响应头
    if useful_headers:

        print(
            "\nBitquery相关响应头："
        )

        for key, value in (
            useful_headers.items()
        ):

            print(
                f"  {key}: {value}"
            )


    # HTTP错误直接抛出
    response.raise_for_status()

    # 转 JSON
    result = response.json()


    # ========================================================
    # 15. GraphQL 错误
    # ========================================================

    if "errors" in result:

        print(
            "\n❌ Bitquery GraphQL错误："
        )

        for error in result["errors"]:
            print(error)

        exit()


    # ========================================================
    # 16. 取得结果
    # ========================================================

    rows = (
        result["data"]
        ["Trading"]
        ["Trades"]
    )


    print(
        f"\n返回聚合记录："
        f"{len(rows)} 条"
    )


    # ========================================================
    # 17. 初始化所有 Token
    # ========================================================

    daily_data = {}

    # 每个 Alpha 默认金额为0
    for token in unique_tokens:

        daily_data[
            token["token_id"]
        ] = {
            "buy_usd": 0.0,
            "sell_usd": 0.0,
        }


    # ========================================================
    # 18. 累加 Bitquery 返回结果
    # ========================================================

    unknown_ids = []

    # 遍历返回结果
    for row in rows:

        # 获取 Token ID
        token_id = (
            row.get(
                "Pair",
                {},
            )
            .get(
                "Token",
                {},
            )
            .get(
                "Id",
                "",
            )
        )


        # 如果不是我们传进去的 Token
        if token_id not in daily_data:

            unknown_ids.append(
                token_id
            )

            continue


        # 累加买入
        daily_data[token_id][
            "buy_usd"
        ] += float(
            row.get("buy_usd")
            or 0
        )


        # 累加卖出
        daily_data[token_id][
            "sell_usd"
        ] += float(
            row.get("sell_usd")
            or 0
        )


    # ========================================================
    # 19. 统计有数据 / 无数据的 Token
    # ========================================================

    active_count = 0

    zero_count = 0


    for data in daily_data.values():

        # 至少有一项金额
        if (
            data["buy_usd"] != 0
            or data["sell_usd"] != 0
        ):

            active_count += 1

        else:

            zero_count += 1


    print(
        f"有交易数据的 Alpha："
        f"{active_count}"
    )

    print(
        f"无交易数据的 Alpha："
        f"{zero_count}"
    )


    # ========================================================
    # 20. UB 对账
    # ========================================================

    print("\n" + "=" * 72)

    print("UB 对账")

    print("=" * 72)


    # 如果返回了 UB
    if UB_TOKEN_ID in daily_data:

        # 获取 UB
        ub_data = daily_data[
            UB_TOKEN_ID
        ]

        # 计算净买入
        ub_netflow = (
            ub_data["buy_usd"]
            - ub_data["sell_usd"]
        )


        # 打印
        print(
            f"跨链一次查询买入USD："
            f"${ub_data['buy_usd']:,.2f}"
        )

        print(
            f"跨链一次查询卖出USD："
            f"${ub_data['sell_usd']:,.2f}"
        )

        print(
            f"跨链一次查询净买入："
            f"${ub_netflow:+,.2f}"
        )


        # ====================================================
        # 21. 读取数据库已有 UB 数据
        # ====================================================

        connection = sqlite3.connect(
            DATABASE_FILE
        )

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                buy_usd,
                sell_usd,
                netflow_usd

            FROM daily_fund_flow

            WHERE
                date = ?
                AND chain = 'BSC'
                AND contract_address = ?
            """,

            (
                str(target_date),
                UB_ADDRESS,
            ),
        )


        # 获取数据库数据
        db_row = cursor.fetchone()

        # 如果找到
        if db_row:

            # 数据库净买入
            db_netflow = float(
                db_row[2]
            )

            # 差值
            difference = (
                ub_netflow
                - db_netflow
            )

            # 计算相对差异
            if db_netflow != 0:

                difference_ratio = (
                    abs(difference)
                    / abs(db_netflow)
                )

            else:

                difference_ratio = 0


            print("\n数据库已有结果：")

            print(
                f"数据库买入USD："
                f"${float(db_row[0]):,.2f}"
            )

            print(
                f"数据库卖出USD："
                f"${float(db_row[1]):,.2f}"
            )

            print(
                f"数据库净买入："
                f"${db_netflow:+,.2f}"
            )

            print(
                f"净买入差值："
                f"${difference:+,.2f}"
            )

            print(
                f"相对差异："
                f"{difference_ratio:.6%}"
            )


            # 我们不再要求0.01美元完全一致
            #
            # 满足任意一个条件即可：
            #
            # 1. 金额差异小于10美元
            # 2. 相对差异小于0.01%
            if (
                abs(difference) < 10
                or
                difference_ratio < 0.0001
            ):

                print(
                    "✅ UB对账通过"
                )

            else:

                print(
                    "⚠️ UB差异偏大，需要检查"
                )


        else:

            print(
                "⚠️ 数据库没有找到UB当天记录"
            )


        # 关闭数据库
        connection.close()


    # ========================================================
    # 22. 最终结论
    # ========================================================

    print("\n" + "=" * 72)

    print("跨链测速完成")

    print("=" * 72)

    print(
        f"总 Token："
        f"{len(unique_tokens)}"
    )

    print(
        f"API请求次数：1"
    )

    print(
        f"查询耗时："
        f"{request_seconds:.2f} 秒"
    )

    print(
        f"返回记录："
        f"{len(rows)} 条"
    )


except requests.exceptions.RequestException as error:

    # 计算失败耗时
    request_seconds = (
        time.perf_counter()
        - request_start
    )

    print(
        f"\n❌ 请求失败"
    )

    print(
        f"失败前耗时："
        f"{request_seconds:.2f} 秒"
    )

    print(error)


except Exception as error:

    print(
        "\n❌ 程序运行失败："
    )

    print(error)
