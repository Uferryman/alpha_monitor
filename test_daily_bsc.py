# ============================================================
# BSC 234个 Binance Alpha —— 单次1天查询测速
#
# 目的：
# 1. 一次请求查询全部 BSC 有效 Alpha
# 2. 时间范围只查昨天1个完整UTC自然日
# 3. 不写数据库
# 4. 不修改埋伏池
# 5. 测试查询速度和稳定性
# ============================================================


# 导入 os，用于读取环境变量
import os

# 导入 csv，用于读取 Alpha 清单
import csv

# 导入 time，用于测速
import time

# 导入 sqlite3，只用于读取现有数据库做 UB 对账
import sqlite3

# 导入 requests，用于调用 Bitquery
import requests

# 导入日期工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env
from dotenv import load_dotenv


# ============================================================
# 1. 加载 Bitquery Token
# ============================================================

# 读取 .env
load_dotenv()

# 获取 Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 如果没有 Token，则停止
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 2. 基础配置
# ============================================================

# Bitquery 亚洲节点
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

# Alpha 清单
ALPHA_FILE = "alpha_tokens_active.csv"

# 现有数据库
DATABASE_FILE = "alpha_monitor.db"

# UB 合约，用于对账
UB_ADDRESS = (
    "0x40b8129b786d766267a7a118cf8c07e31cdb6fde"
)


# ============================================================
# 3. 计算昨天完整UTC自然日
# ============================================================

# 当前 UTC 时间
now_utc = datetime.now(timezone.utc)

# 今天 UTC 日期
today_date = now_utc.date()

# 昨天 UTC 日期
target_date = today_date - timedelta(days=1)

# 昨天 00:00 UTC
start_time = datetime(
    year=target_date.year,
    month=target_date.month,
    day=target_date.day,
    tzinfo=timezone.utc,
)

# 今天 00:00 UTC
end_time = start_time + timedelta(days=1)

# 转换为 Bitquery 时间格式
start_time_str = (
    start_time.isoformat().replace("+00:00", "Z")
)

# 转换截止时间
end_time_str = (
    end_time.isoformat().replace("+00:00", "Z")
)


# ============================================================
# 4. 读取全部 BSC 有效 Alpha
# ============================================================

bsc_tokens = []

# 打开 Alpha 清单
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # CSV读取器
    reader = csv.DictReader(file)

    # 遍历
    for row in reader:

        # 获取链
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # 只要 BSC
        if chain != "BSC":
            continue

        # 获取合约地址并转小写
        contract = (
            row.get(
                "contractAddress",
                "",
            )
            .strip()
            .lower()
        )

        # 没有合约就跳过
        if not contract:
            continue

        # 保存
        bsc_tokens.append(
            {
                "symbol": row.get(
                    "symbol",
                    "",
                ).strip(),

                "contract": contract,
            }
        )


# ============================================================
# 5. 去重
# ============================================================

unique_tokens = []

seen = set()

# 遍历
for token in bsc_tokens:

    # 地址已经出现就跳过
    if token["contract"] in seen:
        continue

    # 记录
    seen.add(
        token["contract"]
    )

    # 保存
    unique_tokens.append(
        token
    )


# 所有合约地址
token_addresses = [
    token["contract"]
    for token in unique_tokens
]


# ============================================================
# 6. GraphQL查询
#
# 这里只查1天。
#
# 不选择 Block.Date，
# 因为整个查询本身已经只覆盖一天。
#
# 结果只按 Token 地址聚合。
# ============================================================

QUERY = """
query DailyBSCAll(
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
            Address: {
              in: $tokens
            }
          }

          Market: {
            NetworkBid: {
              is: "bid:bsc"
            }
          }
        }
      }
    ) {

      Pair {
        Token {
          Address
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
# 7. 查询参数
# ============================================================

variables = {

    # 一次传全部 BSC Alpha 地址
    "tokens": token_addresses,

    # 昨天开始
    "since": start_time_str,

    # 昨天结束
    "till": end_time_str,
}


# ============================================================
# 8. 请求头
# ============================================================

headers = {

    # JSON
    "Content-Type": "application/json",

    # Bitquery Token
    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 9. 开始测速
# ============================================================

print("=" * 72)

print("BSC 全量 Alpha 单日一次请求测速")

print("=" * 72)

print(
    f"BSC Alpha 数量："
    f"{len(unique_tokens)}"
)

print(
    f"查询日期：{target_date} UTC"
)

print(
    f"查询范围："
    f"{start_time_str} → {end_time_str}"
)

print(
    "\n正在一次请求查询全部 BSC Alpha..."
)


# 记录开始时间
request_start = time.perf_counter()


try:

    # 向 Bitquery 发请求
    response = requests.post(
        BITQUERY_URL,
        headers=headers,
        json={
            "query": QUERY,
            "variables": variables,
        },
        timeout=180,
    )


    # 记录响应时间
    request_seconds = (
        time.perf_counter()
        - request_start
    )


    # 打印HTTP状态
    print(
        f"\nHTTP状态码："
        f"{response.status_code}"
    )

    # 打印真实请求耗时
    print(
        f"Bitquery请求耗时："
        f"{request_seconds:.2f} 秒"
    )


    # HTTP异常
    response.raise_for_status()

    # JSON
    result = response.json()


    # ========================================================
    # 10. 检查GraphQL错误
    # ========================================================

    if "errors" in result:

        print(
            "\n❌ Bitquery GraphQL错误："
        )

        for error in result["errors"]:

            print(error)

        exit()


    # ========================================================
    # 11. 获取结果
    # ========================================================

    rows = (
        result["data"]
        ["Trading"]
        ["Trades"]
    )


    print(
        f"返回聚合记录："
        f"{len(rows)} 条"
    )


    # ========================================================
    # 12. 累加同地址可能出现的多条记录
    # ========================================================

    daily_data = {}


    # 初始化234个币
    for token in unique_tokens:

        daily_data[
            token["contract"]
        ] = {
            "buy_usd": 0.0,
            "sell_usd": 0.0,
        }


    # 遍历Bitquery返回结果
    for row in rows:

        # 返回地址
        address = (
            row.get(
                "Pair",
                {},
            )
            .get(
                "Token",
                {},
            )
            .get(
                "Address",
                "",
            )
            .lower()
        )


        # 不属于当前 Alpha 就忽略
        if address not in daily_data:
            continue


        # 累加买入
        daily_data[address][
            "buy_usd"
        ] += float(
            row.get(
                "buy_usd"
            )
            or 0
        )


        # 累加卖出
        daily_data[address][
            "sell_usd"
        ] += float(
            row.get(
                "sell_usd"
            )
            or 0
        )


    # ========================================================
    # 13. 统计实际有交易的币
    # ========================================================

    active_count = 0

    zero_count = 0


    # 遍历
    for data in daily_data.values():

        # 有任何买卖金额
        if (
            data["buy_usd"] != 0
            or
            data["sell_usd"] != 0
        ):

            active_count += 1

        else:

            zero_count += 1


    print(
        f"有交易数据的 Alpha："
        f"{active_count}"
    )

    print(
        f"当天无交易的 Alpha："
        f"{zero_count}"
    )


    # ========================================================
    # 14. 显示UB当天结果
    # ========================================================

    if UB_ADDRESS in daily_data:

        # UB数据
        ub_data = daily_data[
            UB_ADDRESS
        ]

        # UB净买入
        ub_netflow = (
            ub_data["buy_usd"]
            - ub_data["sell_usd"]
        )


        print("\n" + "=" * 72)

        print("UB 当日查询结果")

        print("=" * 72)

        print(
            f"买入USD："
            f"${ub_data['buy_usd']:,.2f}"
        )

        print(
            f"卖出USD："
            f"${ub_data['sell_usd']:,.2f}"
        )

        print(
            f"净买入："
            f"${ub_netflow:+,.2f}"
        )


        # ====================================================
        # 15. 从现有数据库读取UB同一天
        #
        # 这里只读，不修改。
        # ====================================================

        try:

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

            db_row = cursor.fetchone()

            # 如果数据库存在昨天数据
            if db_row:

                print(
                    "\n数据库现有UB结果："
                )

                print(
                    f"买入USD："
                    f"${float(db_row[0]):,.2f}"
                )

                print(
                    f"卖出USD："
                    f"${float(db_row[1]):,.2f}"
                )

                print(
                    f"净买入："
                    f"${float(db_row[2]):+,.2f}"
                )


                # 对比差值
                difference = (
                    ub_netflow
                    - float(db_row[2])
                )

                print(
                    f"净买入差值："
                    f"${difference:+,.2f}"
                )


                # 差值极小
                if abs(difference) < 0.01:

                    print(
                        "✅ 单次234币查询 "
                        "与数据库结果一致"
                    )

                else:

                    print(
                        "⚠️ 单次查询与数据库"
                        "存在差异，需要检查"
                    )


            else:

                print(
                    "\n数据库中没有找到"
                    "UB当天记录。"
                )


            # 关闭数据库
            connection.close()


        except Exception as error:

            print(
                "\n数据库对账失败："
            )

            print(error)


    # ========================================================
    # 16. 最终结论
    # ========================================================

    print("\n" + "=" * 72)

    print("测速完成")

    print("=" * 72)

    print(
        f"234个BSC Alpha"
        f"只使用了 1 次 API 请求"
    )

    print(
        f"查询耗时："
        f"{request_seconds:.2f} 秒"
    )


except requests.exceptions.RequestException as error:

    # 计算失败耗时
    request_seconds = (
        time.perf_counter()
        - request_start
    )

    print(
        f"\n❌ 请求失败，"
        f"耗时 {request_seconds:.2f} 秒"
    )

    print(error)


except Exception as error:

    print(
        "\n❌ 程序运行失败："
    )

    print(error)
