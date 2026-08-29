# ============================================================
# Binance Alpha 埋伏池 —— BSC 10币批量测试
#
# 功能：
# 1. 从 alpha_tokens_active.csv 读取有效 Alpha
# 2. 选择 UB + 另外 9 个 BSC Alpha
# 3. 一次请求 Bitquery 获取这 10 个币最近 30 天每日资金数据
# 4. 将每日数据保存到 SQLite
# 5. 计算：
#       - 正净买入天数
#       - 正净买入比例
#       - 30D 总净买入
# 6. 筛选埋伏池：
#       - 正净买入天数 >= 20
#       - 30D 总净买入 > 50,000 USD
# ============================================================


# 导入 os，用于读取 .env
import os

# 导入 csv，用于读取 Alpha 清单和输出埋伏池
import csv

# 导入 sqlite3，用于保存每日汇总数据
import sqlite3

# 导入 requests，用于请求 Bitquery
import requests

# 导入日期工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env 文件
from dotenv import load_dotenv


# ============================================================
# 1. 基础配置
# ============================================================

# 加载 .env
load_dotenv()

# 获取 Bitquery Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 检查 Token
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env 文件"
    )


# Bitquery API 地址
BITQUERY_URL = "https://streaming.bitquery.io/graphql"


# Alpha 清单文件
ALPHA_FILE = "alpha_tokens_active.csv"


# SQLite 数据库文件
DATABASE_FILE = "alpha_monitor.db"


# 已经验证过的 UB 合约
UB_ADDRESS = (
    "0x40b8129b786d766267a7a118cf8c07e31cdb6fde"
)


# 本次只测试 10 个币
TEST_TOKEN_COUNT = 10


# 埋伏池最低正净买入天数
#
# 30天中：
# 20 / 30 = 66.67%
#
# 满足你要求的 >65%
MIN_POSITIVE_DAYS = 20


# 埋伏池最低 30D 累计净买入
MIN_NETFLOW_30D = 50000.0


# ============================================================
# 2. 计算最近 30 个完整 UTC 自然日
# ============================================================

# 获取当前 UTC 时间
now_utc = datetime.now(timezone.utc)


# 获取今天 UTC 00:00
today_utc = datetime(
    year=now_utc.year,
    month=now_utc.month,
    day=now_utc.day,
    tzinfo=timezone.utc,
)


# 查询截止时间：
# 今天 00:00
#
# 所以不会包含今天还没结束的数据
end_time = today_utc


# 往前推 30 天
start_time = end_time - timedelta(days=30)


# 转成 Bitquery 接受的格式
start_time_str = (
    start_time.isoformat().replace("+00:00", "Z")
)


# 转成 Bitquery 接受的格式
end_time_str = (
    end_time.isoformat().replace("+00:00", "Z")
)


# ============================================================
# 3. 从 Alpha 清单读取 BSC Token
# ============================================================

# 创建一个空列表
bsc_tokens = []


# 打开有效 Alpha 文件
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建 CSV 读取器
    reader = csv.DictReader(file)

    # 一行一行读取
    for row in reader:

        # 获取链名称
        chain_name = row.get(
            "chainName",
            "",
        ).strip()

        # 获取合约地址
        contract_address = row.get(
            "contractAddress",
            "",
        ).strip().lower()

        # 只保留 BSC
        if (
            chain_name == "BSC"
            and contract_address
        ):

            # 保存
            bsc_tokens.append(
                {
                    "symbol": row.get(
                        "symbol",
                        "",
                    ).strip(),

                    "chain": "BSC",

                    "contract": contract_address,
                }
            )


# ============================================================
# 4. 强制让 UB 进入测试组
# ============================================================

# UB 默认还没有找到
ub_token = None


# 其他 BSC Token
other_tokens = []


# 遍历所有 BSC Alpha
for token in bsc_tokens:

    # 如果是 UB
    if token["contract"] == UB_ADDRESS:

        # 保存 UB
        ub_token = token

    else:

        # 其他币放入另外一个列表
        other_tokens.append(token)


# 如果没有找到 UB
if ub_token is None:

    raise ValueError(
        "在 alpha_tokens_active.csv 中没有找到 UB"
    )


# 最终测试列表：
#
# UB
# +
# 前 9 个其他 BSC Alpha
test_tokens = [
    ub_token
] + other_tokens[
    : TEST_TOKEN_COUNT - 1
]


# ============================================================
# 5. 准备 Bitquery Token 地址列表
# ============================================================

# 创建合约地址列表
token_addresses = [
    token["contract"]
    for token in test_tokens
]


# ============================================================
# 6. GraphQL 批量查询
# ============================================================

# 这里最关键的是：
#
# Pair.Token.Address.in
#
# 它允许我们一次传入多个 Token 地址
#
# 同时选择：
#
# Pair.Token
# +
# Block.Date
#
# Bitquery 会按照：
#
# Token + 日期
#
# 进行聚合
QUERY = """
query BatchDailyNetflow(
  $tokens: [String!]!
  $since: DateTime!
  $till: DateTime!
) {
  Trading {
    Trades(
      limit: {count: 1000}

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

      orderBy: {
        ascending: Block_Date
      }
    ) {

      Block {
        Date
      }

      Pair {
        Token {
          Address
          Symbol
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
# 7. GraphQL 参数
# ============================================================

variables = {

    # 一次传入 10 个 Token 地址
    "tokens": token_addresses,

    # 开始时间
    "since": start_time_str,

    # 截止时间
    "till": end_time_str,
}


# ============================================================
# 8. 请求头
# ============================================================

headers = {

    # JSON 请求
    "Content-Type": "application/json",

    # Bitquery Token
    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 9. 显示测试币
# ============================================================

print("=" * 70)

print("BSC Alpha 10币批量测试")

print("=" * 70)


print(
    f"统计区间："
    f"{start_time.date()} 至 "
    f"{(end_time - timedelta(days=1)).date()}"
)


print(
    f"测试数量：{len(test_tokens)}"
)


print("\n测试 Token：")


# 打印本次测试币
for token in test_tokens:

    print(
        f"  {token['symbol']:<15}"
        f"{token['contract']}"
    )


print(
    "\n正在一次性查询这 10 个 Alpha..."
)


# ============================================================
# 10. 请求 Bitquery
# ============================================================

try:

    # 发送请求
    response = requests.post(
        BITQUERY_URL,

        headers=headers,

        json={
            "query": QUERY,
            "variables": variables,
        },

        timeout=120,
    )


    # 打印 HTTP 状态码
    print(
        "\nHTTP 状态码：",
        response.status_code,
    )


    # HTTP 错误直接抛出
    response.raise_for_status()


    # 转换 JSON
    result = response.json()


    # ========================================================
    # 11. 检查 GraphQL 错误
    # ========================================================

    if "errors" in result:

        print(
            "\n❌ Bitquery 返回错误："
        )

        for error in result["errors"]:

            print(error)

        exit()


    # ========================================================
    # 12. 获取查询结果
    # ========================================================

    rows = (
        result[
            "data"
        ][
            "Trading"
        ][
            "Trades"
        ]
    )


    # 打印返回多少组聚合数据
    print(
        f"Bitquery 返回聚合记录："
        f"{len(rows)} 条"
    )


    # ========================================================
    # 13. 建立 Token 信息映射
    # ========================================================

    # 用合约地址快速找到币名
    token_map = {}


    # 遍历测试 Token
    for token in test_tokens:

        # 地址作为 key
        token_map[
            token["contract"]
        ] = token


    # ========================================================
    # 14. 创建完整 10币 × 30天 数据
    # ========================================================

    # 创建空字典
    daily_data = {}


    # 遍历每一个 Token
    for token in test_tokens:

        # 遍历最近 30 天
        for i in range(30):

            # 当前日期
            current_date = (
                start_time
                + timedelta(days=i)
            ).date()

            # 创建唯一键：
            #
            # 合约 + 日期
            key = (
                token["contract"],
                str(current_date),
            )


            # 默认当天没有成交
            daily_data[key] = {

                "symbol": token["symbol"],

                "chain": token["chain"],

                "contract": token[
                    "contract"
                ],

                "date": str(current_date),

                "buy_usd": 0.0,

                "sell_usd": 0.0,

                "netflow_usd": 0.0,
            }


    # ========================================================
    # 15. 将 Bitquery 返回数据填进去
    # ========================================================

    for row in rows:

        # 获取 Token
        pair_token = (
            row.get("Pair", {})
            .get("Token", {})
        )


        # 获取地址
        contract = str(
            pair_token.get(
                "Address",
                "",
            )
        ).lower()


        # 获取日期
        date_str = str(
            row.get(
                "Block",
                {},
            ).get(
                "Date",
                "",
            )
        )[:10]


        # 创建键
        key = (
            contract,
            date_str,
        )


        # 如果属于我们的数据范围
        if key in daily_data:

            # 买入 USD
            buy_usd = float(
                row.get(
                    "buy_usd"
                )
                or 0
            )


            # 卖出 USD
            sell_usd = float(
                row.get(
                    "sell_usd"
                )
                or 0
            )


            # 计算净买入
            netflow_usd = (
                buy_usd
                - sell_usd
            )


            # 更新数据
            daily_data[key][
                "buy_usd"
            ] = buy_usd


            daily_data[key][
                "sell_usd"
            ] = sell_usd


            daily_data[key][
                "netflow_usd"
            ] = netflow_usd


    # ========================================================
    # 16. 打开 SQLite 数据库
    # ========================================================

    # 如果 alpha_monitor.db 不存在
    # SQLite 会自动创建
    connection = sqlite3.connect(
        DATABASE_FILE
    )


    # 创建数据库操作对象
    cursor = connection.cursor()


    # ========================================================
    # 17. 创建每日资金数据表
    # ========================================================

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_fund_flow (

            date TEXT NOT NULL,

            symbol TEXT NOT NULL,

            chain TEXT NOT NULL,

            contract_address TEXT NOT NULL,

            buy_usd REAL NOT NULL,

            sell_usd REAL NOT NULL,

            netflow_usd REAL NOT NULL,

            updated_at TEXT NOT NULL,

            PRIMARY KEY (
                date,
                chain,
                contract_address
            )
        )
        """
    )


    # ========================================================
    # 18. 保存每日数据
    # ========================================================

    # 获取当前更新时间
    updated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    # 遍历所有每日数据
    for data in daily_data.values():

        # 写入数据库
        #
        # 如果同一天、同一个币已经存在
        # 就更新原来的数据
        cursor.execute(
            """
            INSERT OR REPLACE INTO daily_fund_flow (

                date,
                symbol,
                chain,
                contract_address,
                buy_usd,
                sell_usd,
                netflow_usd,
                updated_at

            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,

            (
                data["date"],

                data["symbol"],

                data["chain"],

                data["contract"],

                data["buy_usd"],

                data["sell_usd"],

                data["netflow_usd"],

                updated_at,
            ),
        )


    # 提交保存
    connection.commit()


    # ========================================================
    # 19. 计算每个币的 30D 指标
    # ========================================================

    # 保存最终汇总结果
    summaries = []


    # 遍历测试 Token
    for token in test_tokens:

        # 查询这个币最近 30 天数据
        cursor.execute(
            """
            SELECT

                COUNT(
                    CASE
                        WHEN netflow_usd > 0
                        THEN 1
                    END
                ) AS positive_days,

                SUM(buy_usd),

                SUM(sell_usd),

                SUM(netflow_usd)

            FROM daily_fund_flow

            WHERE
                chain = ?
                AND contract_address = ?
                AND date >= ?
                AND date < ?
            """,

            (
                token["chain"],

                token["contract"],

                str(start_time.date()),

                str(end_time.date()),
            ),
        )


        # 取得查询结果
        summary = cursor.fetchone()


        # 正净买入天数
        positive_days = (
            summary[0]
            or 0
        )


        # 30D 总买入
        total_buy = float(
            summary[1]
            or 0
        )


        # 30D 总卖出
        total_sell = float(
            summary[2]
            or 0
        )


        # 30D 总净买入
        total_netflow = float(
            summary[3]
            or 0
        )


        # 正净买入比例
        positive_ratio = (
            positive_days
            / 30
        )


        # ====================================================
        # 20. 判断是否进入埋伏池
        # ====================================================

        is_ambush = (

            # 30天至少20天正净买入
            positive_days
            >= MIN_POSITIVE_DAYS

            and

            # 30D净买入超过5万美元
            total_netflow
            > MIN_NETFLOW_30D
        )


        # 保存汇总
        summaries.append(
            {
                "symbol": token[
                    "symbol"
                ],

                "chain": token[
                    "chain"
                ],

                "contract": token[
                    "contract"
                ],

                "positive_days": (
                    positive_days
                ),

                "positive_ratio": (
                    positive_ratio
                ),

                "netflow_30d": (
                    total_netflow
                ),

                "is_ambush": (
                    is_ambush
                ),
            }
        )


    # ========================================================
    # 21. 按30D净买入从高到低排序
    # ========================================================

    summaries.sort(
        key=lambda x: x[
            "netflow_30d"
        ],

        reverse=True,
    )


    # ========================================================
    # 22. 终端只显示汇总，不显示每日明细
    # ========================================================

    print("\n" + "=" * 85)

    print(
        "30D Alpha 埋伏池筛选结果"
    )

    print("=" * 85)


    # 打印表头
    print(
        f"{'Token':<15}"
        f"{'正流入天数':>12}"
        f"{'比例':>12}"
        f"{'30D净买入':>20}"
        f"{'结果':>12}"
    )


    # 横线
    print("-" * 85)


    # 逐个显示
    for item in summaries:

        # 判断文字
        status = (
            "✅ 埋伏"
            if item["is_ambush"]
            else "❌"
        )


        # 打印
        print(
            f"{item['symbol']:<15}"
            f"{item['positive_days']:>12}"
            f"{item['positive_ratio']:>11.2%}"
            f"${item['netflow_30d']:>+19,.2f}"
            f"{status:>12}"
        )


    # ========================================================
    # 23. 检查 UB 是否仍然正确
    # ========================================================

    print("\n" + "=" * 85)

    print("UB 对账检查")

    print("=" * 85)


    # 找到 UB
    for item in summaries:

        if (
            item["contract"]
            == UB_ADDRESS
        ):

            print(
                f"正净买入天数："
                f"{item['positive_days']}"
            )

            print(
                f"正净买入比例："
                f"{item['positive_ratio']:.2%}"
            )

            print(
                f"30D净买入："
                f"${item['netflow_30d']:+,.2f}"
            )


            # 与之前单币测试做大致检查
            #
            # 之前：
            # 26天
            # 86.67%
            # +$3,132,728 左右
            if (
                item[
                    "positive_days"
                ]
                == 26
            ):

                print(
                    "✅ UB 正流入天数与单币版一致"
                )

            else:

                print(
                    "⚠️ UB 与之前单币结果不一致"
                )


    # ========================================================
    # 24. 输出测试埋伏池 CSV
    # ========================================================

    # 只保留进入埋伏池的 Token
    ambush_tokens = [
        item
        for item in summaries
        if item["is_ambush"]
    ]


    # 创建 CSV
    with open(
        "test_ambush_pool.csv",
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        # 定义字段
        fieldnames = [
            "symbol",
            "chain",
            "contract",
            "positive_days",
            "positive_ratio",
            "netflow_30d",
        ]


        # 创建写入器
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )


        # 写表头
        writer.writeheader()


        # 写埋伏币
        for item in ambush_tokens:

            # 创建一行
            row = item.copy()

            # 删除不需要输出的字段
            row.pop(
                "is_ambush",
                None,
            )

            # 比例改成百分数
            row["positive_ratio"] = (
                f"{item['positive_ratio']:.2%}"
            )


            # 写入
            writer.writerow(row)


    # ========================================================
    # 25. 完成
    # ========================================================

    print("\n" + "=" * 85)

    print(
        f"本次测试："
        f"{len(test_tokens)} 个 Alpha"
    )

    print(
        f"进入埋伏池："
        f"{len(ambush_tokens)} 个"
    )


    print(
        "\n✅ 每日资金数据已保存："
        "alpha_monitor.db"
    )


    print(
        "✅ 埋伏池测试结果："
        "test_ambush_pool.csv"
    )


    # 关闭数据库
    connection.close()


# ============================================================
# 26. 网络异常处理
# ============================================================

except requests.exceptions.RequestException as error:

    print(
        "\n❌ Bitquery 请求失败："
    )

    print(error)


# ============================================================
# 27. 其他异常
# ============================================================

except Exception as error:

    print(
        "\n❌ 程序运行失败："
    )

    print(error)
