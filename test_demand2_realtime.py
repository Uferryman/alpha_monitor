# ============================================================
# 需求二 —— 15分钟钱包级实时查询测试
#
# 本程序只测试，不写数据库、不发Telegram。
#
# 测试目标：
# 1. 读取当前有效 Binance Alpha
# 2. 只保留 Bitquery 当前支持的6条链
# 3. 305个左右 Alpha 跨链一次请求
# 4. 查询最近15分钟
# 5. 按 Token + 钱包聚合
# 6. 同时取得：
#       - 钱包是否买入
#       - 买入金额
#       - 卖出金额
# 7. 看实际返回多少 Token+Wallet 组合
#
# 这一步非常重要：
# 它决定以后15分钟一次监控的速度、积分和存储量。
# ============================================================


# 导入 os，用于读取环境变量
import os

# 导入 csv，用于读取当前 Alpha 清单
import csv

# 导入 time，用于统计请求耗时
import time

# 导入 requests，用于调用 Bitquery
import requests

# 导入时间工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env 文件
from dotenv import load_dotenv


# ============================================================
# 1. 读取 Bitquery Token
# ============================================================

# 加载当前目录里的 .env
load_dotenv()

# 获取 BITQUERY_TOKEN
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 如果没有读取到 Token，直接停止
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 2. 基础配置
# ============================================================

# Bitquery 亚洲节点
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

# 当前有效 Alpha 文件
ALPHA_FILE = "alpha_tokens_active.csv"

# 最大允许返回多少条 Token+Wallet 聚合数据
#
# 先设20000。
# 如果真的达到20000，说明单次查询数据量过大，
# 我们再调整方案。
QUERY_LIMIT = 20000


# ============================================================
# 3. Binance链名 → Bitquery Token ID前缀
# ============================================================

TOKEN_ID_PREFIX = {
    "BSC": "bid:bsc",
    "Base": "bid:base",
    "Ethereum": "bid:eth",
    "Solana": "bid:solana",
    "Arbitrum": "bid:arbitrum",
    "TRON": "bid:tron",
}


# ============================================================
# 4. EVM链
# ============================================================

# EVM合约地址统一转成小写
EVM_CHAINS = {
    "BSC",
    "Base",
    "Ethereum",
    "Arbitrum",
}


# ============================================================
# 5. 读取全部当前有效Alpha
# ============================================================

tokens = []

# 打开Alpha文件
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建CSV读取器
    reader = csv.DictReader(file)

    # 一行一行读取
    for row in reader:

        # 获取链名称
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # 当前Bitquery不支持的链直接跳过
        if chain not in TOKEN_ID_PREFIX:
            continue

        # 获取Token名称
        symbol = row.get(
            "symbol",
            "",
        ).strip()

        # 获取合约地址
        address = row.get(
            "contractAddress",
            "",
        ).strip()

        # 没有地址就跳过
        if not address:
            continue

        # EVM链地址统一小写
        if chain in EVM_CHAINS:
            address = address.lower()

        # 构造完整Bitquery Token ID
        #
        # 例如：
        # bid:bsc:0x123...
        # bid:solana:xxxx...
        token_id = (
            TOKEN_ID_PREFIX[chain]
            + ":"
            + address
        )

        # 保存
        tokens.append(
            {
                "symbol": symbol,
                "chain": chain,
                "address": address,
                "token_id": token_id,
            }
        )


# ============================================================
# 6. Token去重
# ============================================================

unique_tokens = []

seen = set()

# 遍历全部Token
for token in tokens:

    # 已经存在则跳过
    if token["token_id"] in seen:
        continue

    # 记录已经出现
    seen.add(
        token["token_id"]
    )

    # 保存
    unique_tokens.append(
        token
    )


# ============================================================
# 7. 建立Token映射
# ============================================================

# Token ID → Token详细信息
token_map = {
    token["token_id"]: token
    for token in unique_tokens
}

# 提取全部Token ID
token_ids = list(
    token_map.keys()
)


# ============================================================
# 8. 查询最近15分钟
# ============================================================

# 当前UTC时间
end_time = datetime.now(
    timezone.utc
)

# 往前推15分钟
start_time = (
    end_time
    - timedelta(minutes=15)
)

# 转换成Bitquery需要的ISO时间
start_time_str = (
    start_time
    .isoformat()
    .replace("+00:00", "Z")
)

# 截止时间
end_time_str = (
    end_time
    .isoformat()
    .replace("+00:00", "Z")
)


# ============================================================
# 9. Bitquery查询
#
# 返回粒度：
#
# Token + Trader
#
# 例如：
#
# UB + 钱包A
# UB + 钱包B
# AIA + 钱包C
#
# 同一个钱包15分钟内交易很多次，
# 最终仍然聚合为一个 Token+Wallet 组合。
#
# buys：
# 这个钱包15分钟内买入该Token多少次
#
# buy_usd：
# 买入总金额
#
# sell_usd：
# 卖出总金额
# ============================================================

QUERY = """
query Demand2RealtimeTest(
  $tokens: [String!]!
  $since: DateTime!
  $till: DateTime!
  $limit: Int!
) {

  Trading {

    Trades(

      limit: {
        count: $limit
      }

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

      Trader {
        Address
      }

      buys: count(
        if: {
          Side: {
            is: "Buy"
          }
        }
      )

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
# 10. 查询参数
# ============================================================

variables = {

    # 一次放入全部支持的Alpha
    "tokens": token_ids,

    # 最近15分钟开始
    "since": start_time_str,

    # 当前时间结束
    "till": end_time_str,

    # 最大返回条数
    "limit": QUERY_LIMIT,
}


# ============================================================
# 11. 请求头
# ============================================================

headers = {

    # JSON格式
    "Content-Type": "application/json",

    # Bitquery Token
    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 12. 显示测试基本信息
# ============================================================

print("=" * 72)

print("需求二：15分钟钱包级查询测试")

print("=" * 72)

print(
    f"Alpha数量：{len(unique_tokens)}"
)

print(
    f"查询范围："
    f"{start_time_str}"
)

print(
    f"       → "
    f"{end_time_str}"
)

print(
    "\n正在一次查询全部Alpha..."
)


# ============================================================
# 13. 开始计时
# ============================================================

request_start = time.perf_counter()


try:

    # 发送Bitquery请求
    response = requests.post(
        BITQUERY_URL,

        headers=headers,

        json={
            "query": QUERY,
            "variables": variables,
        },

        timeout=180,
    )


    # 计算真实请求时间
    request_seconds = (
        time.perf_counter()
        - request_start
    )


    # ========================================================
    # 14. HTTP状态
    # ========================================================

    print(
        f"\nHTTP状态码："
        f"{response.status_code}"
    )

    print(
        f"查询耗时："
        f"{request_seconds:.2f} 秒"
    )


    # 如果HTTP失败，抛出异常
    response.raise_for_status()

    # 转成JSON
    result = response.json()


    # ========================================================
    # 15. GraphQL错误
    # ========================================================

    if "errors" in result:

        print(
            "\n❌ Bitquery GraphQL错误："
        )

        for error in result["errors"]:
            print(error)

        raise SystemExit


    # ========================================================
    # 16. 获取聚合结果
    # ========================================================

    rows = (
        result["data"]
        ["Trading"]
        ["Trades"]
    )


    print(
        f"返回 Token+Wallet 记录："
        f"{len(rows)} 条"
    )


    # ========================================================
    # 17. 判断有没有达到20000条上限
    # ========================================================

    if len(rows) >= QUERY_LIMIT:

        print(
            "⚠️ 已达到20000条查询上限"
        )

        print(
            "正式版不能直接这样查，"
            "需要进一步优化。"
        )

    else:

        print(
            "✅ 没有达到查询上限"
        )


    # ========================================================
    # 18. 初始化每个Token的数据
    # ========================================================

    token_stats = {}

    # 遍历全部Alpha
    for token in unique_tokens:

        # 每个Token初始化
        token_stats[
            token["token_id"]
        ] = {

            # 买入过的钱包集合
            "buyer_wallets": set(),

            # 15分钟买入USD
            "buy_usd": 0.0,

            # 15分钟卖出USD
            "sell_usd": 0.0,
        }


    # ========================================================
    # 19. 统计返回数据
    # ========================================================

    # 有买入但是拿不到钱包地址的记录数
    empty_wallet_rows = 0


    # 遍历每条Token+Wallet记录
    for row in rows:

        # 获取Token ID
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


        # 不是当前Alpha则跳过
        if token_id not in token_stats:
            continue


        # 获取钱包地址
        wallet = (
            row.get(
                "Trader",
                {},
            )
            .get(
                "Address",
                "",
            )
        )


        # 获取买入次数
        buys = int(
            row.get("buys")
            or 0
        )


        # 获取买入USD
        buy_usd = float(
            row.get("buy_usd")
            or 0
        )


        # 获取卖出USD
        sell_usd = float(
            row.get("sell_usd")
            or 0
        )


        # 累加Token总买入USD
        token_stats[
            token_id
        ][
            "buy_usd"
        ] += buy_usd


        # 累加Token总卖出USD
        token_stats[
            token_id
        ][
            "sell_usd"
        ] += sell_usd


        # 只要这个钱包有买入行为
        if buys > 0:

            # 钱包地址存在
            if wallet:

                # 加入集合
                #
                # set天然去重，
                # 所以同一个钱包买很多次也只算1个
                token_stats[
                    token_id
                ][
                    "buyer_wallets"
                ].add(
                    wallet
                )

            # 没钱包地址
            else:

                empty_wallet_rows += 1


    # ========================================================
    # 20. 生成15分钟排行榜
    # ========================================================

    ranking = []


    # 遍历全部Token
    for token_id, stats in (
        token_stats.items()
    ):

        # 找到Token信息
        token = token_map[
            token_id
        ]

        # 计算净买入
        netflow = (
            stats["buy_usd"]
            - stats["sell_usd"]
        )


        # 保存
        ranking.append(
            {
                "symbol": (
                    token["symbol"]
                ),

                "chain": (
                    token["chain"]
                ),

                "buyers": len(
                    stats[
                        "buyer_wallets"
                    ]
                ),

                "buy_usd": (
                    stats["buy_usd"]
                ),

                "sell_usd": (
                    stats["sell_usd"]
                ),

                "netflow": netflow,
            }
        )


    # ========================================================
    # 21. 按买入钱包数量排序
    # ========================================================

    ranking.sort(

        key=lambda item: item[
            "buyers"
        ],

        reverse=True,
    )


    # ========================================================
    # 22. 统计活跃Token数量
    # ========================================================

    active_tokens = sum(

        1

        for item in ranking

        if (
            item["buyers"] > 0

            or

            item["buy_usd"] != 0

            or

            item["sell_usd"] != 0
        )
    )


    print(
        f"15分钟内有交易的Alpha："
        f"{active_tokens}"
    )

    print(
        f"有买入但钱包地址为空："
        f"{empty_wallet_rows}"
    )


    # ========================================================
    # 23. 输出TOP20
    # ========================================================

    print(
        "\n15分钟买入钱包数 TOP20："
    )

    print(
        f"{'排名':<6}"
        f"{'Token':<16}"
        f"{'链':<12}"
        f"{'买入钱包':>12}"
        f"{'15m净买入':>20}"
    )

    print("-" * 70)


    # 只显示前20
    for index, item in enumerate(
        ranking[:20],
        start=1,
    ):

        print(
            f"{index:<6}"
            f"{item['symbol']:<16}"
            f"{item['chain']:<12}"
            f"{item['buyers']:>12}"
            f"${item['netflow']:>+19,.2f}"
        )


    # ========================================================
    # 24. 最终测试信息
    # ========================================================

    print("\n" + "=" * 72)

    print("测试完成")

    print("=" * 72)

    print(
        "Bitquery请求次数：1"
    )

    print(
        f"查询耗时："
        f"{request_seconds:.2f} 秒"
    )

    print(
        f"Token+Wallet记录："
        f"{len(rows)}"
    )

    print(
        f"上限：{QUERY_LIMIT}"
    )


except Exception as error:

    # 出现异常
    print(
        "\n❌ 测试失败："
    )

    print(error)
