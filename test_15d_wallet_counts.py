# ============================================================
# 需求二 —— 全部Alpha 15天唯一买入钱包数量统计
#
# 目的：
#
# 不拉取具体钱包地址。
#
# 只统计每个Token过去15天：
#
# 有多少个不同钱包买入过。
#
# 例如：
#
# UB      12,500
# TMX     35,000
# ABC        120
#
# 这样正式初始化时就能：
#
# 普通币 → 多个币合并查询
# 大币   → 单独查询
# 超大币 → 按时间拆分
#
# 本程序：
# 不写SQLite
# 不修改需求一
# 不修改埋伏池
#
# 只调用1次Bitquery。
# ============================================================


# 导入os，用于读取环境变量
import os

# 导入csv，用于读取Alpha清单
import csv

# 导入time，用于测速
import time

# 导入requests，用于调用Bitquery
import requests

# 导入时间工具
from datetime import datetime, timedelta, timezone

# 导入dotenv，用于读取.env
from dotenv import load_dotenv


# ============================================================
# 1. 加载Bitquery Token
# ============================================================

# 加载.env
load_dotenv()

# 读取BITQUERY_TOKEN
BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

# 如果没有Token就停止
if not BITQUERY_TOKEN:

    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 2. 基础配置
# ============================================================

# Bitquery亚洲节点
BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)

# 当前有效Alpha清单
ALPHA_FILE = (
    "alpha_tokens_active.csv"
)


# ============================================================
# 3. Bitquery支持链
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

EVM_CHAINS = {

    "BSC",

    "Base",

    "Ethereum",

    "Arbitrum",
}


# ============================================================
# 5. 读取全部支持的Alpha
# ============================================================

tokens = []

# 打开Alpha清单
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

        # Bitquery不支持的链跳过
        if chain not in TOKEN_ID_PREFIX:
            continue

        # 获取币名
        symbol = row.get(
            "symbol",
            "",
        ).strip()

        # 获取合约
        address = row.get(
            "contractAddress",
            "",
        ).strip()

        # 没有地址跳过
        if not address:
            continue

        # EVM地址统一小写
        if chain in EVM_CHAINS:

            address = (
                address.lower()
            )

        # 构造Bitquery完整Token ID
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
# 6. 去重
# ============================================================

unique_tokens = []

seen = set()

# 遍历Token
for token in tokens:

    # 已出现则跳过
    if token["token_id"] in seen:
        continue

    # 记录
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

# Token ID → Token信息
token_map = {

    token["token_id"]: token

    for token in unique_tokens
}

# 全部Token ID
token_ids = list(
    token_map.keys()
)


# ============================================================
# 8. 查询最近15天
# ============================================================

# 当前UTC时间
end_time = datetime.now(
    timezone.utc
)

# 往前15天
start_time = (
    end_time
    - timedelta(days=15)
)

# Bitquery开始时间
start_time_str = (
    start_time
    .isoformat()
    .replace("+00:00", "Z")
)

# Bitquery结束时间
end_time_str = (
    end_time
    .isoformat()
    .replace("+00:00", "Z")
)


# ============================================================
# 9. GraphQL
#
# 这里不返回Trader.Address。
#
# 而是直接让Bitquery计算：
#
# count(
#     distinct: Trader_Address
# )
#
# 因此每个Token最终只返回一行。
#
# where Side = Buy：
#
# 只统计真正买入该Token的钱包。
# ============================================================

QUERY = """
query Count15DUniqueBuyers(
  $tokens: [String!]!
  $since: DateTime!
  $till: DateTime!
) {

  Trading {

    Trades(

      limit: {
        count: 1000
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

      unique_buyers: count(
        distinct: Trader_Address
      )

      buy_trades: count
    }
  }
}
"""


# ============================================================
# 10. 查询变量
# ============================================================

variables = {

    # 所有支持Alpha
    "tokens": token_ids,

    # 15天前
    "since": start_time_str,

    # 当前时间
    "till": end_time_str,
}


# ============================================================
# 11. HTTP请求头
# ============================================================

headers = {

    "Content-Type": (
        "application/json"
    ),

    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 12. 输出基本信息
# ============================================================

print("=" * 72)

print("305个Alpha：15天唯一买入钱包统计")

print("=" * 72)

print(
    f"Alpha数量："
    f"{len(unique_tokens)}"
)

print(
    f"查询开始："
    f"{start_time_str}"
)

print(
    f"查询结束："
    f"{end_time_str}"
)

print(
    "\n正在查询..."
)


# ============================================================
# 13. 开始测速
# ============================================================

request_start = (
    time.perf_counter()
)


try:

    # 调用Bitquery
    response = requests.post(

        BITQUERY_URL,

        headers=headers,

        json={
            "query": QUERY,

            "variables": variables,
        },

        timeout=180,
    )


    # 查询耗时
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

    # HTTP错误
    response.raise_for_status()

    # 转JSON
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
    # 16. 获取返回数据
    # ========================================================

    rows = (
        result["data"]
        ["Trading"]
        ["Trades"]
    )

    print(
        f"返回Token统计："
        f"{len(rows)} 条"
    )


    # ========================================================
    # 17. 初始化全部Token
    #
    # 没有返回的Token按0处理
    # ========================================================

    stats = {}

    for token in unique_tokens:

        stats[
            token["token_id"]
        ] = {

            "symbol": (
                token["symbol"]
            ),

            "chain": (
                token["chain"]
            ),

            "address": (
                token["address"]
            ),

            "unique_buyers": 0,

            "buy_trades": 0,
        }


    # ========================================================
    # 18. 填入Bitquery统计结果
    # ========================================================

    for row in rows:

        # Token ID
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

        # 不是我们的Alpha则跳过
        if token_id not in stats:
            continue

        # 唯一买入钱包
        stats[token_id][
            "unique_buyers"
        ] = int(
            row.get(
                "unique_buyers"
            )
            or 0
        )

        # 买入交易次数
        stats[token_id][
            "buy_trades"
        ] = int(
            row.get(
                "buy_trades"
            )
            or 0
        )


    # ========================================================
    # 19. 转成排行榜
    # ========================================================

    ranking = list(
        stats.values()
    )

    # 按唯一钱包数量降序
    ranking.sort(

        key=lambda item: item[
            "unique_buyers"
        ],

        reverse=True,
    )


    # ========================================================
    # 20. 计算总Token+Wallet状态数量
    #
    # 这个数字非常重要。
    #
    # 因为正式数据库的：
    #
    # wallet_token_state
    #
    # 大致就是这个行数规模。
    # ========================================================

    total_wallet_states = sum(

        item["unique_buyers"]

        for item in ranking
    )


    # ========================================================
    # 21. 按规模分类
    # ========================================================

    over_30000 = [

        item

        for item in ranking

        if item[
            "unique_buyers"
        ] >= 30000
    ]


    over_20000 = [

        item

        for item in ranking

        if item[
            "unique_buyers"
        ] >= 20000
    ]


    over_10000 = [

        item

        for item in ranking

        if item[
            "unique_buyers"
        ] >= 10000
    ]


    over_5000 = [

        item

        for item in ranking

        if item[
            "unique_buyers"
        ] >= 5000
    ]


    # ========================================================
    # 22. 输出TOP30
    # ========================================================

    print(
        "\n15天唯一买入钱包 TOP30："
    )

    print(
        f"{'排名':<6}"
        f"{'Token':<18}"
        f"{'链':<12}"
        f"{'唯一钱包':>14}"
        f"{'买入次数':>16}"
    )

    print("-" * 70)


    for index, item in enumerate(
        ranking[:30],
        start=1,
    ):

        print(
            f"{index:<6}"
            f"{item['symbol']:<18}"
            f"{item['chain']:<12}"
            f"{item['unique_buyers']:>14,}"
            f"{item['buy_trades']:>16,}"
        )


    # ========================================================
    # 23. 输出整体规模
    # ========================================================

    print("\n" + "=" * 72)

    print("规模统计")

    print("=" * 72)

    print(
        f"全部Token+Wallet状态预计："
        f"{total_wallet_states:,} 条"
    )

    print(
        f">= 5,000钱包的Token："
        f"{len(over_5000)} 个"
    )

    print(
        f">= 10,000钱包的Token："
        f"{len(over_10000)} 个"
    )

    print(
        f">= 20,000钱包的Token："
        f"{len(over_20000)} 个"
    )

    print(
        f">= 30,000钱包的Token："
        f"{len(over_30000)} 个"
    )


    # ========================================================
    # 24. 输出超大币
    # ========================================================

    if over_20000:

        print(
            "\n需要特别处理的超大Token："
        )

        for item in over_20000:

            print(
                f"  {item['symbol']:<18}"
                f"{item['chain']:<12}"
                f"{item['unique_buyers']:>12,}"
            )


    # ========================================================
    # 25. 最终结果
    # ========================================================

    print("\n" + "=" * 72)

    print("统计完成")

    print("=" * 72)

    print(
        "Bitquery请求次数：1"
    )

    print(
        f"查询耗时："
        f"{request_seconds:.2f} 秒"
    )


except Exception as error:

    print(
        "\n❌ 查询失败："
    )

    print(error)
