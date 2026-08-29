# ============================================================
# 需求二 —— 15天钱包状态初始化体量测试
#
# 测试币：
# DEBIT
# TMX
# LAB
# UB
#
# 本程序只测试，不写数据库。
#
# 核心目的：
#
# 查询最近15天中：
#
# Token + Wallet
#
# 每一个组合只返回一行，并取得：
#
# last_buy_time = 这个钱包最近一次买这个Token的时间
#
# 例如：
#
# UB + 钱包A
# 15天内买了100次
#
# 最终希望只返回：
#
# UB + 钱包A + 最后一次买入时间
#
# ============================================================


# 导入os，用于读取环境变量
import os

# 导入csv，用于读取Alpha清单
import csv

# 导入time，用于测速
import time

# 导入requests，用于调用Bitquery
import requests

# 导入UTC时间工具
from datetime import datetime, timedelta, timezone

# 导入dotenv，用于读取.env
from dotenv import load_dotenv


# ============================================================
# 1. 加载Bitquery Token
# ============================================================

# 读取.env文件
load_dotenv()

# 获取BITQUERY_TOKEN
BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

# 如果没有读取到Token，直接停止
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

# 本次只测试这4个币
TEST_SYMBOLS = {
    "DEBIT",
    "TMX",
    "LAB",
    "UB",
}

# 单次最大返回25000行
QUERY_LIMIT = 25000


# ============================================================
# 3. Bitquery链前缀
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
# 5. 从Alpha清单找到4个测试币
# ============================================================

test_tokens = []

# 打开CSV
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建读取器
    reader = csv.DictReader(file)

    # 遍历
    for row in reader:

        # 获取币名
        symbol = row.get(
            "symbol",
            "",
        ).strip()

        # 不是测试币就跳过
        if symbol not in TEST_SYMBOLS:
            continue

        # 获取链
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # Bitquery不支持则跳过
        if chain not in TOKEN_ID_PREFIX:
            continue

        # 获取合约地址
        address = row.get(
            "contractAddress",
            "",
        ).strip()

        # 没有地址跳过
        if not address:
            continue

        # EVM链地址统一转小写
        if chain in EVM_CHAINS:

            address = address.lower()

        # 构建完整Token ID
        token_id = (
            TOKEN_ID_PREFIX[chain]
            + ":"
            + address
        )

        # 保存
        test_tokens.append(
            {
                "symbol": symbol,
                "chain": chain,
                "address": address,
                "token_id": token_id,
            }
        )


# ============================================================
# 6. 打印实际找到的测试币
# ============================================================

print("=" * 72)

print("需求二：15天钱包状态体量测试")

print("=" * 72)

print(
    f"找到测试币：{len(test_tokens)} 个"
)

# 逐个打印
for token in test_tokens:

    print(
        f"  {token['symbol']:<10}"
        f"{token['chain']:<12}"
        f"{token['address']}"
    )


# 如果没有找到4个，先停止
if len(test_tokens) != 4:

    print(
        "\n⚠️ 没有准确找到4个测试币，"
        "先不要继续消耗Bitquery积分。"
    )

    raise SystemExit


# ============================================================
# 7. 提取Token ID
# ============================================================

token_ids = [

    token["token_id"]

    for token in test_tokens
]


# ============================================================
# 8. 建立Token ID → 币名映射
# ============================================================

token_map = {

    token["token_id"]: token

    for token in test_tokens
}


# ============================================================
# 9. 计算最近15天时间窗口
#
# 这里只是建立“当前钱包状态”，
# 所以从当前时间向前精确15天即可。
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

# 转换为Bitquery格式
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
# 10. GraphQL
#
# 关键点：
#
# where中的Side = Buy
#
# 代表我们只关心钱包“买入Token”的行为，
# 不需要把卖出钱包也拉回来。
#
#
# 返回粒度：
#
# Pair.Token.Id
# +
# Trader.Address
#
# 因此同一个钱包15天内买同一个币很多次，
# 仍然只返回一个聚合组合。
#
#
# Block.Time(maximum: Block_Time)
#
# 用来得到这个Token+Wallet组合
# 最近一次买入时间。
# ============================================================

QUERY = """
query Test15DWalletState(
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
        last_buy_time: Time(
          maximum: Block_Time
        )
      }

      buy_count: count
    }
  }
}
"""


# ============================================================
# 11. 查询变量
# ============================================================

variables = {

    # 4个Token
    "tokens": token_ids,

    # 15天前
    "since": start_time_str,

    # 当前时间
    "till": end_time_str,

    # 最大25000行
    "limit": QUERY_LIMIT,
}


# ============================================================
# 12. HTTP请求头
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
# 13. 输出查询范围
# ============================================================

print(
    f"\n查询开始：{start_time_str}"
)

print(
    f"查询结束：{end_time_str}"
)

print(
    "\n正在一次查询4个币的15天钱包状态..."
)


# ============================================================
# 14. 开始测速
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


    # 计算耗时
    request_seconds = (
        time.perf_counter()
        - request_start
    )


    # ========================================================
    # 15. HTTP状态
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
    # 16. GraphQL错误
    # ========================================================

    if "errors" in result:

        print(
            "\n❌ Bitquery GraphQL错误："
        )

        for error in result["errors"]:

            print(error)

        raise SystemExit


    # ========================================================
    # 17. 获取结果
    # ========================================================

    rows = (
        result["data"]
        ["Trading"]
        ["Trades"]
    )

    # 返回总行数
    print(
        f"返回 Token+Wallet 状态："
        f"{len(rows):,} 条"
    )


    # ========================================================
    # 18. 判断是否撞上限
    # ========================================================

    hit_limit = (
        len(rows)
        >= QUERY_LIMIT
    )

    # 如果正好25000
    if hit_limit:

        print(
            "\n⚠️ 已达到25,000条上限"
        )

        print(
            "说明4个币15天不能一次完整拉取。"
        )

        print(
            "正式初始化需要自动拆分。"
        )

    # 没撞上限
    else:

        print(
            "✅ 没有达到25,000条上限"
        )


    # ========================================================
    # 19. 按Token统计返回钱包数
    #
    # 注意：
    # 如果已经撞25000上限，
    # 这里的币种数量只能作为参考，
    # 不能当完整数据。
    # ========================================================

    wallet_counts = {

        token["token_id"]: 0

        for token in test_tokens
    }


    # ========================================================
    # 20. 顺便统计买入次数
    # ========================================================

    buy_counts = {

        token["token_id"]: 0

        for token in test_tokens
    }


    # ========================================================
    # 21. 检查last_buy_time是否正常
    # ========================================================

    valid_last_buy = 0

    empty_wallet = 0


    # 遍历结果
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

        # 获取最后买入时间
        last_buy_time = (
            row.get(
                "Block",
                {},
            )
            .get(
                "last_buy_time",
                ""
            )
        )

        # 获取15天买入次数
        buy_count = int(
            row.get(
                "buy_count"
            )
            or 0
        )


        # 不属于目标Token就跳过
        if token_id not in wallet_counts:
            continue

        # 有钱包地址
        if wallet:

            wallet_counts[
                token_id
            ] += 1

        # 钱包为空
        else:

            empty_wallet += 1

        # 累加买入次数
        buy_counts[
            token_id
        ] += buy_count

        # 有最后买入时间
        if last_buy_time:

            valid_last_buy += 1


    # ========================================================
    # 22. 输出每个币结果
    # ========================================================

    print(
        "\n各Token 15天唯一钱包组合："
    )

    print(
        f"{'Token':<14}"
        f"{'链':<12}"
        f"{'钱包组合':>14}"
        f"{'买入次数':>18}"
    )

    print("-" * 62)


    # 遍历测试币
    for token in test_tokens:

        # Token ID
        token_id = (
            token["token_id"]
        )

        # 打印
        print(
            f"{token['symbol']:<14}"
            f"{token['chain']:<12}"
            f"{wallet_counts[token_id]:>14,}"
            f"{buy_counts[token_id]:>18,}"
        )


    # ========================================================
    # 23. 数据完整性检查
    # ========================================================

    print(
        f"\n有last_buy_time的记录："
        f"{valid_last_buy:,}"
    )

    print(
        f"钱包地址为空的记录："
        f"{empty_wallet:,}"
    )


    # ========================================================
    # 24. 粗略估算内存状态数据
    #
    #这里只打印行数，不凭空猜SQLite MB。
    #后面正式落库以后，我们直接看真实文件大小。
    # ========================================================

    if not hit_limit:

        print(
            "\n✅ 如果正式落库，"
            "这4个币只需要保存 "
            f"{len(rows):,} 条钱包状态，"
            "不会保存它们15天里的原始交易。"
        )


    # ========================================================
    # 25. 最终结论
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
        f"返回状态："
        f"{len(rows):,} / {QUERY_LIMIT:,}"
    )


    # 如果撞上限
    if hit_limit:

        print(
            "结论：需要拆分后再做正式15天初始化"
        )

    # 没撞上限
    else:

        print(
            "结论：这4个币可以一次完整初始化"
        )


except Exception as error:

    print(
        "\n❌ 测试失败："
    )

    print(error)
