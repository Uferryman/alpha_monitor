# ============================================================
# Binance Alpha 埋伏池正式版
#
# 功能：
# 1. 读取 Binance 有效 Alpha 清单
# 2. 按链、按批查询 Bitquery 最近30个完整自然日
# 3. 保存每个币每天：
#       买入USD
#       卖出USD
#       净买入USD
# 4. 筛选埋伏池：
#       正净买入天数 >= 20天
#       30D累计净买入 > 50,000美元
# 5. 最终 alpha_ambush_pool.csv
#    只输出真正进入埋伏池的币
#
# 注意：
# 不保存每笔Swap。
# 每日汇总长期保存在SQLite中，
# 后面可以直接用于资金趋势图。
# ============================================================


# 导入 os，用于读取 .env
import os

# 导入 csv，用于读取 Alpha 清单和输出结果
import csv

# 导入 sqlite3，用于保存每日资金数据
import sqlite3

# 导入 time，用于控制请求频率
import time

# 导入 requests，用于调用 Bitquery
import requests

# 导入日期工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env
from dotenv import load_dotenv


# ============================================================
# 1. 基础配置
# ============================================================

# 加载当前目录下的 .env
load_dotenv()

# 读取 Bitquery Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 如果没有读取到 Token，则停止
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env 文件"
    )


# 使用 Bitquery 亚洲节点
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

# Binance 有效 Alpha 清单
ALPHA_FILE = "alpha_tokens_active.csv"

# SQLite 数据库
DATABASE_FILE = "alpha_monitor.db"

# 最终埋伏池结果
OUTPUT_FILE = "alpha_ambush_pool.csv"


# ============================================================
# 2. 埋伏池规则
# ============================================================

# 最近30天中：
# 净买入 > 0 的天数至少20天
MIN_POSITIVE_DAYS = 20

# 最近30天累计净买入必须大于5万美元
MIN_NETFLOW_30D = 50000.0


# ============================================================
# 3. 批量查询设置
# ============================================================

# 每批30个币
BATCH_SIZE = 30

# 正常批次之间等待10秒
# 主要用于减少429限流
REQUEST_INTERVAL = 10

# 每切换一条链额外等待15秒
CHAIN_INTERVAL = 15

# 单批最多尝试4次
MAX_RETRIES = 4


# ============================================================
# 4. Bitquery当前使用的链映射
# ============================================================

SUPPORTED_CHAINS = {
    "BSC": "bid:bsc",
    "Base": "bid:base",
    "Ethereum": "bid:eth",
    "Solana": "bid:solana",
    "Arbitrum": "bid:arbitrum",
    "TRON": "bid:tron",
}


# ============================================================
# 5. EVM链
# ============================================================

# EVM合约地址统一使用小写
EVM_CHAINS = {
    "BSC",
    "Base",
    "Ethereum",
    "Arbitrum",
}


# ============================================================
# 6. 地址标准化函数
# ============================================================

def normalize_address(chain, address):

    # 转成字符串并去掉前后空格
    address = str(address).strip()

    # EVM地址统一转小写
    if chain in EVM_CHAINS:
        return address.lower()

    # Solana、TRON保持原样
    return address


# ============================================================
# 7. 计算最近30个完整UTC自然日
# ============================================================

# 获取当前UTC时间
now_utc = datetime.now(timezone.utc)

# 得到今天UTC的00:00
today_utc = datetime(
    year=now_utc.year,
    month=now_utc.month,
    day=now_utc.day,
    tzinfo=timezone.utc,
)

# 截止到今天00:00
# 因此今天尚未结束的数据不会参与计算
end_time = today_utc

# 往前推30天
start_time = end_time - timedelta(days=30)

# 转换为Bitquery接受的时间格式
start_time_str = (
    start_time.isoformat().replace("+00:00", "Z")
)

# 转换截止时间
end_time_str = (
    end_time.isoformat().replace("+00:00", "Z")
)


# ============================================================
# 8. Bitquery批量查询
# ============================================================

# 这里特别注意：
#
# Pair.Token 中只取 Address
#
# 不再取 Symbol。
#
# 因为币名我们已经从 Binance Alpha 接口拿到了，
# 不需要 Bitquery 再返回。
#
# 这样可以避免同一个合约由于 Symbol 元数据不同，
# 被 Bitquery 分成多条日聚合记录。
QUERY = """
query BatchDailyNetflow(
  $tokens: [String!]!
  $network: String!
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
              is: $network
            }
          }
        }
      }
    ) {

      Block {
        Date
      }

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
# 9. HTTP请求头
# ============================================================

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BITQUERY_TOKEN}",
}


# ============================================================
# 10. 读取有效Alpha清单
# ============================================================

all_tokens = []

# 打开有效Alpha CSV
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建CSV读取器
    reader = csv.DictReader(file)

    # 逐行读取
    for row in reader:

        # 获取链
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # 获取并标准化合约地址
        contract = normalize_address(
            chain,
            row.get(
                "contractAddress",
                "",
            ),
        )

        # 获取币名
        symbol = row.get(
            "symbol",
            "",
        ).strip()

        # 没有地址就跳过
        if not contract:
            continue

        # 保存
        all_tokens.append(
            {
                "symbol": symbol,
                "chain": chain,
                "contract": contract,
            }
        )


# ============================================================
# 11. 去除重复Alpha
# ============================================================

unique_tokens = []

# 用集合记录已经出现的：
# 链 + 合约地址
seen_tokens = set()

# 遍历
for token in all_tokens:

    # 组成唯一键
    key = (
        token["chain"],
        token["contract"],
    )

    # 如果已经出现过就跳过
    if key in seen_tokens:
        continue

    # 记录
    seen_tokens.add(key)

    # 保存
    unique_tokens.append(token)


# ============================================================
# 12. 区分Bitquery支持与暂不支持
# ============================================================

supported_tokens = []

unsupported_tokens = []

# 遍历全部有效Alpha
for token in unique_tokens:

    # 当前支持
    if token["chain"] in SUPPORTED_CHAINS:

        supported_tokens.append(token)

    # Sui、Sonic等暂不处理
    else:

        unsupported_tokens.append(token)


# ============================================================
# 13. 按链分组
# ============================================================

tokens_by_chain = {}

# 遍历支持的币
for token in supported_tokens:

    # 获取链
    chain = token["chain"]

    # 第一次出现就创建列表
    if chain not in tokens_by_chain:
        tokens_by_chain[chain] = []

    # 加入对应链
    tokens_by_chain[chain].append(token)


# ============================================================
# 14. 打开SQLite数据库
# ============================================================

# 如果数据库不存在会自动创建
connection = sqlite3.connect(
    DATABASE_FILE
)

# 创建数据库游标
cursor = connection.cursor()


# ============================================================
# 15. 创建每日资金表
# ============================================================

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


# ============================================================
# 16. 创建数据库索引
# ============================================================

cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS
    idx_daily_fund_flow_token_date

    ON daily_fund_flow (
        chain,
        contract_address,
        date
    )
    """
)

# 保存数据库结构
connection.commit()


# ============================================================
# 17. 请求一个批次
# ============================================================

def query_batch(
    chain,
    network_bid,
    batch_tokens,
):

    # 当前批次的全部合约地址
    token_addresses = [
        token["contract"]
        for token in batch_tokens
    ]

    # GraphQL变量
    variables = {
        "tokens": token_addresses,
        "network": network_bid,
        "since": start_time_str,
        "till": end_time_str,
    }


    # 最多尝试MAX_RETRIES次
    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            # 发送请求
            response = requests.post(
                BITQUERY_URL,
                headers=HEADERS,
                json={
                    "query": QUERY,
                    "variables": variables,
                },
                timeout=120,
            )


            # =================================================
            # 如果遇到429限流
            # =================================================

            if response.status_code == 429:

                # 先看看服务器有没有告诉我们等待多久
                retry_after = response.headers.get(
                    "Retry-After"
                )

                # 如果服务器给了数字
                if (
                    retry_after
                    and retry_after.isdigit()
                ):

                    wait_seconds = int(
                        retry_after
                    )

                # 如果没有给，就自己逐步增加等待时间
                else:

                    wait_seconds = (
                        15 * attempt
                    )

                print(
                    f"    ⚠️ 触发429限流，"
                    f"等待 {wait_seconds} 秒后重试..."
                )

                # 等待
                time.sleep(wait_seconds)

                # 重新进入下一次循环
                continue


            # 其他HTTP错误
            response.raise_for_status()

            # 转换JSON
            result = response.json()


            # =================================================
            # 检查GraphQL错误
            # =================================================

            if "errors" in result:

                raise RuntimeError(
                    str(result["errors"])
                )


            # 获取返回数据
            rows = (
                result["data"]
                ["Trading"]
                ["Trades"]
            )


            # =================================================
            # 数据量简单检查
            # =================================================

            # 理论最多：
            #
            # Token数量 × 30天
            expected_max_rows = (
                len(batch_tokens)
                * 30
            )

            # 如果仍然超过理论数量，
            # 打印警告，但不会直接停止，
            # 因为下面还有“累加”保护。
            if len(rows) > expected_max_rows:

                print(
                    f"    ⚠️ 返回 {len(rows)} 条，"
                    f"理论上限 {expected_max_rows} 条，"
                    f"程序将自动累加重复记录。"
                )


            # 成功返回
            return rows


        except Exception as error:

            # 如果已经最后一次
            if attempt >= MAX_RETRIES:

                # 抛出错误
                raise

            # 非429错误时稍等再试
            wait_seconds = 5 * attempt

            print(
                f"    ⚠️ 第 {attempt} 次请求失败："
                f"{error}"
            )

            print(
                f"    等待 {wait_seconds} 秒后重试..."
            )

            # 等待
            time.sleep(wait_seconds)


    # 理论上不会走到这里
    raise RuntimeError(
        f"{chain} 批次查询失败"
    )


# ============================================================
# 18. 保存一个批次的每日数据
# ============================================================

def save_batch(
    chain,
    batch_tokens,
    rows,
):

    # 地址 → Token信息
    token_map = {}

    # 建立映射
    for token in batch_tokens:

        # 标准化地址
        contract = normalize_address(
            chain,
            token["contract"],
        )

        # 保存
        token_map[contract] = token


    # ========================================================
    # 先创建完整的：
    #
    # Token × 30天
    #
    # 没有成交的日子默认全部为0
    # ========================================================

    daily_data = {}

    # 遍历所有Token
    for token in batch_tokens:

        # 遍历30天
        for i in range(30):

            # 得到日期
            current_date = (
                start_time
                + timedelta(days=i)
            ).date()

            # 唯一键
            key = (
                token["contract"],
                str(current_date),
            )

            # 默认值
            daily_data[key] = {
                "symbol": token["symbol"],
                "chain": chain,
                "contract": token["contract"],
                "date": str(current_date),
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "netflow_usd": 0.0,
            }


    # ========================================================
    # 将Bitquery数据填进去
    # ========================================================

    for row in rows:

        # 获取Token数据
        pair_token = (
            row.get("Pair", {})
            .get("Token", {})
        )

        # 获取并标准化返回的地址
        returned_address = normalize_address(
            chain,
            pair_token.get(
                "Address",
                "",
            ),
        )

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

        # 返回的Token不属于当前批次就忽略
        if returned_address not in token_map:
            continue

        # 唯一键
        key = (
            returned_address,
            date_str,
        )

        # 不在最近30天范围则忽略
        if key not in daily_data:
            continue

        # 获取本行买入USD
        buy_usd = float(
            row.get(
                "buy_usd"
            )
            or 0
        )

        # 获取本行卖出USD
        sell_usd = float(
            row.get(
                "sell_usd"
            )
            or 0
        )


        # ====================================================
        # 这里是这次最重要的修复
        # ====================================================
        #
        # 之前使用：
        #
        # daily_data[key]["buy_usd"] = buy_usd
        #
        # 如果同一个币同一天出现第二行，
        # 第二行会覆盖第一行。
        #
        # 现在改成 +=
        #
        # 即使出现重复聚合记录，
        # 也全部累加起来。
        # ====================================================

        # 累加买入金额
        daily_data[key]["buy_usd"] += (
            buy_usd
        )

        # 累加卖出金额
        daily_data[key]["sell_usd"] += (
            sell_usd
        )

        # 用累计后的买入减累计后的卖出
        daily_data[key]["netflow_usd"] = (

            daily_data[key]["buy_usd"]

            -

            daily_data[key]["sell_usd"]
        )


    # ========================================================
    # 保存到SQLite
    # ========================================================

    # 当前更新时间
    updated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # 遍历所有30天数据
    for data in daily_data.values():

        # 写入数据库
        #
        # 相同：
        # 日期 + 链 + 合约
        #
        # 会覆盖旧数据
        cursor.execute(
            """
            INSERT OR REPLACE INTO
            daily_fund_flow (

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

    # 每处理完一个批次立即保存
    connection.commit()


# ============================================================
# 19. 开始运行
# ============================================================

print("=" * 72)

print("Binance Alpha 埋伏池正式版")

print("=" * 72)

# 打印日期区间
print(
    f"统计区间："
    f"{start_time.date()} 至 "
    f"{(end_time - timedelta(days=1)).date()}"
)

# 有效Alpha总数
print(
    f"有效 Alpha 总数："
    f"{len(unique_tokens)}"
)

# 可处理数量
print(
    f"Bitquery 可处理："
    f"{len(supported_tokens)}"
)

# 暂不支持数量
print(
    f"暂不支持："
    f"{len(unsupported_tokens)}"
)

# 埋伏规则
print(
    f"埋伏条件："
    f"正净买入 >= {MIN_POSITIVE_DAYS} 天"
    f" 且 30D净买入 > "
    f"${MIN_NETFLOW_30D:,.0f}"
)


# ============================================================
# 20. 保存本次成功处理的Token
# ============================================================

processed_tokens = []


# ============================================================
# 21. 按链查询
# ============================================================

# 按我们指定的链顺序处理
for chain in SUPPORTED_CHAINS:

    # 如果当前没有这条链的Alpha
    if chain not in tokens_by_chain:
        continue

    # 当前链所有币
    chain_tokens = tokens_by_chain[
        chain
    ]

    # Bitquery网络ID
    network_bid = SUPPORTED_CHAINS[
        chain
    ]

    # 总批次数
    total_batches = (
        len(chain_tokens)
        + BATCH_SIZE
        - 1
    ) // BATCH_SIZE

    print("\n" + "-" * 72)

    print(
        f"{chain}："
        f"{len(chain_tokens)} 个 Alpha，"
        f"共 {total_batches} 批"
    )

    print("-" * 72)


    # ========================================================
    # 当前链按批循环
    # ========================================================

    for start_index in range(
        0,
        len(chain_tokens),
        BATCH_SIZE,
    ):

        # 当前批次
        batch_tokens = chain_tokens[
            start_index:
            start_index + BATCH_SIZE
        ]

        # 当前批次编号
        batch_number = (
            start_index
            // BATCH_SIZE
            + 1
        )

        # 打印进度
        print(
            f"  正在处理 "
            f"{batch_number}/{total_batches}："
            f"{len(batch_tokens)} 个币..."
        )


        try:

            # 请求Bitquery
            rows = query_batch(
                chain,
                network_bid,
                batch_tokens,
            )

            # 保存到数据库
            save_batch(
                chain,
                batch_tokens,
                rows,
            )

            # 标记本次成功处理
            processed_tokens.extend(
                batch_tokens
            )

            # 打印成功
            print(
                f"    ✅ 成功，"
                f"返回 {len(rows)} 条日聚合数据"
            )


        except Exception as error:

            # 当前批次失败
            print(
                f"    ❌ 当前批次失败："
                f"{error}"
            )


        # 如果当前链还有下一批
        if (
            start_index + BATCH_SIZE
            < len(chain_tokens)
        ):

            # 控制请求频率
            time.sleep(
                REQUEST_INTERVAL
            )


    # 一条链处理完以后稍微等待
    time.sleep(
        CHAIN_INTERVAL
    )


# ============================================================
# 22. 计算30D指标
# ============================================================

summaries = []

# 遍历本轮成功处理的Token
for token in processed_tokens:

    # 从SQLite计算30天结果
    cursor.execute(
        """
        SELECT

            COUNT(
                CASE
                    WHEN netflow_usd > 0
                    THEN 1
                END
            ),

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

    # 获取查询结果
    result = cursor.fetchone()

    # 正净买入天数
    positive_days = (
        result[0]
        or 0
    )

    # 30天累计净买入
    netflow_30d = float(
        result[1]
        or 0
    )

    # 正净买入比例
    positive_ratio = (
        positive_days
        / 30
    )


    # ========================================================
    # 23. 埋伏池判断
    # ========================================================

    is_ambush = (

        # 至少20个正净买入日
        positive_days
        >= MIN_POSITIVE_DAYS

        and

        # 30D累计净买入超过5万美元
        netflow_30d
        > MIN_NETFLOW_30D
    )


    # ========================================================
    # 只保存真正进入埋伏池的币
    # ========================================================

    if is_ambush:

        summaries.append(
            {
                "symbol": token[
                    "symbol"
                ],

                "chain": token[
                    "chain"
                ],

                "contract_address": token[
                    "contract"
                ],

                "positive_days": (
                    positive_days
                ),

                "positive_ratio": (
                    positive_ratio
                ),

                "netflow_30d": (
                    netflow_30d
                ),
            }
        )


# ============================================================
# 24. 按30D净买入金额从高到低排序
# ============================================================

summaries.sort(
    key=lambda item: item[
        "netflow_30d"
    ],
    reverse=True,
)


# ============================================================
# 25. 输出最终埋伏池CSV
# ============================================================

with open(
    OUTPUT_FILE,
    "w",
    newline="",
    encoding="utf-8-sig",
) as file:

    # 字段
    fieldnames = [
        "rank",
        "symbol",
        "chain",
        "contract_address",
        "positive_days",
        "positive_ratio",
        "netflow_30d",
        "updated_at",
    ]

    # 创建CSV写入器
    writer = csv.DictWriter(
        file,
        fieldnames=fieldnames,
    )

    # 写表头
    writer.writeheader()

    # 更新时间
    updated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # 只写真正的埋伏币
    for rank, item in enumerate(
        summaries,
        start=1,
    ):

        writer.writerow(
            {
                "rank": rank,

                "symbol": item[
                    "symbol"
                ],

                "chain": item[
                    "chain"
                ],

                "contract_address": item[
                    "contract_address"
                ],

                "positive_days": item[
                    "positive_days"
                ],

                "positive_ratio": (
                    f"{item['positive_ratio']:.2%}"
                ),

                "netflow_30d": round(
                    item[
                        "netflow_30d"
                    ],
                    2,
                ),

                "updated_at": updated_at,
            }
        )


# ============================================================
# 26. 输出最终结果
# ============================================================

print("\n" + "=" * 72)

print("埋伏池生成完成")

print("=" * 72)

# 成功处理数量
print(
    f"成功处理 Alpha："
    f"{len(processed_tokens)}"
)

# 埋伏币数量
print(
    f"最终进入埋伏池："
    f"{len(summaries)}"
)

# 文件
print(
    f"\n✅ 埋伏池："
    f"{OUTPUT_FILE}"
)

# 数据库
print(
    f"✅ 每日资金数据库："
    f"{DATABASE_FILE}"
)


# ============================================================
# 27. 预览埋伏池前20名
# ============================================================

if summaries:

    print("\n埋伏池前20名：")

    print(
        f"{'排名':<6}"
        f"{'Token':<16}"
        f"{'链':<12}"
        f"{'正流入天数':>12}"
        f"{'比例':>10}"
        f"{'30D净买入':>20}"
    )

    print("-" * 82)


    # 打印前20名
    for index, item in enumerate(
        summaries[:20],
        start=1,
    ):

        print(
            f"{index:<6}"
            f"{item['symbol']:<16}"
            f"{item['chain']:<12}"
            f"{item['positive_days']:>12}"
            f"{item['positive_ratio']:>9.2%}"
            f"${item['netflow_30d']:>+19,.2f}"
        )


# ============================================================
# 28. 单独检查UB
# ============================================================

UB_ADDRESS = (
    "0x40b8129b786d766267a7a118cf8c07e31cdb6fde"
)

print("\nUB校验：")

# 从数据库查询UB
cursor.execute(
    """
    SELECT

        COUNT(
            CASE
                WHEN netflow_usd > 0
                THEN 1
            END
        ),

        SUM(netflow_usd)

    FROM daily_fund_flow

    WHERE
        chain = 'BSC'
        AND contract_address = ?
        AND date >= ?
        AND date < ?
    """,

    (
        UB_ADDRESS,
        str(start_time.date()),
        str(end_time.date()),
    ),
)

# 获取UB结果
ub_result = cursor.fetchone()

# 打印
print(
    f"  正净买入天数："
    f"{ub_result[0] or 0}"
)

print(
    f"  30D净买入："
    f"${float(ub_result[1] or 0):+,.2f}"
)


# ============================================================
# 29. 显示暂不支持链
# ============================================================

if unsupported_tokens:

    # 创建统计
    unsupported_counts = {}

    # 遍历
    for token in unsupported_tokens:

        # 获取链
        chain = token["chain"]

        # 计数
        unsupported_counts[chain] = (
            unsupported_counts.get(
                chain,
                0,
            )
            + 1
        )

    print("\n暂未计算的链：")

    # 打印
    for chain, count in (
        unsupported_counts.items()
    ):

        print(
            f"  {chain}: {count} 个"
        )


# ============================================================
# 30. 关闭数据库
# ============================================================

connection.close()
