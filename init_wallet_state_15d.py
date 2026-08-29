# ============================================================
# Binance Alpha 需求二
# 15天钱包状态正式初始化程序
#
# 核心目标：
#
# 为每一个：
#
#     Token + Wallet
#
# 保存这个钱包最近一次买入该Token的时间：
#
#     last_buy_time
#
# 用途：
#
# 以后每15分钟收到新交易时，
# 就可以在本地判断：
#
#     这个钱包过去15天有没有买过这个Token？
#
# 如果没有：
#
#     → 算一个15D新钱包
#
#
# 重要原则：
#
# 1. 不保存原始Swap
# 2. 不保存每笔交易
# 3. 只保存 Token + Wallet + last_buy_time
# 4. 普通Token自动合并查询
# 5. 大Token自动拆分
# 6. 单次结果撞25,000行后自动继续拆
# 7. 每个成功区间立即写SQLite
# 8. 支持断点续跑
# 9. 已经成功的区间下次不重新花Bitquery积分
# ============================================================


# ============================================================
# 1. 导入Python模块
# ============================================================

# os：
# 用于读取环境变量
import os

# csv：
# 用于读取当前Alpha清单
import csv

# json：
# 用于保存初始化计划和拆分信息
import json

# time：
# 用于请求重试和测速
import time

# hashlib：
# 用于给每个查询区间生成唯一ID
import hashlib

# sqlite3：
# 用于操作本地SQLite数据库
import sqlite3

# requests：
# 用于访问Bitquery
import requests

# datetime：
# 用于处理15天时间窗口
from datetime import datetime, timedelta, timezone

# dotenv：
# 用于读取.env中的BITQUERY_TOKEN
from dotenv import load_dotenv


# ============================================================
# 2. 读取.env
# ============================================================

# 加载.env
load_dotenv()

# 读取Bitquery Token
BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

# 如果Token不存在，直接停止
if not BITQUERY_TOKEN:

    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 3. 基础配置
# ============================================================

# Bitquery亚洲节点
BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)

# 当前有效Alpha清单
ALPHA_FILE = (
    "alpha_tokens_active.csv"
)

# 我们现有的SQLite数据库
DATABASE_FILE = (
    "alpha_monitor.db"
)

# 单次Bitquery最多返回25000条
QUERY_LIMIT = 25000

# 普通Token打包时，
# 尽量把预计钱包总数控制在18000以内
#
# 这样和25000上限之间留出约7000余量
PACK_TARGET = 18000

# 如果单个Token仍然爆量，
# 时间可以不断切小
#
# 最小允许切到60秒
MIN_SEGMENT_SECONDS = 60

# 一个请求最多尝试4次
MAX_RETRIES = 4


# ============================================================
# 4. Bitquery支持链
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
# 5. EVM链
# ============================================================

# EVM钱包地址和Token地址统一转小写
EVM_CHAINS = {

    "BSC",

    "Base",

    "Ethereum",

    "Arbitrum",
}


# ============================================================
# 6. 时间格式工具
# ============================================================

def to_bitquery_time(dt):

    # 去掉微秒，让初始化时间更加整洁和固定
    dt = dt.replace(
        microsecond=0
    )

    # 转换成：
    #
    # 2026-08-28T13:00:00Z
    return (
        dt.isoformat()
        .replace("+00:00", "Z")
    )


# ============================================================
# 7. Bitquery时间字符串转datetime
# ============================================================

def parse_bitquery_time(value):

    # 把Z改为+00:00
    value = value.replace(
        "Z",
        "+00:00",
    )

    # 转换为datetime
    return datetime.fromisoformat(
        value
    )


# ============================================================
# 8. 地址标准化
# ============================================================

def normalize_address(
    chain,
    address,
):

    # 转字符串并去空格
    address = str(
        address
    ).strip()

    # EVM链统一小写
    if chain in EVM_CHAINS:

        address = (
            address.lower()
        )

    # 返回
    return address


# ============================================================
# 9. 构造Bitquery Token ID
# ============================================================

def make_token_id(
    chain,
    address,
):

    # 例如：
    #
    # bid:bsc:0x...
    return (
        TOKEN_ID_PREFIX[chain]
        + ":"
        + address
    )


# ============================================================
# 10. 打开数据库
# ============================================================

connection = sqlite3.connect(
    DATABASE_FILE
)

# SQLite如果暂时被占用，
# 最多等待30秒
connection.execute(
    "PRAGMA busy_timeout = 30000"
)

# 开启外键
connection.execute(
    "PRAGMA foreign_keys = ON"
)

# WAL模式：
# 后续实时监控时更适合频繁小写入
connection.execute(
    "PRAGMA journal_mode = WAL"
)

# 创建游标
cursor = connection.cursor()


# ============================================================
# 11. Token注册表
#
# 为什么单独建一张？
#
# wallet_token_state以后可能有几十万行。
#
# 如果每一行都重复存：
#
# symbol
# chain
# contract
# token_id
#
# 很浪费空间。
#
# 所以给每个Token一个整数token_key。
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS alpha_token_registry (

        token_key INTEGER PRIMARY KEY AUTOINCREMENT,

        token_id TEXT NOT NULL UNIQUE,

        symbol TEXT NOT NULL,

        chain TEXT NOT NULL,

        contract_address TEXT NOT NULL,

        updated_at TEXT NOT NULL

    )
    """
)


# ============================================================
# 12. 钱包状态表
#
# 核心表。
#
# 一行就是：
#
# Token + Wallet + 最后买入时间
#
# WITHOUT ROWID：
# 对这种联合主键表可以节省一定空间。
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_token_state (

        token_key INTEGER NOT NULL,

        wallet TEXT NOT NULL,

        last_buy_time TEXT NOT NULL,

        updated_at TEXT NOT NULL,

        PRIMARY KEY (
            token_key,
            wallet
        ),

        FOREIGN KEY (
            token_key
        )
        REFERENCES alpha_token_registry(
            token_key
        )

    ) WITHOUT ROWID
    """
)


# ============================================================
# 13. last_buy_time索引
#
# 以后每天清理超过15天的钱包状态时会用到。
# ============================================================

cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS
    idx_wallet_token_state_last_buy

    ON wallet_token_state(
        last_buy_time
    )
    """
)


# ============================================================
# 14. 初始化断点表
#
# 每个成功查询区间都会记录。
#
# 如果中途程序停止：
#
# 下次重新运行
# → 成功的区间直接跳过
# → 不重新消耗Bitquery积分
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_init_checkpoint (

        segment_key TEXT PRIMARY KEY,

        status TEXT NOT NULL,

        token_count INTEGER NOT NULL,

        since_time TEXT NOT NULL,

        till_time TEXT NOT NULL,

        row_count INTEGER,

        split_type TEXT,

        split_payload TEXT,

        completed_at TEXT

    )
    """
)


# ============================================================
# 15. 初始化元数据表
#
# 保存：
#
# 初始化窗口
# 初始化计划
# 初始化状态
#
# 保证中断以后仍然继续同一个15天窗口。
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_init_meta (

        meta_key TEXT PRIMARY KEY,

        meta_value TEXT NOT NULL

    )
    """
)


# 保存表结构
connection.commit()


# ============================================================
# 16. 获取初始化元数据
# ============================================================

def get_meta(key):

    # 查询
    cursor.execute(
        """
        SELECT meta_value

        FROM wallet_init_meta

        WHERE meta_key = ?
        """,
        (key,),
    )

    # 获取结果
    row = cursor.fetchone()

    # 没有则返回None
    if not row:

        return None

    # 返回值
    return row[0]


# ============================================================
# 17. 保存初始化元数据
# ============================================================

def set_meta(
    key,
    value,
):

    # 插入或更新
    cursor.execute(
        """
        INSERT INTO wallet_init_meta (
            meta_key,
            meta_value
        )

        VALUES (?, ?)

        ON CONFLICT(meta_key)

        DO UPDATE SET
            meta_value = excluded.meta_value
        """,
        (
            key,
            str(value),
        ),
    )

    # 保存
    connection.commit()


# ============================================================
# 18. 检查是否已经初始化完成
# ============================================================

init_status = get_meta(
    "wallet_15d_init_status"
)

# 如果之前已经完整完成
if init_status == "complete":

    print("=" * 72)

    print(
        "15天钱包状态已经初始化完成"
    )

    print("=" * 72)

    # 查询当前钱包状态数量
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM wallet_token_state
        """
    )

    state_count = (
        cursor.fetchone()[0]
        or 0
    )

    print(
        f"当前钱包状态："
        f"{state_count:,} 条"
    )

    print(
        "无需重复初始化，避免浪费Bitquery积分。"
    )

    connection.close()

    raise SystemExit


# ============================================================
# 19. 读取当前有效Alpha
# ============================================================

tokens = []

# 打开Alpha清单
with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    # 创建CSV读取器
    reader = csv.DictReader(
        file
    )

    # 遍历
    for row in reader:

        # 获取链
        chain = row.get(
            "chainName",
            "",
        ).strip()

        # Bitquery暂不支持就跳过
        if chain not in TOKEN_ID_PREFIX:

            continue

        # 获取币名
        symbol = row.get(
            "symbol",
            "",
        ).strip()

        # 获取Token地址
        address = normalize_address(
            chain,
            row.get(
                "contractAddress",
                "",
            ),
        )

        # 没地址跳过
        if not address:

            continue

        # 构造Token ID
        token_id = make_token_id(
            chain,
            address,
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
# 20. Token去重
# ============================================================

unique_tokens = []

seen_tokens = set()

# 遍历
for token in tokens:

    # Token ID作为唯一键
    token_id = token[
        "token_id"
    ]

    # 已经出现就跳过
    if token_id in seen_tokens:

        continue

    # 标记
    seen_tokens.add(
        token_id
    )

    # 保存
    unique_tokens.append(
        token
    )


# ============================================================
# 21. 把当前Alpha写入Token注册表
# ============================================================

updated_at = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

# 遍历所有支持的Token
for token in unique_tokens:

    # 插入或更新
    cursor.execute(
        """
        INSERT INTO alpha_token_registry (

            token_id,
            symbol,
            chain,
            contract_address,
            updated_at

        )

        VALUES (?, ?, ?, ?, ?)

        ON CONFLICT(token_id)

        DO UPDATE SET

            symbol = excluded.symbol,

            chain = excluded.chain,

            contract_address = excluded.contract_address,

            updated_at = excluded.updated_at
        """,
        (
            token["token_id"],

            token["symbol"],

            token["chain"],

            token["address"],

            updated_at,
        ),
    )


# 保存
connection.commit()


# ============================================================
# 22. 读取token_key
# ============================================================

cursor.execute(
    """
    SELECT

        token_key,
        token_id,
        symbol,
        chain,
        contract_address

    FROM alpha_token_registry
    """
)

# Token ID → 注册信息
registry_map = {}

# 遍历
for row in cursor.fetchall():

    registry_map[
        row[1]
    ] = {

        "token_key": row[0],

        "token_id": row[1],

        "symbol": row[2],

        "chain": row[3],

        "address": row[4],
    }


# ============================================================
# 23. 通用Bitquery请求头
# ============================================================

HEADERS = {

    "Content-Type": (
        "application/json"
    ),

    "Authorization": (
        f"Bearer {BITQUERY_TOKEN}"
    ),
}


# ============================================================
# 24. 本次程序实际发送多少次API
# ============================================================

request_counter = 0


# ============================================================
# 25. 通用Bitquery查询函数
# ============================================================

def run_query(
    query,
    variables,
):

    # 使用全局请求计数器
    global request_counter

    # 最多尝试4次
    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            # 每真正发一次HTTP请求，
            # 请求计数+1
            request_counter += 1

            # 开始计时
            start_clock = (
                time.perf_counter()
            )

            # 请求Bitquery
            response = requests.post(

                BITQUERY_URL,

                headers=HEADERS,

                json={
                    "query": query,
                    "variables": variables,
                },

                timeout=180,
            )

            # 计算耗时
            seconds = (
                time.perf_counter()
                - start_clock
            )


            # 429限流
            if response.status_code == 429:

                # 等待时间逐次增加
                wait_seconds = (
                    15 * attempt
                )

                print(
                    f"    ⚠️ 429限流，"
                    f"{wait_seconds}秒后重试"
                )

                time.sleep(
                    wait_seconds
                )

                continue


            # 其他HTTP错误
            response.raise_for_status()

            # 转JSON
            result = response.json()

            # GraphQL错误
            if "errors" in result:

                raise RuntimeError(
                    str(
                        result["errors"]
                    )
                )

            # 获取Trading结果
            rows = (
                result["data"]
                ["Trading"]
                ["Trades"]
            )

            # 返回：
            # 数据 + 查询耗时
            return (
                rows,
                seconds,
            )


        except Exception as error:

            # 如果已经最后一次
            if attempt >= MAX_RETRIES:

                raise

            # 等待再重试
            wait_seconds = (
                5 * attempt
            )

            print(
                f"    ⚠️ 请求失败：{error}"
            )

            print(
                f"    {wait_seconds}秒后重试..."
            )

            time.sleep(
                wait_seconds
            )


# ============================================================
# 26. 第一次统计305个Token的15D钱包数量
#
# 只返回约305行。
#
# 不返回具体钱包地址。
# ============================================================

COUNT_QUERY = """
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
    }
  }
}
"""


# ============================================================
# 27. 正式钱包状态查询
#
# 每行：
#
# Token + Wallet + 最近一次买入时间
# ============================================================

STATE_QUERY = """
query InitWalletState(
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
    }
  }
}
"""


# ============================================================
# 28. 查看以前有没有保存初始化计划
# ============================================================

saved_plan_json = get_meta(
    "wallet_15d_init_plan"
)


# ============================================================
# 29. 第一次运行：建立固定15天窗口和查询计划
# ============================================================

if not saved_plan_json:

    print("=" * 72)

    print(
        "需求二：建立15天钱包状态初始化计划"
    )

    print("=" * 72)


    # 当前UTC时间
    init_end_dt = datetime.now(
        timezone.utc
    ).replace(
        microsecond=0
    )

    # 向前15天
    init_start_dt = (
        init_end_dt
        - timedelta(days=15)
    )

    # 转成固定字符串
    init_start = to_bitquery_time(
        init_start_dt
    )

    init_end = to_bitquery_time(
        init_end_dt
    )


    print(
        f"Alpha数量："
        f"{len(unique_tokens)}"
    )

    print(
        f"初始化开始："
        f"{init_start}"
    )

    print(
        f"初始化结束："
        f"{init_end}"
    )

    print(
        "\n正在统计每个Token的15D唯一买入钱包..."
    )


    # 所有Token ID
    all_token_ids = [

        token["token_id"]

        for token in unique_tokens
    ]


    # 查询统计
    count_rows, count_seconds = run_query(

        COUNT_QUERY,

        {
            "tokens": all_token_ids,

            "since": init_start,

            "till": init_end,
        },
    )


    print(
        f"✅ 钱包规模统计完成："
        f"{count_seconds:.2f} 秒"
    )


    # ========================================================
    # 30. 初始化所有Token钱包数为0
    # ========================================================

    expected_counts = {

        token_id: 0

        for token_id in all_token_ids
    }


    # ========================================================
    # 31. 填入真实钱包数
    # ========================================================

    for row in count_rows:

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

        # 不是我们的Token则忽略
        if token_id not in expected_counts:

            continue

        # 保存预计唯一钱包数
        expected_counts[
            token_id
        ] = int(
            row.get(
                "unique_buyers"
            )
            or 0
        )


    # ========================================================
    # 32. 按钱包量从大到小排序
    # ========================================================

    sorted_token_ids = sorted(

        all_token_ids,

        key=lambda token_id: (
            expected_counts[
                token_id
            ]
        ),

        reverse=True,
    )


    # ========================================================
    # 33. 自动装箱
    #
    # >=18000钱包：
    # 单币一组
    #
    # 其他币：
    # 尽量让一个批次总钱包量<=18000
    # ========================================================

    root_batches = []

    # 当前普通批次
    current_batch = []

    # 当前批次预计钱包量
    current_total = 0


    # 遍历
    for token_id in sorted_token_ids:

        # 当前Token预计钱包量
        wallet_count = (
            expected_counts[
                token_id
            ]
        )


        # --------------------------------------------
        # 如果一个Token自己就>=18000
        # --------------------------------------------

        if wallet_count >= PACK_TARGET:

            # 先保存前面的普通批次
            if current_batch:

                root_batches.append(
                    current_batch
                )

                current_batch = []

                current_total = 0


            # 大币自己一批
            root_batches.append(
                [
                    token_id
                ]
            )

            continue


        # --------------------------------------------
        # 普通Token：
        # 如果放进去会超过18000
        # --------------------------------------------

        if (
            current_batch
            and
            current_total + wallet_count
            > PACK_TARGET
        ):

            # 先结束当前批次
            root_batches.append(
                current_batch
            )

            # 新建批次
            current_batch = []

            current_total = 0


        # 当前Token加入批次
        current_batch.append(
            token_id
        )

        # 累加预计钱包
        current_total += (
            wallet_count
        )


    # 最后一批
    if current_batch:

        root_batches.append(
            current_batch
        )


    # ========================================================
    # 34. 构造初始化计划
    # ========================================================

    plan_tokens = []

    # 遍历Token
    for token in unique_tokens:

        # Token ID
        token_id = token[
            "token_id"
        ]

        # 保存Token和预计钱包数
        plan_tokens.append(
            {
                "token_id": token_id,

                "symbol": token[
                    "symbol"
                ],

                "chain": token[
                    "chain"
                ],

                "address": token[
                    "address"
                ],

                "expected_wallets": (
                    expected_counts[
                        token_id
                    ]
                ),
            }
        )


    # 完整计划
    plan = {

        "start": init_start,

        "end": init_end,

        "tokens": plan_tokens,

        "root_batches": (
            root_batches
        ),
    }


    # 转JSON
    saved_plan_json = json.dumps(
        plan,
        ensure_ascii=False,
    )

    # 保存计划
    set_meta(
        "wallet_15d_init_plan",
        saved_plan_json,
    )

    # 标记初始化进行中
    set_meta(
        "wallet_15d_init_status",
        "running",
    )


# ============================================================
# 35. 如果已经有计划，直接读取
#
# 这就是断点续跑的关键之一。
# ============================================================

else:

    print("=" * 72)

    print(
        "检测到未完成的15天初始化"
    )

    print(
        "将继续上次进度，"
        "已完成区间不会重复查询"
    )

    print("=" * 72)


# 解析计划
plan = json.loads(
    saved_plan_json
)

# 固定开始时间
init_start = plan[
    "start"
]

# 固定截止时间
init_end = plan[
    "end"
]

# 根批次
root_batches = plan[
    "root_batches"
]


# ============================================================
# 36. 建立预计钱包数量映射
# ============================================================

expected_wallets = {}

# Token基本信息映射
plan_token_map = {}

# 遍历计划中的Token
for item in plan[
    "tokens"
]:

    # Token ID
    token_id = item[
        "token_id"
    ]

    # 预计钱包数
    expected_wallets[
        token_id
    ] = int(
        item[
            "expected_wallets"
        ]
    )

    # 保存信息
    plan_token_map[
        token_id
    ] = item


# ============================================================
# 37. 计算预计总钱包状态
# ============================================================

expected_total = sum(
    expected_wallets.values()
)


# ============================================================
# 38. 生成查询区间唯一Key
# ============================================================

def make_segment_key(
    token_ids,
    since_time,
    till_time,
):

    # Token排序保证顺序变化不影响Key
    sorted_ids = sorted(
        token_ids
    )

    # 拼接
    raw_value = (
        "|".join(sorted_ids)
        + "|"
        + since_time
        + "|"
        + till_time
    )

    # SHA256
    return hashlib.sha256(
        raw_value.encode(
            "utf-8"
        )
    ).hexdigest()


# ============================================================
# 39. 查询断点记录
# ============================================================

def get_checkpoint(
    segment_key,
):

    cursor.execute(
        """
        SELECT

            status,
            split_type,
            split_payload,
            row_count

        FROM wallet_init_checkpoint

        WHERE segment_key = ?
        """,
        (
            segment_key,
        ),
    )

    return cursor.fetchone()


# ============================================================
# 40. 保存“需要拆Token”的断点
# ============================================================

def save_token_split_checkpoint(
    segment_key,
    token_ids,
    since_time,
    till_time,
    groups,
):

    # 将两个子Token组保存下来
    payload = json.dumps(
        groups,
        ensure_ascii=False,
    )

    cursor.execute(
        """
        INSERT OR REPLACE INTO
        wallet_init_checkpoint (

            segment_key,
            status,
            token_count,
            since_time,
            till_time,
            row_count,
            split_type,
            split_payload,
            completed_at

        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            segment_key,

            "split",

            len(token_ids),

            since_time,

            till_time,

            QUERY_LIMIT,

            "tokens",

            payload,

            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ),
    )

    connection.commit()


# ============================================================
# 41. 保存“需要拆时间”的断点
# ============================================================

def save_time_split_checkpoint(
    segment_key,
    token_ids,
    since_time,
    till_time,
    midpoint,
):

    # 保存中间时间点
    payload = json.dumps(
        {
            "midpoint": midpoint
        }
    )

    cursor.execute(
        """
        INSERT OR REPLACE INTO
        wallet_init_checkpoint (

            segment_key,
            status,
            token_count,
            since_time,
            till_time,
            row_count,
            split_type,
            split_payload,
            completed_at

        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            segment_key,

            "split",

            len(token_ids),

            since_time,

            till_time,

            QUERY_LIMIT,

            "time",

            payload,

            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ),
    )

    connection.commit()


# ============================================================
# 42. 将Token组拆成两个相对均衡的组
#
# 根据预计钱包数量进行平衡，
# 而不是简单按Token个数一半一半。
# ============================================================

def split_token_group(
    token_ids,
):

    # 从预计钱包最多的开始
    sorted_ids = sorted(

        token_ids,

        key=lambda token_id: (
            expected_wallets.get(
                token_id,
                0,
            )
        ),

        reverse=True,
    )


    # 两个子组
    group_a = []

    group_b = []

    # 两组预计钱包总量
    total_a = 0

    total_b = 0


    # 每个Token放到当前较小的一组
    for token_id in sorted_ids:

        count = (
            expected_wallets.get(
                token_id,
                0,
            )
        )

        if total_a <= total_b:

            group_a.append(
                token_id
            )

            total_a += count

        else:

            group_b.append(
                token_id
            )

            total_b += count


    # 返回
    return [
        group_a,
        group_b,
    ]


# ============================================================
# 43. 钱包地址标准化
# ============================================================

def normalize_wallet(
    token_id,
    wallet,
):

    # 去空格
    wallet = str(
        wallet
    ).strip()

    # 获取当前Token信息
    token_info = (
        plan_token_map.get(
            token_id,
            {}
        )
    )

    # 当前链
    chain = token_info.get(
        "chain",
        "",
    )

    # EVM钱包统一小写
    if chain in EVM_CHAINS:

        wallet = (
            wallet.lower()
        )

    return wallet


# ============================================================
# 44. 保存一个成功查询区间
#
# 钱包状态UPSERT：
#
# 如果同一 Token + Wallet
# 在不同时间分片里重复出现，
#
# 只保留更新的last_buy_time。
# ============================================================

def save_success_segment(
    segment_key,
    token_ids,
    since_time,
    till_time,
    rows,
):

    # 当前更新时间
    now_text = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # 要写入数据库的数据
    insert_rows = []


    # 遍历Bitquery返回结果
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

        # 不属于当前注册Token就跳过
        if token_id not in registry_map:

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


        # 获取最后一次买入时间
        last_buy_time = (
            row.get(
                "Block",
                {},
            )
            .get(
                "last_buy_time",
                "",
            )
        )


        # 缺钱包地址就跳过
        if not wallet:

            continue

        # 缺最后买入时间也跳过
        if not last_buy_time:

            continue


        # 钱包地址标准化
        wallet = normalize_wallet(
            token_id,
            wallet,
        )


        # 当前Token的整数ID
        token_key = (
            registry_map[
                token_id
            ][
                "token_key"
            ]
        )


        # 保存
        insert_rows.append(
            (
                token_key,

                wallet,

                last_buy_time,

                now_text,
            )
        )


    # ========================================================
    # 数据和断点放在同一个事务里
    #
    # 要么一起成功，
    # 要么一起失败。
    #
    # 避免：
    #
    # 钱包写了一半
    # 但断点已经显示完成
    # ========================================================

    try:

        # 开始事务
        connection.execute(
            "BEGIN"
        )


        # 批量UPSERT
        cursor.executemany(
            """
            INSERT INTO wallet_token_state (

                token_key,
                wallet,
                last_buy_time,
                updated_at

            )

            VALUES (?, ?, ?, ?)

            ON CONFLICT(
                token_key,
                wallet
            )

            DO UPDATE SET

                last_buy_time =
                    CASE

                        WHEN excluded.last_buy_time
                             > wallet_token_state.last_buy_time

                        THEN excluded.last_buy_time

                        ELSE wallet_token_state.last_buy_time

                    END,

                updated_at =
                    excluded.updated_at
            """,
            insert_rows,
        )


        # 保存成功断点
        cursor.execute(
            """
            INSERT OR REPLACE INTO
            wallet_init_checkpoint (

                segment_key,
                status,
                token_count,
                since_time,
                till_time,
                row_count,
                split_type,
                split_payload,
                completed_at

            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                segment_key,

                "complete",

                len(token_ids),

                since_time,

                till_time,

                len(rows),

                None,

                None,

                now_text,
            ),
        )


        # 一起提交
        connection.commit()


    except Exception:

        # 有任何错误就回滚
        connection.rollback()

        raise


# ============================================================
# 45. 递归处理一个查询区间
#
# 这是整个初始化程序最核心的函数。
#
# 情况1：
#
# 返回 < 25000
# → 保存
#
# 情况2：
#
# 返回 = 25000
# 且有多个Token
# → 自动拆Token
#
# 情况3：
#
# 返回 = 25000
# 且只有1个Token
# → 自动拆时间
# ============================================================

def process_segment(
    token_ids,
    since_time,
    till_time,
    depth=0,
):

    # 缩进只是为了让终端进度更好看
    indent = (
        "    " * depth
    )


    # 生成区间Key
    segment_key = make_segment_key(
        token_ids,
        since_time,
        till_time,
    )


    # ========================================================
    # 先看是否有断点
    # ========================================================

    checkpoint = get_checkpoint(
        segment_key
    )


    # 如果已经成功完成
    if (
        checkpoint
        and
        checkpoint[0] == "complete"
    ):

        print(
            f"{indent}↪ 已完成，跳过："
            f"{len(token_ids)}个Token，"
            f"{checkpoint[3] or 0:,}条"
        )

        return


    # ========================================================
    # 如果以前已经判断需要拆分
    #
    # 直接照旧计划继续，
    # 不再重新查询父区间。
    # ========================================================

    if (
        checkpoint
        and
        checkpoint[0] == "split"
    ):

        split_type = (
            checkpoint[1]
        )

        split_payload = json.loads(
            checkpoint[2]
        )


        # --------------------------------------------
        # Token拆分
        # --------------------------------------------

        if split_type == "tokens":

            for child_group in split_payload:

                # 空组跳过
                if not child_group:

                    continue

                process_segment(
                    child_group,
                    since_time,
                    till_time,
                    depth + 1,
                )

            return


        # --------------------------------------------
        # 时间拆分
        # --------------------------------------------

        if split_type == "time":

            midpoint = split_payload[
                "midpoint"
            ]

            # 前半段
            process_segment(
                token_ids,
                since_time,
                midpoint,
                depth + 1,
            )

            # 后半段
            process_segment(
                token_ids,
                midpoint,
                till_time,
                depth + 1,
            )

            return


    # ========================================================
    # 显示当前Token
    # ========================================================

    # 如果只有一个Token，
    # 显示Token名称
    if len(token_ids) == 1:

        token_id = token_ids[0]

        symbol = (
            plan_token_map[
                token_id
            ][
                "symbol"
            ]
        )

        label = (
            f"{symbol}"
        )

    # 多Token只显示数量
    else:

        label = (
            f"{len(token_ids)}个Token"
        )


    print(
        f"{indent}查询 {label}："
        f"{since_time} → {till_time}"
    )


    # ========================================================
    # 正式请求Bitquery
    # ========================================================

    rows, seconds = run_query(

        STATE_QUERY,

        {
            "tokens": token_ids,

            "since": since_time,

            "till": till_time,

            "limit": QUERY_LIMIT,
        },
    )


    print(
        f"{indent}  返回："
        f"{len(rows):,} 条，"
        f"{seconds:.2f} 秒"
    )


    # ========================================================
    # 没撞25000上限
    # → 直接保存
    # ========================================================

    if len(rows) < QUERY_LIMIT:

        save_success_segment(
            segment_key,
            token_ids,
            since_time,
            till_time,
            rows,
        )

        print(
            f"{indent}  ✅ 已写入并记录断点"
        )

        return


    # ========================================================
    # 撞到25000
    # ========================================================

    print(
        f"{indent}  ⚠️ 达到25,000上限，"
        "自动拆分"
    )


    # ========================================================
    # 多个Token：
    # 优先按Token拆
    # ========================================================

    if len(token_ids) > 1:

        # 分成两个相对均衡的组
        groups = split_token_group(
            token_ids
        )

        # 保存拆分决定
        save_token_split_checkpoint(
            segment_key,
            token_ids,
            since_time,
            till_time,
            groups,
        )

        # 递归处理
        for group in groups:

            if not group:

                continue

            process_segment(
                group,
                since_time,
                till_time,
                depth + 1,
            )

        return


    # ========================================================
    # 单个Token：
    # 只能拆时间
    # ========================================================

    start_dt = parse_bitquery_time(
        since_time
    )

    end_dt = parse_bitquery_time(
        till_time
    )

    # 当前区间秒数
    duration_seconds = (
        end_dt
        - start_dt
    ).total_seconds()


    # 已经小到1分钟仍然25000
    # 就不再盲目继续，安全停止
    if duration_seconds <= MIN_SEGMENT_SECONDS:

        raise RuntimeError(
            "单个Token在60秒窗口内仍达到25000条，"
            "为避免继续消耗积分，程序已停止。"
        )


    # 计算中间点
    midpoint_dt = (
        start_dt
        + (
            end_dt
            - start_dt
        ) / 2
    )

    # 转字符串
    midpoint = to_bitquery_time(
        midpoint_dt
    )


    # 保存拆时间决定
    save_time_split_checkpoint(
        segment_key,
        token_ids,
        since_time,
        till_time,
        midpoint,
    )


    # 前半段
    process_segment(
        token_ids,
        since_time,
        midpoint,
        depth + 1,
    )

    # 后半段
    process_segment(
        token_ids,
        midpoint,
        till_time,
        depth + 1,
    )


# ============================================================
# 46. 输出正式初始化计划
# ============================================================

print("\n" + "=" * 72)

print(
    "开始15天钱包状态正式初始化"
)

print("=" * 72)

print(
    f"初始化区间："
    f"{init_start}"
)

print(
    f"        → "
    f"{init_end}"
)

print(
    f"Token数量："
    f"{len(plan['tokens'])}"
)

print(
    f"预计Token-Wallet状态："
    f"{expected_total:,} 条"
)

print(
    f"根批次数："
    f"{len(root_batches)}"
)

print(
    f"普通批次目标："
    f"{PACK_TARGET:,} 钱包以内"
)

print(
    "如果撞25,000，程序会自动继续拆。"
)

print(
    "如果中断，重新运行本文件即可断点续跑。"
)


# ============================================================
# 47. 按根批次逐个处理
# ============================================================

try:

    # 总根批次数
    total_root_batches = len(
        root_batches
    )


    # 遍历
    for index, token_ids in enumerate(
        root_batches,
        start=1,
    ):

        # 预计钱包数
        estimated = sum(

            expected_wallets.get(
                token_id,
                0,
            )

            for token_id in token_ids
        )


        print("\n" + "-" * 72)

        print(
            f"根批次 "
            f"{index}/{total_root_batches}"
        )

        print(
            f"Token数量："
            f"{len(token_ids)}"
        )

        print(
            f"预计钱包："
            f"{estimated:,}"
        )


        # 正式处理
        process_segment(
            token_ids,
            init_start,
            init_end,
        )


except KeyboardInterrupt:

    # 用户Ctrl+C
    print(
        "\n\n⏸ 初始化已手工停止。"
    )

    print(
        "已经成功的区间均已保存。"
    )

    print(
        "以后重新运行："
    )

    print(
        "python init_wallet_state_15d.py"
    )

    print(
        "即可从断点继续。"
    )

    connection.close()

    raise SystemExit


except Exception as error:

    # 其他错误
    print("\n" + "=" * 72)

    print(
        "❌ 初始化过程中出现错误"
    )

    print("=" * 72)

    print(error)

    print(
        "\n已经完成的区间不会丢失。"
    )

    print(
        "修复问题后重新运行同一个文件即可继续。"
    )

    connection.close()

    raise SystemExit


# ============================================================
# 48. 所有查询完成后统计真实钱包状态
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)

    FROM wallet_token_state
    """
)

actual_total = (
    cursor.fetchone()[0]
    or 0
)


# ============================================================
# 49. 对比预计数量
# ============================================================

difference = (
    actual_total
    - expected_total
)

# 计算相对差异
if expected_total > 0:

    difference_ratio = (
        abs(difference)
        / expected_total
    )

else:

    difference_ratio = 0


# ============================================================
# 50. 输出验证结果
# ============================================================

print("\n" + "=" * 72)

print(
    "15天钱包状态初始化查询完成"
)

print("=" * 72)

print(
    f"预计钱包状态："
    f"{expected_total:,}"
)

print(
    f"实际数据库状态："
    f"{actual_total:,}"
)

print(
    f"差值："
    f"{difference:+,}"
)

print(
    f"相对差异："
    f"{difference_ratio:.4%}"
)

print(
    f"本次程序Bitquery请求："
    f"{request_counter} 次"
)


# ============================================================
# 51. 最终完整性保险
#
# 如果差异超过2%，
# 不直接宣布初始化成功。
#
# 数据仍然保留，
# 我们再检查原因。
# ============================================================

if difference_ratio > 0.02:

    print(
        "\n⚠️ 实际钱包数与预估差异超过2%。"
    )

    print(
        "数据库数据已经安全保存，"
        "但暂不标记为complete。"
    )

    set_meta(
        "wallet_15d_init_status",
        "needs_review",
    )

    connection.close()

    raise SystemExit


# ============================================================
# 52. 标记初始化成功
# ============================================================

set_meta(
    "wallet_15d_init_status",
    "complete",
)

# 保存初始化完成到哪个时间点
set_meta(
    "wallet_state_initialized_until",
    init_end,
)


# ============================================================
# 53. 查看数据库文件大小
# ============================================================

try:

    # 字节
    database_bytes = os.path.getsize(
        DATABASE_FILE
    )

    # 转MB
    database_mb = (
        database_bytes
        / 1024
        / 1024
    )

except Exception:

    database_mb = 0


# ============================================================
# 54. 最终输出
# ============================================================

print("\n" + "=" * 72)

print(
    "✅ 15天钱包状态初始化成功"
)

print("=" * 72)

print(
    f"Token："
    f"{len(plan['tokens'])}"
)

print(
    f"钱包状态："
    f"{actual_total:,} 条"
)

print(
    f"Bitquery请求："
    f"{request_counter} 次"
)

print(
    f"数据库文件："
    f"{DATABASE_FILE}"
)

print(
    f"当前数据库大小："
    f"{database_mb:.2f} MB"
)

print(
    f"钱包状态有效截至："
    f"{init_end}"
)

print(
    "\n下一步可以开始做："
)

print(
    "每15分钟增量新钱包监控。"
)


# ============================================================
# 55. 关闭数据库
# ============================================================

connection.close()
