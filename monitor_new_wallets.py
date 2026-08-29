# ============================================================
# Binance Alpha 需求二
# 15分钟新钱包数据采集层
#
# 本文件只负责：
#
# 1. 每15分钟查询一次Bitquery
# 2. 识别15D新钱包
# 3. 保存本15分钟新增新钱包
# 4. 累加今日新钱包
# 5. 保存买入/卖出/净买入
# 6. 更新钱包最近一次买入时间
#
# 本文件不再负责：
#
# - 钱包异常判断
# - Z-score
# - 2倍判断
# - 资金确认
# - 观察/跟进/重点
# - Telegram
#
# 上述内容全部交给：
#
# evaluate_wallet_anomaly.py
#
#
# 核心口径：
#
# 每15分钟只是“检查频率”。
#
# 本文件保存：
#
# 本15分钟新增新钱包
# +
# 今日截至当前累计新钱包
#
# 后面的异常程序使用：
#
# “今日累计新钱包”
#
# 判断启动异常。
# ============================================================


# ============================================================
# 1. 导入模块
# ============================================================

import os
import csv
import time
import sqlite3
import requests

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from dotenv import load_dotenv


# ============================================================
# 2. 环境变量
# ============================================================

load_dotenv()

BITQUERY_TOKEN = os.getenv(
    "BITQUERY_TOKEN"
)

if not BITQUERY_TOKEN:

    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# ============================================================
# 3. 基础配置
# ============================================================

BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)

DATABASE_FILE = (
    "alpha_monitor.db"
)

# Alpha清单由需求一每天更新
ALPHA_FILE = (
    "alpha_tokens_active.csv"
)

# 每15分钟一次
INTERVAL_MINUTES = 15

# Bitquery返回上限
QUERY_LIMIT = 25000

# 请求失败重试次数
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

EVM_CHAINS = {

    "BSC",

    "Base",

    "Ethereum",

    "Arbitrum",
}


# ============================================================
# 6. 时间解析
# ============================================================

def parse_time(value):

    return datetime.fromisoformat(

        value.replace(
            "Z",
            "+00:00",
        )
    )


# ============================================================
# 7. 时间格式化
# ============================================================

def format_time(value):

    return (

        value
        .replace(
            microsecond=0
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


# ============================================================
# 8. 向下对齐15分钟
#
# 14:27
#
# ↓
#
# 14:15
# ============================================================

def floor_to_15_minutes(value):

    minute = (

        value.minute
        //
        INTERVAL_MINUTES
        *
        INTERVAL_MINUTES
    )

    return value.replace(

        minute=minute,

        second=0,

        microsecond=0,
    )


# ============================================================
# 9. Token地址标准化
# ============================================================

def normalize_address(
    chain,
    address,
):

    address = str(
        address or ""
    ).strip()

    if chain in EVM_CHAINS:

        address = (
            address.lower()
        )

    return address


# ============================================================
# 10. 钱包地址标准化
# ============================================================

def normalize_wallet(
    chain,
    wallet,
):

    wallet = str(
        wallet or ""
    ).strip()

    if chain in EVM_CHAINS:

        wallet = (
            wallet.lower()
        )

    return wallet


# ============================================================
# 11. Bitquery Token ID
# ============================================================

def make_token_id(
    chain,
    address,
):

    return (

        TOKEN_ID_PREFIX[
            chain
        ]

        + ":"

        + address
    )


# ============================================================
# 12. 打开数据库
# ============================================================

connection = sqlite3.connect(
    DATABASE_FILE
)

connection.execute(
    "PRAGMA busy_timeout = 30000"
)

connection.execute(
    "PRAGMA journal_mode = WAL"
)

cursor = connection.cursor()


# ============================================================
# 13. 15分钟统计表
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS new_wallet_15m (

        token_key INTEGER NOT NULL,

        interval_start TEXT NOT NULL,

        interval_end TEXT NOT NULL,

        new_wallet_count INTEGER NOT NULL,

        buy_wallet_count INTEGER NOT NULL,

        new_wallet_buy_usd REAL NOT NULL,

        market_buy_usd REAL NOT NULL,

        market_sell_usd REAL NOT NULL,

        market_net_buy REAL NOT NULL,

        is_complete INTEGER NOT NULL DEFAULT 1,

        updated_at TEXT NOT NULL,

        PRIMARY KEY (
            token_key,
            interval_start
        )

    ) WITHOUT ROWID
    """
)


# ============================================================
# 14. 每日累计表
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS daily_new_wallets (

        date TEXT NOT NULL,

        token_key INTEGER NOT NULL,

        new_wallet_count INTEGER NOT NULL,

        new_wallet_buy_usd REAL NOT NULL,

        market_buy_usd REAL NOT NULL,

        market_sell_usd REAL NOT NULL,

        market_net_buy REAL NOT NULL,

        has_overflow INTEGER NOT NULL DEFAULT 0,

        updated_at TEXT NOT NULL,

        PRIMARY KEY (
            date,
            token_key
        )

    ) WITHOUT ROWID
    """
)


# ============================================================
# 15. 每次运行记录
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS new_wallet_monitor_runs (

        interval_start TEXT PRIMARY KEY,

        interval_end TEXT NOT NULL,

        returned_rows INTEGER NOT NULL,

        overflow INTEGER NOT NULL,

        processed_tokens INTEGER NOT NULL,

        completed_at TEXT NOT NULL

    )
    """
)


# ============================================================
# 16. 断点记录
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS new_wallet_monitor_meta (

        meta_key TEXT PRIMARY KEY,

        meta_value TEXT NOT NULL

    )
    """
)

connection.commit()


# ============================================================
# 17. 获取监控断点
# ============================================================

def get_monitor_meta(key):

    cursor.execute(
        """
        SELECT meta_value

        FROM new_wallet_monitor_meta

        WHERE meta_key = ?
        """,
        (
            key,
        ),
    )

    row = cursor.fetchone()

    if not row:

        return None

    return row[0]


# ============================================================
# 18. 获取钱包初始化信息
# ============================================================

def get_init_meta(key):

    cursor.execute(
        """
        SELECT meta_value

        FROM wallet_init_meta

        WHERE meta_key = ?
        """,
        (
            key,
        ),
    )

    row = cursor.fetchone()

    if not row:

        return None

    return row[0]


# ============================================================
# 19. 读取本地Alpha
#
# 这里不访问Binance。
#
# Alpha清单由需求一每天刷新。
# ============================================================

active_tokens = []

with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    reader = csv.DictReader(
        file
    )

    for row in reader:

        chain = str(
            row.get(
                "chainName",
                "",
            )
        ).strip()

        if chain not in TOKEN_ID_PREFIX:

            continue


        symbol = str(
            row.get(
                "symbol",
                "",
            )
        ).strip()


        address = normalize_address(

            chain,

            row.get(
                "contractAddress",
                "",
            ),
        )


        if not address:

            continue


        active_tokens.append(
            {

                "symbol":
                    symbol,

                "chain":
                    chain,

                "address":
                    address,

                "token_id":
                    make_token_id(
                        chain,
                        address,
                    ),
            }
        )


# ============================================================
# 20. 读取Token注册表
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


registry = {}


for (
    token_key,
    token_id,
    symbol,
    chain,
    address
) in cursor.fetchall():

    registry[
        token_id
    ] = {

        "token_key":
            token_key,

        "token_id":
            token_id,

        "symbol":
            symbol,

        "chain":
            chain,

        "address":
            normalize_address(
                chain,
                address,
            ),
    }


# ============================================================
# 21. 当前可以监控的Token
# ============================================================

monitor_tokens = []

new_uninitialized_tokens = []

seen = set()


for token in active_tokens:

    token_id = token[
        "token_id"
    ]


    # 防止重复
    if token_id in seen:

        continue


    seen.add(
        token_id
    )


    # 新Alpha还没有初始化15D底库
    if token_id not in registry:

        new_uninitialized_tokens.append(
            token
        )

        continue


    item = registry[
        token_id
    ].copy()


    # 使用Alpha文件里的最新symbol
    item[
        "symbol"
    ] = token[
        "symbol"
    ]


    monitor_tokens.append(
        item
    )


# ============================================================
# 22. Token映射
# ============================================================

token_map = {

    token[
        "token_id"
    ]:
        token

    for token in monitor_tokens
}


token_ids = list(
    token_map.keys()
)


# ============================================================
# 23. Bitquery查询
#
# 返回粒度：
#
# Token + Trader
#
# 一个钱包15分钟买100次，
# 最终仍然只是一条Token+Wallet聚合记录。
#
#
# buys：
# 当前15分钟买入次数
#
# buy_usd：
# 买入USD
#
# sell_usd：
# 卖出USD
#
# first_buy_time：
# 当前15分钟第一次买入
#
# last_buy_time：
# 当前15分钟最后一次买入
# ============================================================

QUERY = """
query MonitorNewWallets(
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

      Block {

        first_buy_time: Time(

          minimum: Block_Time

          if: {
            Side: {
              is: "Buy"
            }
          }
        )

        last_buy_time: Time(

          maximum: Block_Time

          if: {
            Side: {
              is: "Buy"
            }
          }
        )
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
# 24. HTTP请求头
# ============================================================

HEADERS = {

    "Content-Type":
        "application/json",

    "Authorization":
        f"Bearer {BITQUERY_TOKEN}",
}


# ============================================================
# 25. Bitquery请求
# ============================================================

def run_query(
    since_time,
    till_time,
):

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            start_clock = (
                time.perf_counter()
            )


            response = requests.post(

                BITQUERY_URL,

                headers=HEADERS,

                json={
                    "query":
                        QUERY,

                    "variables": {

                        "tokens":
                            token_ids,

                        "since":
                            since_time,

                        "till":
                            till_time,

                        "limit":
                            QUERY_LIMIT,
                    },
                },

                timeout=180,
            )


            query_seconds = (

                time.perf_counter()
                -
                start_clock
            )


            # 429
            if response.status_code == 429:

                wait_seconds = (
                    15
                    *
                    attempt
                )

                print(
                    f"⚠️ 429限流，"
                    f"{wait_seconds}秒后重试"
                )

                time.sleep(
                    wait_seconds
                )

                continue


            response.raise_for_status()

            result = (
                response.json()
            )


            if "errors" in result:

                raise RuntimeError(
                    str(
                        result[
                            "errors"
                        ]
                    )
                )


            rows = (
                result[
                    "data"
                ][
                    "Trading"
                ][
                    "Trades"
                ]
            )


            return (
                rows,
                query_seconds,
            )


        except Exception as error:

            if attempt >= MAX_RETRIES:

                raise


            wait_seconds = (
                5
                *
                attempt
            )


            print(
                f"⚠️ 请求失败："
                f"{error}"
            )

            print(
                f"{wait_seconds}秒后重试..."
            )

            time.sleep(
                wait_seconds
            )


# ============================================================
# 26. 找到监控起点
# ============================================================

last_success_end = (
    get_monitor_meta(
        "last_success_end"
    )
)


# 已经跑过
if last_success_end:

    last_success_dt = (
        parse_time(
            last_success_end
        )
    )


# 第一次运行
else:

    initialized_until = (
        get_init_meta(
            "wallet_state_initialized_until"
        )
    )


    if not initialized_until:

        print(
            "❌ 没找到15D钱包初始化截止时间"
        )

        connection.close()

        raise SystemExit


    last_success_dt = (

        floor_to_15_minutes(

            parse_time(
                initialized_until
            )
        )
    )


# ============================================================
# 27. 当前最近完整15分钟
# ============================================================

latest_complete_end = (

    floor_to_15_minutes(

        datetime.now(
            timezone.utc
        )
    )
)


# ============================================================
# 28. 没有新数据
# ============================================================

if (
    last_success_dt
    >=
    latest_complete_end
):

    print("=" * 72)

    print(
        "当前没有新的完整15分钟需要处理"
    )

    print("=" * 72)

    print(
        f"已经处理到："
        f"{format_time(last_success_dt)}"
    )

    connection.close()

    raise SystemExit


# ============================================================
# 29. 开始
# ============================================================

print("=" * 72)

print(
    "需求二：15分钟新钱包数据采集"
)

print("=" * 72)

print(
    "Alpha清单：读取本地文件"
)

print(
    f"当前监控Token："
    f"{len(monitor_tokens)}"
)

print(
    f"未初始化新Alpha："
    f"{len(new_uninitialized_tokens)}"
)

print(
    f"从："
    f"{format_time(last_success_dt)}"
)

print(
    f"补到："
    f"{format_time(latest_complete_end)}"
)


# ============================================================
# 30. 逐个完整15分钟处理
# ============================================================

while (
    last_success_dt
    <
    latest_complete_end
):


    # 当前15分钟开始
    interval_start_dt = (
        last_success_dt
    )


    # 当前15分钟结束
    interval_end_dt = (

        interval_start_dt

        +

        timedelta(
            minutes=15
        )
    )


    interval_start = (
        format_time(
            interval_start_dt
        )
    )


    interval_end = (
        format_time(
            interval_end_dt
        )
    )


    print(
        "\n"
        + "-" * 72
    )

    print(
        f"处理："
        f"{interval_start}"
        f" → "
        f"{interval_end}"
    )


    # ========================================================
    # 31. 查询Bitquery
    # ========================================================

    try:

        (
            rows,
            query_seconds
        ) = run_query(

            interval_start,
            interval_end,
        )


    except Exception as error:

        print(
            f"❌ 当前15分钟查询失败："
            f"{error}"
        )

        print(
            "当前区间不会写数据库。"
        )

        connection.close()

        raise SystemExit


    # ========================================================
    # 32. 是否达到25000总上限
    # ========================================================

    overflow = int(

        len(rows)
        >=
        QUERY_LIMIT
    )


    print(
        f"返回Token+Wallet："
        f"{len(rows):,} 条"
    )

    print(
        f"查询耗时："
        f"{query_seconds:.2f} 秒"
    )


    if overflow:

        print(
            "🔥 达到25,000总行上限"
        )

        print(
            "按约定："
            "不拆、不追加查询。"
        )


    # ========================================================
    # 33. 初始化Token统计
    # ========================================================

    token_stats = {}


    for token in monitor_tokens:

        token_key = (
            token[
                "token_key"
            ]
        )


        token_stats[
            token_key
        ] = {

            "symbol":
                token[
                    "symbol"
                ],

            "chain":
                token[
                    "chain"
                ],

            "buy_wallets":
                set(),

            "new_wallet_count":
                0,

            "new_wallet_buy_usd":
                0.0,

            "market_buy_usd":
                0.0,

            "market_sell_usd":
                0.0,
        }


    # ========================================================
    # 34. Token+Wallet聚合保险
    # ========================================================

    wallet_rows = {}


    for row in rows:


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


        if token_id not in token_map:

            continue


        token = (
            token_map[
                token_id
            ]
        )


        token_key = (
            token[
                "token_key"
            ]
        )


        # 钱包地址
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


        if wallet:

            wallet = normalize_wallet(

                token[
                    "chain"
                ],

                wallet,
            )


        # 买入次数
        buys = int(

            row.get(
                "buys"
            )

            or 0
        )


        # 买入金额
        buy_usd = float(

            row.get(
                "buy_usd"
            )

            or 0
        )


        # 卖出金额
        sell_usd = float(

            row.get(
                "sell_usd"
            )

            or 0
        )


        # ====================================================
        # 市场资金流
        # ====================================================

        token_stats[
            token_key
        ][
            "market_buy_usd"
        ] += buy_usd


        token_stats[
            token_key
        ][
            "market_sell_usd"
        ] += sell_usd


        # ====================================================
        # 只有买入钱包才判断新钱包
        #
        # 卖出不算新钱包。
        # ====================================================

        if (
            buys <= 0
            or
            not wallet
        ):

            continue


        first_buy_time = (

            row.get(
                "Block",
                {},
            )

            .get(
                "first_buy_time",
                "",
            )
        )


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


        if (
            not first_buy_time
            or
            not last_buy_time
        ):

            continue


        key = (
            token_key,
            wallet,
        )


        # 第一次出现
        if key not in wallet_rows:

            wallet_rows[
                key
            ] = {

                "token_key":
                    token_key,

                "wallet":
                    wallet,

                "first_buy_time":
                    first_buy_time,

                "last_buy_time":
                    last_buy_time,

                "buy_usd":
                    buy_usd,
            }


        # 出现重复则合并
        else:

            item = (
                wallet_rows[
                    key
                ]
            )


            item[
                "first_buy_time"
            ] = min(

                item[
                    "first_buy_time"
                ],

                first_buy_time,
            )


            item[
                "last_buy_time"
            ] = max(

                item[
                    "last_buy_time"
                ],

                last_buy_time,
            )


            item[
                "buy_usd"
            ] += (
                buy_usd
            )


    # ========================================================
    # 35. 判断15D新钱包
    #
    # 当前买入前15天没有买过该Token
    #
    # → 算一个15D新钱包
    # ========================================================

    state_updates = []


    for (
        (
            token_key,
            wallet
        ),
        item
    ) in wallet_rows.items():


        # 当前15分钟有买入的钱包
        token_stats[
            token_key
        ][
            "buy_wallets"
        ].add(
            wallet
        )


        # 当前第一次买入时间
        first_buy_dt = (
            parse_time(
                item[
                    "first_buy_time"
                ]
            )
        )


        # 向前15天
        threshold_time = (
            format_time(

                first_buy_dt

                -

                timedelta(
                    days=15
                )
            )
        )


        # 查询这个钱包此前最后一次买入
        cursor.execute(
            """
            SELECT last_buy_time

            FROM wallet_token_state

            WHERE
                token_key = ?
                AND wallet = ?
            """,
            (
                token_key,
                wallet,
            ),
        )


        previous = (
            cursor.fetchone()
        )


        # ====================================================
        # 新钱包条件
        # ====================================================

        is_new_wallet = (

            not previous

            or

            previous[0]
            <
            threshold_time
        )


        if is_new_wallet:

            token_stats[
                token_key
            ][
                "new_wallet_count"
            ] += 1


            token_stats[
                token_key
            ][
                "new_wallet_buy_usd"
            ] += (
                item[
                    "buy_usd"
                ]
            )


        # 无论新旧钱包，
        # 都更新最后一次买入时间
        state_updates.append(
            (
                token_key,

                wallet,

                item[
                    "last_buy_time"
                ],
            )
        )


    # ========================================================
    # 36. 写数据库
    # ========================================================

    updated_at = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


    current_date_text = (
        interval_start_dt
        .date()
        .isoformat()
    )


    try:

        connection.execute(
            "BEGIN"
        )


        # ====================================================
        # 37. 更新钱包最近一次买入
        # ====================================================

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

                        WHEN
                            excluded.last_buy_time
                            >
                            wallet_token_state.last_buy_time

                        THEN
                            excluded.last_buy_time

                        ELSE
                            wallet_token_state.last_buy_time

                    END,

                updated_at =
                    excluded.updated_at
            """,

            [

                (
                    token_key,

                    wallet,

                    last_buy_time,

                    updated_at,
                )

                for (
                    token_key,
                    wallet,
                    last_buy_time
                )

                in state_updates
            ],
        )


        # ====================================================
        # 38. 写入15分钟和今日累计
        # ====================================================

        for (
            token_key,
            stats
        ) in token_stats.items():


            market_net_buy = (

                stats[
                    "market_buy_usd"
                ]

                -

                stats[
                    "market_sell_usd"
                ]
            )


            # --------------------------------------------
            # 15分钟记录
            # --------------------------------------------

            cursor.execute(
                """
                INSERT OR REPLACE INTO
                new_wallet_15m (

                    token_key,

                    interval_start,

                    interval_end,

                    new_wallet_count,

                    buy_wallet_count,

                    new_wallet_buy_usd,

                    market_buy_usd,

                    market_sell_usd,

                    market_net_buy,

                    is_complete,

                    updated_at

                )

                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    token_key,

                    interval_start,

                    interval_end,

                    stats[
                        "new_wallet_count"
                    ],

                    len(
                        stats[
                            "buy_wallets"
                        ]
                    ),

                    stats[
                        "new_wallet_buy_usd"
                    ],

                    stats[
                        "market_buy_usd"
                    ],

                    stats[
                        "market_sell_usd"
                    ],

                    market_net_buy,

                    0
                    if overflow
                    else 1,

                    updated_at,
                ),
            )


            # --------------------------------------------
            # 今日累计
            # --------------------------------------------

            cursor.execute(
                """
                INSERT INTO daily_new_wallets (

                    date,

                    token_key,

                    new_wallet_count,

                    new_wallet_buy_usd,

                    market_buy_usd,

                    market_sell_usd,

                    market_net_buy,

                    has_overflow,

                    updated_at

                )

                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                )

                ON CONFLICT(
                    date,
                    token_key
                )

                DO UPDATE SET

                    new_wallet_count =
                        daily_new_wallets.new_wallet_count
                        +
                        excluded.new_wallet_count,

                    new_wallet_buy_usd =
                        daily_new_wallets.new_wallet_buy_usd
                        +
                        excluded.new_wallet_buy_usd,

                    market_buy_usd =
                        daily_new_wallets.market_buy_usd
                        +
                        excluded.market_buy_usd,

                    market_sell_usd =
                        daily_new_wallets.market_sell_usd
                        +
                        excluded.market_sell_usd,

                    market_net_buy =
                        daily_new_wallets.market_net_buy
                        +
                        excluded.market_net_buy,

                    has_overflow =
                        CASE

                            WHEN
                                daily_new_wallets.has_overflow = 1

                                OR

                                excluded.has_overflow = 1

                            THEN 1

                            ELSE 0

                        END,

                    updated_at =
                        excluded.updated_at
                """,
                (
                    current_date_text,

                    token_key,

                    stats[
                        "new_wallet_count"
                    ],

                    stats[
                        "new_wallet_buy_usd"
                    ],

                    stats[
                        "market_buy_usd"
                    ],

                    stats[
                        "market_sell_usd"
                    ],

                    market_net_buy,

                    overflow,

                    updated_at,
                ),
            )


        # ====================================================
        # 39. 保存当前15分钟运行状态
        # ====================================================

        cursor.execute(
            """
            INSERT OR REPLACE INTO
            new_wallet_monitor_runs (

                interval_start,

                interval_end,

                returned_rows,

                overflow,

                processed_tokens,

                completed_at

            )

            VALUES (
                ?, ?, ?, ?, ?, ?
            )
            """,
            (
                interval_start,

                interval_end,

                len(rows),

                overflow,

                len(
                    monitor_tokens
                ),

                updated_at,
            ),
        )


        # ====================================================
        # 40. 保存断点
        # ====================================================

        cursor.execute(
            """
            INSERT INTO new_wallet_monitor_meta (

                meta_key,
                meta_value

            )

            VALUES (
                'last_success_end',
                ?
            )

            ON CONFLICT(meta_key)

            DO UPDATE SET

                meta_value =
                    excluded.meta_value
            """,
            (
                interval_end,
            ),
        )


        # 一次性提交
        connection.commit()


    except Exception as error:

        connection.rollback()

        print(
            f"❌ 数据库写入失败："
            f"{error}"
        )

        print(
            "当前15分钟全部回滚。"
        )

        connection.close()

        raise SystemExit


    # ========================================================
    # 41. 读取今天累计
    # ========================================================

    cursor.execute(
        """
        SELECT

            token_key,
            new_wallet_count,
            market_net_buy

        FROM daily_new_wallets

        WHERE date = ?
        """,
        (
            current_date_text,
        ),
    )


    today_map = {

        int(
            token_key
        ): {

            "new_wallets":
                int(
                    new_wallets
                    or 0
                ),

            "net_buy":
                float(
                    net_buy
                    or 0
                ),
        }

        for (
            token_key,
            new_wallets,
            net_buy
        )

        in cursor.fetchall()
    }


    # ========================================================
    # 42. 终端TOP20
    # ========================================================

    ranking = []


    for (
        token_key,
        stats
    ) in token_stats.items():


        interval_new = (
            stats[
                "new_wallet_count"
            ]
        )


        interval_buy = len(
            stats[
                "buy_wallets"
            ]
        )


        if interval_buy > 0:

            new_ratio = (

                interval_new
                /
                interval_buy
            )

        else:

            new_ratio = 0.0


        today = (
            today_map.get(
                token_key,
                {}
            )
        )


        ranking.append(
            {

                "symbol":
                    stats[
                        "symbol"
                    ],

                "chain":
                    stats[
                        "chain"
                    ],

                "interval_new":
                    interval_new,

                "interval_buy":
                    interval_buy,

                "ratio":
                    new_ratio,

                "today_new":
                    today.get(
                        "new_wallets",
                        0,
                    ),

                "today_net":
                    today.get(
                        "net_buy",
                        0.0,
                    ),
            }
        )


    ranking.sort(

        key=lambda item:
            item[
                "interval_new"
            ],

        reverse=True,
    )


    print(
        "✅ 数据库保存成功"
    )


    print(
        "\n本15分钟新钱包 TOP20："
    )


    print(
        f"{'排名':<5}"
        f"{'Token':<15}"
        f"{'链':<10}"
        f"{'本15m新增':>10}"
        f"{'买入钱包':>10}"
        f"{'新钱包占比':>12}"
        f"{'今日累计':>10}"
        f"{'今日净买入':>16}"
    )


    print(
        "-" * 94
    )


    for (
        index,
        item
    ) in enumerate(

        ranking[
            :20
        ],

        start=1,
    ):


        print(
            f"{index:<5}"
            f"{item['symbol']:<15}"
            f"{item['chain']:<10}"
            f"{item['interval_new']:>10}"
            f"{item['interval_buy']:>10}"
            f"{item['ratio']:>11.1%}"
            f"{item['today_new']:>10}"
            f"${item['today_net']:>+15,.2f}"
        )


    # 当前15分钟处理完成
    last_success_dt = (
        interval_end_dt
    )


# ============================================================
# 43. 最终状态
# ============================================================

try:

    database_mb = (

        os.path.getsize(
            DATABASE_FILE
        )

        /
        1024
        /
        1024
    )

except Exception:

    database_mb = 0.0


print(
    "\n"
    + "=" * 72
)

print(
    "✅ 15分钟新钱包数据采集完成"
)

print("=" * 72)

print(
    f"已处理到："
    f"{format_time(last_success_dt)}"
)

print(
    f"监控Token："
    f"{len(monitor_tokens)}"
)

print(
    f"数据库大小："
    f"{database_mb:.2f} MB"
)

print(
    "异常判断：本文件不执行"
)

print(
    "Bitquery：每个完整15分钟正常1次"
)


if new_uninitialized_tokens:

    print(
        f"\n⚠️ 有 "
        f"{len(new_uninitialized_tokens)} 个"
        "新Alpha尚未初始化15D钱包历史。"
    )


connection.close()
