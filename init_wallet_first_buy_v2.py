# ============================================================
# Binance Alpha 需求二
# 首次买入钱包历史初始化 V2
#
# ------------------------------------------------------------
# V2目标
# ------------------------------------------------------------
#
# 对每一个：
#
#     Token + Wallet
#
# 只保存初始化窗口内：
#
#     first_buy_time
#
# 也就是这个钱包在当前可见历史窗口中
# 第一次买入这个Token的时间。
#
#
# ------------------------------------------------------------
# 和昨晚版本的区别
# ------------------------------------------------------------
#
# 昨晚：
#
# 先查大区间
# → 撞25,000
# → 再拆
# → 很多失败探测也消耗Points
#
#
# V2：
#
# 1. 使用本地旧钱包数量做容量估算
# 2. 不额外调用Bitquery统计钱包规模
# 3. 普通Token提前打包到约18,000
# 4. 大Token直接20,000一页
# 5. 固定历史窗口
# 6. 大Token按Trader_Address稳定排序分页
# 7. 普通批次如果意外达到20,000才拆分
# 8. 每一页都记录断点
# 9. 402立即停止
#
#
# ------------------------------------------------------------
# 命令
# ------------------------------------------------------------
#
# 只生成计划，不请求Bitquery：
#
# python init_wallet_first_buy_v2.py --prepare
#
#
# 查看状态，不请求Bitquery：
#
# python init_wallet_first_buy_v2.py --status
#
#
# 真正开始初始化：
#
# python init_wallet_first_buy_v2.py --run
#
#
# 注意：
#
# 在我们确认prepare结果之前，
# 先不要执行--run。
# ============================================================


# ============================================================
# 1. 导入模块
# ============================================================

import os
import json
import math
import time
import sqlite3
import argparse
import requests

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from dotenv import load_dotenv


# ============================================================
# 2. 基础配置
# ============================================================

DATABASE_FILE = "alpha_monitor.db"

BITQUERY_URL = (
    "https://asia.streaming.bitquery.io/graphql"
)


# 普通Token批次目标
#
# 昨天模拟使用12,000偏保守。
#
# V2提高到18,000，
# 距离20,000查询保护线仍有余量。
SMALL_BATCH_TARGET = 18000


# 超过15,000预计钱包的Token
# 直接作为大Token分页。
LARGE_TOKEN_THRESHOLD = 15000


# 大Token每页20,000
#
# 不主动靠近Bitquery默认25,000上限。
PAGE_SIZE = 20000


# 请求失败最多重试
MAX_RETRIES = 3


# 每次成功请求后暂停
#
# 主要为了避免短时间请求过密。
REQUEST_GAP_SECONDS = 2.5


# V2版本号
PLAN_VERSION = "first_buy_v2_20260829"


# ============================================================
# 3. 命令行
# ============================================================

parser = argparse.ArgumentParser(
    description="需求二 first_buy 历史初始化 V2"
)


mode = parser.add_mutually_exclusive_group(
    required=True
)


mode.add_argument(
    "--prepare",
    action="store_true",
    help="生成并冻结初始化计划，0 Bitquery请求",
)


mode.add_argument(
    "--status",
    action="store_true",
    help="查看V2初始化状态，0 Bitquery请求",
)


mode.add_argument(
    "--run",
    action="store_true",
    help="真正执行Bitquery历史初始化",
)


args = parser.parse_args()


# ============================================================
# 4. 时间格式
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
# 5. 对齐到最近一个完整15分钟
#
# 例如：
#
# 04:17
#
# ↓
#
# 04:15
# ============================================================

def floor_to_15_minutes(value):

    minute = (
        value.minute
        //
        15
        *
        15
    )

    return value.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )


# ============================================================
# 6. 当前时间
# ============================================================

def now_text():

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# ============================================================
# 7. 打开数据库
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
# 8. V2首次买入钱包表
#
# 注意：
#
# 这是全新的表。
#
# 不覆盖昨晚：
#
# wallet_token_first_buy
#
# 也不覆盖更早错误的：
#
# wallet_token_state
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_token_first_buy_v2 (

        token_key INTEGER NOT NULL,

        wallet TEXT NOT NULL,

        first_buy_time TEXT NOT NULL,

        updated_at TEXT NOT NULL,

        PRIMARY KEY (
            token_key,
            wallet
        )

    ) WITHOUT ROWID
    """
)


# ============================================================
# 9. V2计划表
#
# job_type：
#
# batch
#     普通Token批量查询
#
# large
#     大Token分页查询
#
#
# status：
#
# pending
# running
# complete
# split
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_first_buy_v2_plan (

        job_id INTEGER PRIMARY KEY AUTOINCREMENT,

        parent_job_id INTEGER,

        job_type TEXT NOT NULL,

        token_keys_json TEXT NOT NULL,

        estimated_wallets INTEGER NOT NULL,

        status TEXT NOT NULL DEFAULT 'pending',

        next_offset INTEGER NOT NULL DEFAULT 0,

        last_row_count INTEGER,

        created_at TEXT NOT NULL,

        completed_at TEXT

    )
    """
)


# ============================================================
# 10. V2元数据
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_first_buy_v2_meta (

        meta_key TEXT PRIMARY KEY,

        meta_value TEXT NOT NULL

    )
    """
)


connection.commit()


# ============================================================
# 11. Meta读取
# ============================================================

def get_meta(key):

    cursor.execute(
        """
        SELECT meta_value

        FROM wallet_first_buy_v2_meta

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
# 12. Meta写入
# ============================================================

def set_meta(
    key,
    value,
):

    cursor.execute(
        """
        INSERT INTO wallet_first_buy_v2_meta (

            meta_key,
            meta_value

        )

        VALUES (?, ?)

        ON CONFLICT(meta_key)

        DO UPDATE SET

            meta_value =
                excluded.meta_value
        """,
        (
            key,
            str(value),
        ),
    )


# ============================================================
# 13. 读取Token注册表
#
# V2继续使用已经初始化好的本地Token registry。
#
# 不访问Binance。
# 不访问Bitquery。
# ============================================================

def load_tokens():

    cursor.execute(
        """
        SELECT

            r.token_key,

            r.token_id,

            r.symbol,

            COALESCE(
                c.wallet_count,
                0
            )

        FROM alpha_token_registry r

        LEFT JOIN (

            SELECT

                token_key,

                COUNT(*) AS wallet_count

            FROM wallet_token_state

            GROUP BY token_key

        ) c

        ON
            c.token_key =
            r.token_key

        ORDER BY
            wallet_count DESC
        """
    )


    result = []


    for (
        token_key,
        token_id,
        symbol,
        wallet_count
    ) in cursor.fetchall():


        result.append(
            {

                "token_key":
                    int(
                        token_key
                    ),

                "token_id":
                    str(
                        token_id
                    ),

                "symbol":
                    str(
                        symbol
                    ),

                # 这里只拿旧表的钱包数量
                # 做查询容量估算。
                #
                # 绝不使用旧last_buy_time
                # 做业务判断。
                "estimated_wallets":
                    int(
                        wallet_count
                        or 0
                    ),
            }
        )


    return result


# ============================================================
# 14. 按预计钱包数量打包普通Token
#
# 使用First Fit Decreasing。
#
# 每个批次目标：
#
# <=18,000
# ============================================================

def pack_small_tokens(
    small_tokens,
):

    sorted_tokens = sorted(

        small_tokens,

        key=lambda item:
            item[
                "estimated_wallets"
            ],

        reverse=True,
    )


    batches = []


    for token in sorted_tokens:


        placed = False


        for batch in batches:


            current_total = sum(

                item[
                    "estimated_wallets"
                ]

                for item in batch
            )


            if (
                current_total
                +
                token[
                    "estimated_wallets"
                ]

                <=
                SMALL_BATCH_TARGET
            ):


                batch.append(
                    token
                )


                placed = True


                break


        if not placed:


            batches.append(
                [
                    token
                ]
            )


    return batches


# ============================================================
# 15. 准备计划
#
# 这一步：
#
# Bitquery = 0
#
# 并且会冻结：
#
# window_start
# window_end
#
#
# window_end：
#
# prepare执行时最近完整15分钟。
#
#
# window_start：
#
# 当天UTC 00:00往前15天。
#
#
# 这样包含：
#
# 过去15个UTC自然日
#
# +
#
# 今天截至当前检查点。
# ============================================================

def prepare_plan():


    # --------------------------------------------------------
    # 防止误重复prepare
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)

        FROM wallet_first_buy_v2_plan
        """
    )


    existing_jobs = int(
        cursor.fetchone()[0]
    )


    if existing_jobs > 0:


        print("=" * 78)

        print(
            "V2计划已经存在，不会重复生成。"
        )

        print("=" * 78)

        print(
            "如需查看："
        )

        print(
            "python init_wallet_first_buy_v2.py --status"
        )


        return


    # --------------------------------------------------------
    # 冻结结束时间
    # --------------------------------------------------------

    current_utc = datetime.now(
        timezone.utc
    )


    window_end_dt = (
        floor_to_15_minutes(
            current_utc
        )
    )


    # --------------------------------------------------------
    # 当前UTC自然日00:00
    # --------------------------------------------------------

    current_day_start = datetime(

        window_end_dt.year,

        window_end_dt.month,

        window_end_dt.day,

        tzinfo=timezone.utc,
    )


    # --------------------------------------------------------
    # 往前15个完整UTC自然日
    # --------------------------------------------------------

    window_start_dt = (

        current_day_start

        -

        timedelta(
            days=15
        )
    )


    window_start = format_time(
        window_start_dt
    )


    window_end = format_time(
        window_end_dt
    )


    # --------------------------------------------------------
    # 加载Token
    # --------------------------------------------------------

    tokens = load_tokens()


    if not tokens:

        raise RuntimeError(
            "alpha_token_registry为空，无法生成计划。"
        )


    # --------------------------------------------------------
    # 大Token
    # --------------------------------------------------------

    large_tokens = [

        token

        for token in tokens

        if token[
            "estimated_wallets"
        ]
        >
        LARGE_TOKEN_THRESHOLD
    ]


    # --------------------------------------------------------
    # 普通Token
    # --------------------------------------------------------

    small_tokens = [

        token

        for token in tokens

        if token[
            "estimated_wallets"
        ]
        <=
        LARGE_TOKEN_THRESHOLD
    ]


    # --------------------------------------------------------
    # 普通Token打包
    # --------------------------------------------------------

    small_batches = pack_small_tokens(
        small_tokens
    )


    created_at = now_text()


    # --------------------------------------------------------
    # 写大Token任务
    # --------------------------------------------------------

    for token in large_tokens:


        cursor.execute(
            """
            INSERT INTO wallet_first_buy_v2_plan (

                parent_job_id,

                job_type,

                token_keys_json,

                estimated_wallets,

                status,

                next_offset,

                created_at

            )

            VALUES (
                NULL,
                'large',
                ?,
                ?,
                'pending',
                0,
                ?
            )
            """,
            (
                json.dumps(
                    [
                        token[
                            "token_key"
                        ]
                    ]
                ),

                token[
                    "estimated_wallets"
                ],

                created_at,
            ),
        )


    # --------------------------------------------------------
    # 写普通批次任务
    # --------------------------------------------------------

    for batch in small_batches:


        token_keys = [

            item[
                "token_key"
            ]

            for item in batch
        ]


        estimated_total = sum(

            item[
                "estimated_wallets"
            ]

            for item in batch
        )


        cursor.execute(
            """
            INSERT INTO wallet_first_buy_v2_plan (

                parent_job_id,

                job_type,

                token_keys_json,

                estimated_wallets,

                status,

                next_offset,

                created_at

            )

            VALUES (
                NULL,
                'batch',
                ?,
                ?,
                'pending',
                0,
                ?
            )
            """,
            (
                json.dumps(
                    token_keys
                ),

                estimated_total,

                created_at,
            ),
        )


    # --------------------------------------------------------
    # 保存冻结窗口
    # --------------------------------------------------------

    set_meta(
        "plan_version",
        PLAN_VERSION,
    )


    set_meta(
        "window_start",
        window_start,
    )


    set_meta(
        "window_end",
        window_end,
    )


    set_meta(
        "status",
        "prepared",
    )


    set_meta(
        "token_count",
        len(
            tokens
        ),
    )


    set_meta(
        "successful_query_count",
        0,
    )


    connection.commit()


    # --------------------------------------------------------
    # 预计大Token基础分页数
    # --------------------------------------------------------

    large_pages = 0


    for token in large_tokens:


        pages = max(

            1,

            math.ceil(

                token[
                    "estimated_wallets"
                ]

                /
                PAGE_SIZE
            ),
        )


        large_pages += pages


    estimated_calls = (

        large_pages

        +
        len(
            small_batches
        )
    )


    # --------------------------------------------------------
    # 输出
    # --------------------------------------------------------

    print("=" * 78)

    print(
        "需求二 first_buy V2 初始化计划"
    )

    print("=" * 78)

    print(
        f"冻结开始：{window_start}"
    )

    print(
        f"冻结结束：{window_end}"
    )

    print(
        f"Token数量：{len(tokens)}"
    )

    print(
        f"大Token：{len(large_tokens)}"
    )

    print(
        f"普通Token：{len(small_tokens)}"
    )

    print(
        f"普通批次：{len(small_batches)}"
    )

    print(
        f"大Token基础分页：约 {large_pages} 页"
    )

    print(
        f"预计基础请求：约 {estimated_calls} 次"
    )

    print(
        "\nBitquery请求：0"
    )

    print(
        "Points消耗：0"
    )


    print(
        "\n"
        + "=" * 78
    )

    print(
        "大Token计划"
    )

    print("=" * 78)


    for token in large_tokens:


        pages = max(

            1,

            math.ceil(

                token[
                    "estimated_wallets"
                ]

                /
                PAGE_SIZE
            ),
        )


        print(
            f"{token['symbol']:<20}"
            f"预计钱包 "
            f"{token['estimated_wallets']:>8,}"
            f"    基础页数 {pages}"
        )


# ============================================================
# 16. 查看状态
#
# 0 Bitquery Points
# ============================================================

def show_status():


    print("=" * 78)

    print(
        "需求二 first_buy V2 状态"
    )

    print("=" * 78)


    plan_version = get_meta(
        "plan_version"
    )


    window_start = get_meta(
        "window_start"
    )


    window_end = get_meta(
        "window_end"
    )


    status = get_meta(
        "status"
    )


    query_count = get_meta(
        "successful_query_count"
    )


    if not plan_version:


        print(
            "当前还没有生成V2计划。"
        )


        print(
            "\n请执行："
        )


        print(
            "python init_wallet_first_buy_v2.py --prepare"
        )


        return


    print(
        f"版本：{plan_version}"
    )

    print(
        f"窗口：{window_start}"
    )

    print(
        f"   → {window_end}"
    )

    print(
        f"状态：{status}"
    )

    print(
        f"成功Bitquery请求：{query_count or 0}"
    )


    # --------------------------------------------------------
    # 各状态任务数
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT

            status,

            COUNT(*)

        FROM wallet_first_buy_v2_plan

        GROUP BY status

        ORDER BY status
        """
    )


    print(
        "\n任务状态："
    )


    for (
        job_status,
        count
    ) in cursor.fetchall():


        print(
            f"{job_status:<12}"
            f"{count:>6}"
        )


    # --------------------------------------------------------
    # first_buy钱包数量
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)

        FROM wallet_token_first_buy_v2
        """
    )


    wallet_count = int(
        cursor.fetchone()[0]
    )


    print(
        f"\n当前V2 Token-Wallet："
        f"{wallet_count:,}"
    )


    # --------------------------------------------------------
    # 未完成任务前20
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT

            job_id,

            job_type,

            token_keys_json,

            estimated_wallets,

            status,

            next_offset

        FROM wallet_first_buy_v2_plan

        WHERE status IN (
            'pending',
            'running'
        )

        ORDER BY job_id

        LIMIT 20
        """
    )


    pending_rows = cursor.fetchall()


    if pending_rows:


        tokens = load_tokens()


        token_name = {

            item[
                "token_key"
            ]:
                item[
                    "symbol"
                ]

            for item in tokens
        }


        print(
            "\n未完成任务前20："
        )


        for (
            job_id,
            job_type,
            token_keys_json,
            estimated_wallets,
            job_status,
            next_offset
        ) in pending_rows:


            token_keys = json.loads(
                token_keys_json
            )


            names = ", ".join(

                token_name.get(
                    int(
                        token_key
                    ),
                    str(
                        token_key
                    ),
                )

                for token_key in token_keys
            )


            print(
                f"Job {job_id:<4}"
                f"{job_type:<8}"
                f"{job_status:<10}"
                f"offset={next_offset:<7}"
                f"预计={estimated_wallets:<8,}"
                f"{names}"
            )


    print(
        "\nBitquery请求：0"
    )


# ============================================================
# 17. 钱包地址标准化
#
# EVM钱包统一小写。
#
# Solana/TRON保持原样。
# ============================================================

def normalize_wallet(
    token_id,
    wallet,
):


    wallet = str(
        wallet or ""
    ).strip()


    if (

        token_id.startswith(
            "bid:bsc:"
        )

        or

        token_id.startswith(
            "bid:base:"
        )

        or

        token_id.startswith(
            "bid:eth:"
        )

        or

        token_id.startswith(
            "bid:arbitrum:"
        )
    ):


        return wallet.lower()


    return wallet


# ============================================================
# 18. Bitquery查询
#
# 只查询Buy。
#
# 聚合粒度：
#
# Token + Trader
#
# 对该Token+Wallet：
#
# first_buy_time =
# minimum(Block_Time)
#
#
# limit：
#
# 20,000
#
#
# large单Token分页时：
#
# offset：
#
# 0
# 20000
# 40000
# ...
#
#
# 固定时间窗口 +
# 单Token +
# Trader_Address排序
#
# 用于稳定分页。
# ============================================================

QUERY = """
query FirstBuyV2(
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
# 19. Bitquery调用
# ============================================================

def run_query(
    token_ids,
    since_time,
    till_time,
    offset,
):


    # --run时才检查Token
    load_dotenv()


    bitquery_token = os.getenv(
        "BITQUERY_TOKEN"
    )


    if not bitquery_token:


        raise RuntimeError(
            "没有读取到BITQUERY_TOKEN"
        )


    headers = {

        "Content-Type":
            "application/json",

        "Authorization":
            f"Bearer {bitquery_token}",
    }


    variables = {

        "tokens":
            token_ids,

        "since":
            since_time,

        "till":
            till_time,

        "limit":
            PAGE_SIZE,

        "offset":
            offset,
    }


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

                headers=headers,

                json={

                    "query":
                        QUERY,

                    "variables":
                        variables,
                },

                timeout=180,
            )


            seconds = (

                time.perf_counter()

                -

                start_clock
            )


            # ------------------------------------------------
            # 402
            #
            # 额度/套餐问题。
            #
            # 立即停止。
            #
            # 不做昨晚那种重复重试。
            # ------------------------------------------------

            if response.status_code == 402:


                raise RuntimeError(
                    "BITQUERY_402_PAYMENT_REQUIRED"
                )


            # ------------------------------------------------
            # 429
            # ------------------------------------------------

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


            result = response.json()


            # ------------------------------------------------
            # GraphQL错误
            # ------------------------------------------------

            if "errors" in result:


                error_text = str(
                    result[
                        "errors"
                    ]
                )


                # timeout类允许少量重试
                if (
                    "deadline"
                    in
                    error_text.lower()
                ):


                    if attempt < MAX_RETRIES:


                        wait_seconds = (
                            5
                            *
                            attempt
                        )


                        print(
                            "⚠️ Bitquery timeout，"
                            f"{wait_seconds}秒后重试"
                        )


                        time.sleep(
                            wait_seconds
                        )


                        continue


                raise RuntimeError(
                    error_text
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


            # ------------------------------------------------
            # 记录实际成功请求数量
            # ------------------------------------------------

            current_count = int(

                get_meta(
                    "successful_query_count"
                )

                or 0
            )


            set_meta(

                "successful_query_count",

                current_count + 1,
            )


            connection.commit()


            return (
                rows,
                seconds,
            )


        except RuntimeError as error:


            # 402绝不重试
            if (
                str(
                    error
                )
                ==
                "BITQUERY_402_PAYMENT_REQUIRED"
            ):


                raise


            if attempt >= MAX_RETRIES:

                raise


            wait_seconds = (
                5
                *
                attempt
            )


            print(
                f"⚠️ 请求错误：{error}"
            )


            print(
                f"{wait_seconds}秒后重试"
            )


            time.sleep(
                wait_seconds
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
                f"⚠️ 网络错误：{error}"
            )


            print(
                f"{wait_seconds}秒后重试"
            )


            time.sleep(
                wait_seconds
            )


# ============================================================
# 20. 把Bitquery结果写入first_buy V2
#
# 冲突时：
#
# 永远保留更早的first_buy_time。
# ============================================================

def save_rows(
    rows,
    token_by_id,
):


    insert_rows = []


    updated_at = now_text()


    for row in rows:


        token_id = (

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


        if token_id not in token_by_id:

            continue


        token = token_by_id[
            token_id
        ]


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


        if (
            not wallet

            or

            not first_buy_time
        ):

            continue


        insert_rows.append(
            (
                token[
                    "token_key"
                ],

                wallet,

                first_buy_time,

                updated_at,
            )
        )


    if not insert_rows:

        return 0


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


    return len(
        insert_rows
    )


# ============================================================
# 21. 把普通批次拆成两个更小批次
#
# 只在意外达到20,000时使用。
#
# 不是正常路径。
# ============================================================

def split_batch(
    token_keys,
    estimate_by_key,
):


    ordered = sorted(

        token_keys,

        key=lambda key:
            estimate_by_key.get(
                int(
                    key
                ),
                0,
            ),

        reverse=True,
    )


    left = []

    right = []


    left_total = 0

    right_total = 0


    for token_key in ordered:


        estimate = estimate_by_key.get(
            int(
                token_key
            ),
            0,
        )


        if left_total <= right_total:


            left.append(
                int(
                    token_key
                )
            )


            left_total += (
                estimate
            )


        else:


            right.append(
                int(
                    token_key
                )
            )


            right_total += (
                estimate
            )


    return (
        left,
        right,
        left_total,
        right_total,
    )


# ============================================================
# 22. 新建拆分后的子任务
# ============================================================

def create_child_job(
    parent_job_id,
    token_keys,
    estimated_wallets,
):


    cursor.execute(
        """
        INSERT INTO wallet_first_buy_v2_plan (

            parent_job_id,

            job_type,

            token_keys_json,

            estimated_wallets,

            status,

            next_offset,

            created_at

        )

        VALUES (
            ?,
            'batch',
            ?,
            ?,
            'pending',
            0,
            ?
        )
        """,
        (
            parent_job_id,

            json.dumps(
                token_keys
            ),

            int(
                estimated_wallets
            ),

            now_text(),
        ),
    )


# ============================================================
# 23. 真正执行初始化
# ============================================================

def run_initialization():


    window_start = get_meta(
        "window_start"
    )


    window_end = get_meta(
        "window_end"
    )


    if (
        not window_start

        or

        not window_end
    ):


        raise RuntimeError(
            "尚未prepare。请先执行 --prepare"
        )


    # --------------------------------------------------------
    # Token映射
    # --------------------------------------------------------

    tokens = load_tokens()


    token_by_key = {

        token[
            "token_key"
        ]:
            token

        for token in tokens
    }


    token_by_id = {

        token[
            "token_id"
        ]:
            token

        for token in tokens
    }


    estimate_by_key = {

        token[
            "token_key"
        ]:
            token[
                "estimated_wallets"
            ]

        for token in tokens
    }


    set_meta(
        "status",
        "running",
    )


    connection.commit()


    print("=" * 78)

    print(
        "需求二 first_buy V2 正式初始化"
    )

    print("=" * 78)

    print(
        f"窗口：{window_start}"
    )

    print(
        f"   → {window_end}"
    )

    print(
        f"Token：{len(tokens)}"
    )

    print(
        f"普通批次目标：{SMALL_BATCH_TARGET:,}"
    )

    print(
        f"分页大小：{PAGE_SIZE:,}"
    )


    # --------------------------------------------------------
    # 每次取一个pending任务
    #
    # 这样运行过程中如果产生新的拆分子任务，
    # 本轮也能继续处理。
    # --------------------------------------------------------

    while True:


        cursor.execute(
            """
            SELECT

                job_id,

                job_type,

                token_keys_json,

                estimated_wallets,

                next_offset

            FROM wallet_first_buy_v2_plan

            WHERE status IN (
                'pending',
                'running'
            )

            ORDER BY job_id

            LIMIT 1
            """
        )


        job = cursor.fetchone()


        # ----------------------------------------------------
        # 没有pending任务
        # ----------------------------------------------------

        if not job:

            break


        (
            job_id,
            job_type,
            token_keys_json,
            estimated_wallets,
            next_offset
        ) = job


        token_keys = [

            int(
                item
            )

            for item in json.loads(
                token_keys_json
            )
        ]


        token_ids = [

            token_by_key[
                token_key
            ][
                "token_id"
            ]

            for token_key in token_keys
        ]


        symbols = [

            token_by_key[
                token_key
            ][
                "symbol"
            ]

            for token_key in token_keys
        ]


        print(
            "\n"
            + "-" * 78
        )


        print(
            f"Job {job_id}"
        )


        print(
            f"类型：{job_type}"
        )


        print(
            f"Token：{', '.join(symbols)}"
        )


        print(
            f"预计钱包：{estimated_wallets:,}"
        )


        # ----------------------------------------------------
        # 标记running
        # ----------------------------------------------------

        cursor.execute(
            """
            UPDATE wallet_first_buy_v2_plan

            SET status = 'running'

            WHERE job_id = ?
            """,
            (
                job_id,
            ),
        )


        connection.commit()


        # ====================================================
        # A. 大Token分页
        # ====================================================

        if job_type == "large":


            # large任务一定只能有一个Token
            if len(
                token_keys
            ) != 1:


                raise RuntimeError(
                    f"Job {job_id} large任务Token数量异常"
                )


            offset = int(
                next_offset
                or 0
            )


            while True:


                print(
                    f"查询 offset={offset:,}"
                )


                (
                    rows,
                    seconds
                ) = run_query(

                    token_ids,

                    window_start,

                    window_end,

                    offset,
                )


                row_count = len(
                    rows
                )


                print(
                    f"返回：{row_count:,} 条，"
                    f"{seconds:.2f} 秒"
                )


                # --------------------------------------------
                # 保存当前页
                # --------------------------------------------

                connection.execute(
                    "BEGIN"
                )


                saved = save_rows(

                    rows,

                    token_by_id,
                )


                # --------------------------------------------
                # 最后一页
                #
                # <20,000
                # → Token完成
                # --------------------------------------------

                if row_count < PAGE_SIZE:


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

                            now_text(),

                            job_id,
                        ),
                    )


                    connection.commit()


                    print(
                        f"✅ 大Token完成，"
                        f"本页保存 {saved:,}"
                    )


                    break


                # --------------------------------------------
                # 还有下一页
                # --------------------------------------------

                offset += PAGE_SIZE


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
                        offset,

                        row_count,

                        job_id,
                    ),
                )


                connection.commit()


                print(
                    f"✅ 当前页已保存，"
                    f"下一页 offset={offset:,}"
                )


                time.sleep(
                    REQUEST_GAP_SECONDS
                )


        # ====================================================
        # B. 普通批次
        # ====================================================

        elif job_type == "batch":


            (
                rows,
                seconds
            ) = run_query(

                token_ids,

                window_start,

                window_end,

                0,
            )


            row_count = len(
                rows
            )


            print(
                f"返回：{row_count:,} 条，"
                f"{seconds:.2f} 秒"
            )


            # --------------------------------------------
            # 正常情况
            #
            # <20,000
            # --------------------------------------------

            if row_count < PAGE_SIZE:


                connection.execute(
                    "BEGIN"
                )


                saved = save_rows(

                    rows,

                    token_by_id,
                )


                cursor.execute(
                    """
                    UPDATE wallet_first_buy_v2_plan

                    SET

                        status = 'complete',

                        last_row_count = ?,

                        completed_at = ?

                    WHERE job_id = ?
                    """,
                    (
                        row_count,

                        now_text(),

                        job_id,
                    ),
                )


                connection.commit()


                print(
                    f"✅ 普通批次完成，"
                    f"保存 {saved:,}"
                )


            # --------------------------------------------
            # 意外达到20,000
            #
            # 说明计划低估。
            # --------------------------------------------

            else:


                print(
                    "⚠️ 普通批次达到20,000保护线"
                )


                # ----------------------------------------
                # 如果本批只有1个Token
                #
                # 直接转换成large分页。
                #
                # 当前第0页已经拿到了，
                # 不浪费。
                # ----------------------------------------

                if len(
                    token_keys
                ) == 1:


                    connection.execute(
                        "BEGIN"
                    )


                    saved = save_rows(

                        rows,

                        token_by_id,
                    )


                    cursor.execute(
                        """
                        UPDATE wallet_first_buy_v2_plan

                        SET

                            job_type = 'large',

                            status = 'pending',

                            next_offset = ?,

                            last_row_count = ?

                        WHERE job_id = ?
                        """,
                        (
                            PAGE_SIZE,

                            row_count,

                            job_id,
                        ),
                    )


                    connection.commit()


                    print(
                        f"→ 单Token转分页模式，"
                        f"第1页已保存 {saved:,}"
                    )


                # ----------------------------------------
                # 多Token
                #
                # 不保存这20,000条partial结果。
                #
                # 拆成两个更小批次。
                # ----------------------------------------

                else:


                    (
                        left,
                        right,
                        left_total,
                        right_total
                    ) = split_batch(

                        token_keys,

                        estimate_by_key,
                    )


                    if (
                        not left

                        or

                        not right
                    ):


                        raise RuntimeError(
                            "批次自动拆分失败"
                        )


                    connection.execute(
                        "BEGIN"
                    )


                    cursor.execute(
                        """
                        UPDATE wallet_first_buy_v2_plan

                        SET

                            status = 'split',

                            last_row_count = ?,

                            completed_at = ?

                        WHERE job_id = ?
                        """,
                        (
                            row_count,

                            now_text(),

                            job_id,
                        ),
                    )


                    create_child_job(

                        job_id,

                        left,

                        left_total,
                    )


                    create_child_job(

                        job_id,

                        right,

                        right_total,
                    )


                    connection.commit()


                    print(
                        "→ 自动拆成两个更小批次"
                    )


                    print(
                        f"  A：{len(left)} Token，"
                        f"预计 {left_total:,}"
                    )


                    print(
                        f"  B：{len(right)} Token，"
                        f"预计 {right_total:,}"
                    )


        else:


            raise RuntimeError(
                f"未知job_type：{job_type}"
            )


        # ----------------------------------------------------
        # 成功处理一个请求/任务后稍作间隔
        # ----------------------------------------------------

        time.sleep(
            REQUEST_GAP_SECONDS
        )


    # ========================================================
    # 检查是否全部完成
    # ========================================================

    cursor.execute(
        """
        SELECT COUNT(*)

        FROM wallet_first_buy_v2_plan

        WHERE status IN (
            'pending',
            'running'
        )
        """
    )


    remaining = int(
        cursor.fetchone()[0]
    )


    if remaining == 0:


        set_meta(
            "status",
            "complete",
        )


        set_meta(
            "wallet_first_buy_v2_initialized_until",
            window_end,
        )


        connection.commit()


        cursor.execute(
            """
            SELECT COUNT(*)

            FROM wallet_token_first_buy_v2
            """
        )


        total_wallets = int(
            cursor.fetchone()[0]
        )


        print(
            "\n"
            + "=" * 78
        )


        print(
            "✅ first_buy V2 历史初始化完成"
        )


        print("=" * 78)


        print(
            f"Token-Wallet："
            f"{total_wallets:,}"
        )


        print(
            f"初始化截止："
            f"{window_end}"
        )


        print(
            f"成功Bitquery请求："
            f"{get_meta('successful_query_count')}"
        )


    else:


        print(
            f"\n仍有 {remaining} 个任务未完成。"
        )


# ============================================================
# 24. 主程序
# ============================================================

try:


    if args.prepare:


        prepare_plan()


    elif args.status:


        show_status()


    elif args.run:


        run_initialization()


except Exception as error:


    # --------------------------------------------------------
    # 额度402
    # --------------------------------------------------------

    if (
        str(
            error
        )
        ==
        "BITQUERY_402_PAYMENT_REQUIRED"
    ):


        set_meta(
            "status",
            "paused_402",
        )


        connection.commit()


        print(
            "\n"
            + "=" * 78
        )


        print(
            "❌ Bitquery返回402"
        )


        print("=" * 78)


        print(
            "当前任务进度已经保存。"
        )


        print(
            "不会重新查询已经完成的页。"
        )


        print(
            "额度恢复后重新执行 --run 即可。"
        )


    else:


        print(
            "\n"
            + "=" * 78
        )


        print(
            "❌ V2初始化出现错误"
        )


        print("=" * 78)


        print(
            str(
                error
            )
        )


        print(
            "\n已经提交的页和任务不会丢失。"
        )


        print(
            "修复问题后重新执行同一个命令即可。"
        )


finally:


    connection.close()
