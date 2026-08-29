# ============================================================
# Binance Alpha 埋伏池 —— 快速每日更新版
#
# 每天执行一次：
#
# 1. 自动刷新 Binance Alpha 清单
# 2. 排除 offline 和 stock
# 3. 将所有 Bitquery 支持链的 Alpha 合并
# 4. 一次 API 请求查询昨天 1 个完整 UTC 日
# 5. 保存每日资金数据到 SQLite
# 6. 新 Alpha 如果没有30天历史，只给新币补历史
# 7. 重新计算最近30天埋伏池
#
# 埋伏条件：
# 正净买入天数 >= 20
# 且
# 30D累计净买入 > 50,000 USD
#
# 当前统计日口径：
# UTC自然日
# ============================================================


# 导入 os，用于文件操作
import os

# 导入 sys，用于获取当前 Python
import sys

# 导入 csv，用于读取和输出 CSV
import csv

# 导入 sqlite3，用于本地数据库
import sqlite3

# 导入 subprocess，用于自动刷新 Alpha 清单
import subprocess

# 导入 time，用于重试等待和测速
import time

# 导入 requests，用于请求 Bitquery
import requests

# 导入日期工具
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env
from dotenv import load_dotenv


# ============================================================
# 1. 基础配置
# ============================================================

# 读取 .env
load_dotenv()

# 获取 Bitquery Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 没有 Token 就停止
if not BITQUERY_TOKEN:
    raise ValueError(
        "没有读取到 BITQUERY_TOKEN，请检查 .env"
    )


# Bitquery 亚洲节点
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

# Alpha 清单文件
ALPHA_FILE = "alpha_tokens_active.csv"

# Alpha 清单刷新程序
ALPHA_REFRESH_SCRIPT = "get_alpha_tokens.py"

# SQLite 数据库
DATABASE_FILE = "alpha_monitor.db"

# 最终埋伏池
OUTPUT_FILE = "alpha_ambush_pool.csv"

# 临时输出文件
TEMP_OUTPUT_FILE = "alpha_ambush_pool.csv.tmp"


# ============================================================
# 2. 埋伏池条件
# ============================================================

# 最近30天至少20天净买入大于0
MIN_POSITIVE_DAYS = 20

# 最近30天累计净买入必须大于5万美元
MIN_NETFLOW_30D = 50000.0


# ============================================================
# 3. Bitquery Token ID 链前缀
# ============================================================

# Token ID 自带链信息
#
# 例如：
# bid:bsc:0x...
# bid:solana:xxxx
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

# EVM 地址统一转换为小写
EVM_CHAINS = {
    "BSC",
    "Base",
    "Ethereum",
    "Arbitrum",
}


# ============================================================
# 5. 地址标准化函数
# ============================================================

def normalize_address(chain, address):

    # 转成字符串并去掉空格
    address = str(address).strip()

    # EVM 地址转小写
    if chain in EVM_CHAINS:
        address = address.lower()

    # 返回处理后的地址
    return address


# ============================================================
# 6. 构造 Bitquery Token ID
# ============================================================

def make_token_id(chain, address):

    # 获取当前链对应的前缀
    prefix = TOKEN_ID_PREFIX[chain]

    # 拼成完整 Token ID
    return prefix + ":" + address


# ============================================================
# 7. 计算昨天以及滚动30天
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

# 滚动30天的第一天
window_start_date = (
    target_date
    - timedelta(days=29)
)

# 昨天 UTC 00:00
target_start = datetime(
    year=target_date.year,
    month=target_date.month,
    day=target_date.day,
    tzinfo=timezone.utc,
)

# 今天 UTC 00:00
target_end = (
    target_start
    + timedelta(days=1)
)

# 转为 Bitquery 时间格式
target_start_str = (
    target_start
    .isoformat()
    .replace("+00:00", "Z")
)

# 转换截止时间
target_end_str = (
    target_end
    .isoformat()
    .replace("+00:00", "Z")
)


# ============================================================
# 8. 显示程序开始
# ============================================================

print("=" * 72)

print("Binance Alpha 埋伏池快速每日更新")

print("=" * 72)

print(
    f"更新日期：{target_date} UTC"
)

print(
    f"30D窗口："
    f"{window_start_date} 至 {target_date}"
)


# ============================================================
# 9. 自动刷新 Binance Alpha 清单
# ============================================================

print(
    "\n正在刷新 Binance Alpha 清单..."
)

try:

    # --------------------------------------------------------
    # 继续调用原来的 get_alpha_tokens.py。
    #
    # 这里只访问 Binance 公共接口，
    # 不访问 Bitquery。
    # --------------------------------------------------------

    refresh_result = subprocess.run(
        [
            sys.executable,
            ALPHA_REFRESH_SCRIPT,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    print(
        "✅ Binance Alpha 清单刷新成功"
    )


except subprocess.CalledProcessError as error:

    print(
        "\n❌ Binance Alpha 清单刷新失败"
    )

    if error.stderr:

        print(
            error.stderr
        )

    print(
        "旧 alpha_ambush_pool.csv 保持不变"
    )

    sys.exit(1)


# ============================================================
# 10. 读取当前有效 Binance Alpha
# ============================================================

all_tokens = []


with open(
    ALPHA_FILE,
    "r",
    encoding="utf-8-sig",
) as file:

    reader = csv.DictReader(
        file
    )


    for row in reader:

        # ----------------------------------------------------
        # 链名称
        # ----------------------------------------------------

        chain = (
            row.get(
                "chainName",
                "",
            )
            .strip()
        )


        # ----------------------------------------------------
        # Token简称
        # ----------------------------------------------------

        symbol = (
            row.get(
                "symbol",
                "",
            )
            .strip()
        )


        # ----------------------------------------------------
        # 合约地址
        #
        # 继续使用旧脚本前面已经定义好的地址规范化函数。
        # ----------------------------------------------------

        address = normalize_address(
            chain,
            row.get(
                "contractAddress",
                "",
            ),
        )


        if not address:
            continue


        # ----------------------------------------------------
        # TOKEN_ID_PREFIX 是旧脚本已有的支持链映射。
        #
        # 虽然需求一现在已经不调用 Bitquery，
        # 但 daily_fund_flow 本身目前就是这些支持链产生的。
        #
        # 因此这里继续用它判断哪些链具备资金历史。
        # ----------------------------------------------------

        if chain not in TOKEN_ID_PREFIX:
            continue


        all_tokens.append(
            {
                "symbol":
                    symbol,

                "chain":
                    chain,

                "address":
                    address,
            }
        )


# ============================================================
# 11. 按 链 + 合约地址 去重
# ============================================================

supported_tokens = []

seen = set()


for token in all_tokens:

    key = (
        token["chain"],
        token["address"],
    )


    if key in seen:
        continue


    seen.add(
        key
    )

    supported_tokens.append(
        token
    )


print(
    "\n当前可计算 Alpha：",
    len(
        supported_tokens
    )
)


# ============================================================
# 12. 打开本地数据库
# ============================================================

connection = sqlite3.connect(
    DATABASE_FILE
)

cursor = connection.cursor()


# ============================================================
# 13. 确保 daily_fund_flow 存在
#
# 以后这张表由需求二每天自动写入。
#
# 需求一只读取，
# 不再负责向 Bitquery 查询昨天数据。
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS
    daily_fund_flow (

        date TEXT NOT NULL,

        symbol TEXT,

        chain TEXT NOT NULL,

        contract_address TEXT NOT NULL,

        buy_usd REAL NOT NULL DEFAULT 0,

        sell_usd REAL NOT NULL DEFAULT 0,

        netflow_usd REAL NOT NULL DEFAULT 0,

        updated_at TEXT,

        PRIMARY KEY (
            date,
            chain,
            contract_address
        )
    )
    """
)

connection.commit()


# ============================================================
# 14. 检查需求二是否已经产生昨天完整日数据
#
# target_date：
# 旧脚本前面已经算好的“昨天 UTC 日期”。
#
# 如果昨天还没有进入 daily_fund_flow，
# 需求一直接停止。
#
# 绝对不能拿前天冒充昨天继续算。
# ============================================================

cursor.execute(
    """
    SELECT
        MAX(date)

    FROM daily_fund_flow
    """
)

latest_date_row = cursor.fetchone()

latest_date = (
    latest_date_row[0]
    if latest_date_row
    else None
)


print(
    "需求二资金数据最新日期：",
    latest_date
)


print(
    "本次需求一目标日期：",
    str(
        target_date
    )
)


if latest_date != str(
    target_date
):

    connection.close()

    print()

    print(
        "❌ daily_fund_flow 尚未生成目标日数据"
    )

    print(
        "❌ 本次不更新埋伏池"
    )

    print(
        "✅ 旧 alpha_ambush_pool.csv 保持不变"
    )

    sys.exit(1)


# ============================================================
# 15. 查看目标日共有多少资金记录
#
#这里只用于检查和日志。
#
# 不再写死305。
# Binance Alpha以后增减币，也不会因为数量变化直接报错。
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)

    FROM daily_fund_flow

    WHERE date = ?
    """,
    (
        str(
            target_date
        ),
    ),
)

target_day_rows = (
    cursor.fetchone()[0]
    or 0
)


print(
    "目标日资金记录：",
    target_day_rows
)


if target_day_rows <= 0:

    connection.close()

    raise RuntimeError(
        "目标日没有任何资金记录，"
        "正式埋伏池未修改。"
    )


# ============================================================
# 16. 计算当前 Alpha 的最近30日
# ============================================================

print()

print(
    "正在使用本地 daily_fund_flow "
    "计算最新30D埋伏池..."
)


ambush_pool = []

complete_history_count = 0

insufficient_history = []


for token in supported_tokens:

    # --------------------------------------------------------
    # 对当前 Token 查询最近30个完整UTC自然日
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT

            COUNT(
                DISTINCT date
            ) AS total_days,

            COUNT(
                CASE
                    WHEN netflow_usd > 0
                    THEN 1
                END
            ) AS positive_days,

            SUM(
                netflow_usd
            ) AS netflow_30d

        FROM daily_fund_flow

        WHERE
            chain = ?

            AND contract_address = ?

            AND date >= ?

            AND date <= ?
        """,
        (
            token[
                "chain"
            ],

            token[
                "address"
            ],

            str(
                window_start_date
            ),

            str(
                target_date
            ),
        ),
    )


    result = cursor.fetchone()


    total_days = (
        result[0]
        or 0
    )


    positive_days = (
        result[1]
        or 0
    )


    netflow_30d = float(
        result[2]
        or 0
    )


    # --------------------------------------------------------
    # 新上线 Alpha 很可能没有30天历史。
    #
    # 新逻辑：
    # 历史不足30天 → 直接跳过。
    #
    # 不再为了它额外调用 Bitquery 补历史。
    # --------------------------------------------------------

    if total_days != 30:

        insufficient_history.append(
            (
                token[
                    "symbol"
                ],

                total_days,
            )
        )

        continue


    complete_history_count += 1


    # --------------------------------------------------------
    # 正净流入天数占比
    # --------------------------------------------------------

    positive_ratio = (
        positive_days
        /
        30
    )


    # --------------------------------------------------------
    # 需求一固定规则：
    #
    # ① 最近30天
    # ② 正净流入天数 >= 20
    # ③ 30D累计净流入 > 50,000美元
    # --------------------------------------------------------

    if (
        positive_days
        >=
        MIN_POSITIVE_DAYS

        and

        netflow_30d
        >
        MIN_NETFLOW_30D
    ):

        ambush_pool.append(
            {
                "symbol":
                    token[
                        "symbol"
                    ],

                "chain":
                    token[
                        "chain"
                    ],

                "contract_address":
                    token[
                        "address"
                    ],

                "positive_days":
                    positive_days,

                "positive_ratio":
                    positive_ratio,

                "netflow_30d":
                    netflow_30d,
            }
        )


# ============================================================
# 17. 按30D累计净流入从高到低排名
# ============================================================

ambush_pool.sort(
    key=lambda item:
        item[
            "netflow_30d"
        ],

    reverse=True,
)


# ============================================================
# 18. 当前更新时间
# ============================================================

updated_at = (
    datetime.now(
        timezone.utc
    )
    .strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
)


# ============================================================
# 19. 先生成临时CSV
#
# 只有整个计算成功，
# 才会覆盖正式文件。
# ============================================================

with open(
    TEMP_OUTPUT_FILE,
    "w",
    newline="",
    encoding="utf-8-sig",
) as file:

    fields = [
        "rank",
        "symbol",
        "chain",
        "contract_address",
        "positive_days",
        "positive_ratio",
        "netflow_30d",
        "updated_at",
    ]


    writer = csv.DictWriter(
        file,
        fieldnames=fields,
    )


    writer.writeheader()


    for rank, item in enumerate(
        ambush_pool,
        start=1,
    ):

        writer.writerow(
            {
                "rank":
                    rank,

                "symbol":
                    item[
                        "symbol"
                    ],

                "chain":
                    item[
                        "chain"
                    ],

                "contract_address":
                    item[
                        "contract_address"
                    ],

                "positive_days":
                    item[
                        "positive_days"
                    ],

                "positive_ratio":
                    (
                        f"{item['positive_ratio']:.2%}"
                    ),

                "netflow_30d":
                    round(
                        item[
                            "netflow_30d"
                        ],
                        2,
                    ),

                "updated_at":
                    updated_at,
            }
        )


# ============================================================
# 20. 全部成功后原子替换正式埋伏池
# ============================================================

os.replace(
    TEMP_OUTPUT_FILE,
    OUTPUT_FILE,
)


# ============================================================
# 21. 关闭数据库
# ============================================================

connection.close()


# ============================================================
# 22. 最终输出
# ============================================================

print()

print(
    "=" * 72
)

print(
    "需求一每日本地更新完成"
)

print(
    "=" * 72
)


print(
    "当前可计算 Alpha：",
    len(
        supported_tokens
    )
)


print(
    "具备完整30天历史：",
    complete_history_count
)


print(
    "历史不足30天：",
    len(
        insufficient_history
    )
)


if insufficient_history:

    print(
        "历史不足示例：",
        ", ".join(
            (
                f"{symbol}"
                f"({days}天)"
            )

            for symbol, days
            in insufficient_history[:10]
        )
    )


print(
    "埋伏池：",
    len(
        ambush_pool
    ),
    "个"
)


print(
    "✅",
    OUTPUT_FILE
)


print()

print(
    "Bitquery资金查询：0次"
)

print(
    "Points消耗：0"
)

