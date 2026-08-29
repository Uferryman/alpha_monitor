# ============================================================
# Binance Alpha 需求二
# 新钱包“启动异常”判定层
#
# 重要：
#
# 每15分钟只是检查一次。
#
# 真正用于判断异常的指标始终是：
#
#     今天截至当前时点
#     累计15D新钱包数量
#
#
# 例如：
#
# 13:45 今日累计 = 144
# 14:00 今日累计 = 264
# 14:15 今日累计 = 395
# 14:30 今日累计 = 508
#
# 14:30执行异常判断时：
#
#     当前值 = 508
#
# 而不是：
#
#     当前15分钟新增 = 113
#
#
# ------------------------------------------------------------
# 基线分两个阶段
# ------------------------------------------------------------
#
# 第一阶段：冷启动
#
# 如果还没有完整历史日：
#
# 当前今日累计
#
# VS
#
# 今天前面已经保存的累计检查点
#
#
# 第二阶段：正式历史同期
#
# 一旦有完整历史日：
#
# 今天14:30累计
#
# VS
#
# 昨天14:30累计
# 前天14:30累计
# ...
#
# 最多过去15天。
#
#
# ------------------------------------------------------------
# 钱包启动异常
# ------------------------------------------------------------
#
# 今日累计新钱包 > 40
#
# 且
#
# 今日累计新钱包 > 基线均值 × 2
#
# 如果历史样本 >= 2：
#
# 再要求：
#
# Z-score > 2
#
#
# ------------------------------------------------------------
# 资金确认
# ------------------------------------------------------------
#
# 钱包启动异常之后：
#
# 今日累计净买入 > 1000美元
#
# 且
#
# 5D累计净买入 > 0
#
#
# ------------------------------------------------------------
# 信号等级
# ------------------------------------------------------------
#
# 5D净买入：
#
# 0 ～ 1万美元
# → 观察
#
# 1万 ～ 10万美元
# → 跟进
#
# >= 10万美元
# → 重点
#
#
# ------------------------------------------------------------
# 需求一共振
# ------------------------------------------------------------
#
# 如果同时进入：
#
# alpha_ambush_pool.csv
#
# 则：
#
# 🔥 双信号共振
#
#
# 本程序：
#
# Bitquery请求 = 0
# ============================================================


# ============================================================
# 1. 导入模块
# ============================================================

import os
import csv
import sqlite3
import statistics
import requests

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from dotenv import load_dotenv


# ============================================================
# 2. 加载环境变量
# ============================================================

load_dotenv()


# Telegram暂时可以没有配置
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID"
)


# ============================================================
# 3. 基础配置
# ============================================================

DATABASE_FILE = (
    "alpha_monitor.db"
)

AMBUSH_FILE = (
    "alpha_ambush_pool.csv"
)

# 正式历史基线最多使用15天
BASELINE_DAYS = 15


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
# 5. 时间解析
# ============================================================

def parse_time(value):

    return datetime.fromisoformat(

        value.replace(
            "Z",
            "+00:00",
        )
    )


# ============================================================
# 6. 时间格式化
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
# 7. 地址标准化
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
# 8. 打开SQLite
# ============================================================

connection = sqlite3.connect(
    DATABASE_FILE
)

connection.execute(
    "PRAGMA busy_timeout = 30000"
)

cursor = connection.cursor()


# ============================================================
# 9. 获取15分钟监控最新断点
# ============================================================

cursor.execute(
    """
    SELECT meta_value

    FROM new_wallet_monitor_meta

    WHERE meta_key = 'last_success_end'
    """
)


row = cursor.fetchone()


if not row:

    print(
        "❌ 尚未找到15分钟监控数据"
    )

    connection.close()

    raise SystemExit


# 最新处理结束时间
latest_end = (
    row[0]
)


latest_end_dt = (
    parse_time(
        latest_end
    )
)


# 当前最新15分钟开始时间
latest_start_dt = (

    latest_end_dt

    -

    timedelta(
        minutes=15
    )
)


latest_start = (
    format_time(
        latest_start_dt
    )
)


# ============================================================
# 10. 当前UTC日期
# ============================================================

current_date = (
    latest_start_dt.date()
)


current_date_text = (
    current_date.isoformat()
)


# UTC当天00:00
current_day_start_dt = datetime(

    current_date.year,

    current_date.month,

    current_date.day,

    tzinfo=timezone.utc,
)


current_day_start = (
    format_time(
        current_day_start_dt
    )
)


# ============================================================
# 11. Token注册表
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


tokens = {}


# 用于：
#
# chain + contract
#
# 找token_key
identity_to_key = {}


for (
    token_key,
    token_id,
    symbol,
    chain,
    address
) in cursor.fetchall():


    address = normalize_address(
        chain,
        address,
    )


    tokens[
        int(
            token_key
        )
    ] = {

        "token_key":
            int(
                token_key
            ),

        "token_id":
            token_id,

        "symbol":
            symbol,

        "chain":
            chain,

        "address":
            address,
    }


    identity_to_key[
        (
            chain,
            address,
        )
    ] = int(
        token_key
    )


# ============================================================
# 12. 读取需求一埋伏池
#
# 本地CSV
#
# Bitquery Points = 0
# ============================================================

ambush_tokens = set()


if os.path.exists(
    AMBUSH_FILE
):

    with open(
        AMBUSH_FILE,
        "r",
        encoding="utf-8-sig",
    ) as file:


        reader = csv.DictReader(
            file
        )


        for item in reader:


            chain = str(
                item.get(
                    "chain",
                    "",
                )
            ).strip()


            address = normalize_address(

                chain,

                item.get(
                    "contract_address",
                    "",
                ),
            )


            if (
                chain
                and
                address
            ):

                ambush_tokens.add(
                    (
                        chain,
                        address,
                    )
                )


# ============================================================
# 13. 最终信号表
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_signal_15m (

        token_key INTEGER NOT NULL,

        interval_end TEXT NOT NULL,

        baseline_mode TEXT NOT NULL,

        baseline_samples INTEGER NOT NULL,

        metric_value INTEGER NOT NULL,

        baseline_mean REAL NOT NULL,

        baseline_std REAL NOT NULL,

        multiple REAL NOT NULL,

        z_score REAL NOT NULL,

        interval_new_wallets INTEGER NOT NULL,

        interval_buy_wallets INTEGER NOT NULL,

        interval_new_wallet_ratio REAL NOT NULL,

        today_new_wallets INTEGER NOT NULL,

        today_net_buy REAL NOT NULL,

        net_buy_5d REAL NOT NULL,

        wallet_anomaly INTEGER NOT NULL,

        fund_confirmed INTEGER NOT NULL,

        final_signal INTEGER NOT NULL,

        level TEXT,

        ambush_flag INTEGER NOT NULL,

        resonance_flag INTEGER NOT NULL,

        overflow INTEGER NOT NULL,

        updated_at TEXT NOT NULL,

        PRIMARY KEY (
            token_key,
            interval_end
        )

    ) WITHOUT ROWID
    """
)


# ============================================================
# 14. Telegram去重状态
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS wallet_alert_state (

        token_key INTEGER PRIMARY KEY,

        active INTEGER NOT NULL DEFAULT 0,

        last_alerted_level TEXT,

        last_alerted_at TEXT,

        updated_at TEXT NOT NULL

    )
    """
)


# ============================================================
# 15. 新逻辑版本
#
# 之前跑过的错误“单15分钟异常逻辑”
# 不再保留。
#
# 第一次运行新版时清空旧信号结果。
# 不影响：
#
# - 钱包状态
# - 15分钟数据
# - 今日累计
# - 需求一
# ============================================================

cursor.execute(
    """
    SELECT meta_value

    FROM new_wallet_monitor_meta

    WHERE meta_key = 'wallet_signal_logic_version'
    """
)


version_row = (
    cursor.fetchone()
)


current_version = (

    version_row[0]

    if version_row

    else None
)


if current_version != "cumulative_v2":


    cursor.execute(
        """
        DELETE FROM wallet_signal_15m
        """
    )


    cursor.execute(
        """
        DELETE FROM wallet_alert_state
        """
    )


    cursor.execute(
        """
        INSERT INTO new_wallet_monitor_meta (

            meta_key,

            meta_value

        )

        VALUES (
            'wallet_signal_logic_version',
            'cumulative_v2'
        )

        ON CONFLICT(meta_key)

        DO UPDATE SET

            meta_value =
                excluded.meta_value
        """
    )


    connection.commit()


# ============================================================
# 16. 最新15分钟是否overflow
# ============================================================

cursor.execute(
    """
    SELECT overflow

    FROM new_wallet_monitor_runs

    WHERE interval_start = ?
    """,
    (
        latest_start,
    ),
)


overflow_row = (
    cursor.fetchone()
)


global_overflow = int(

    overflow_row[0]

    if overflow_row

    else 0
)


# ============================================================
# 17. 读取最新15分钟数据
# ============================================================

cursor.execute(
    """
    SELECT

        token_key,

        new_wallet_count,

        buy_wallet_count,

        market_net_buy,

        is_complete

    FROM new_wallet_15m

    WHERE interval_start = ?
    """,
    (
        latest_start,
    ),
)


latest_rows = (
    cursor.fetchall()
)


# ============================================================
# 18. 今天累计数据
#
# 这里才是异常主指标。
# ============================================================

cursor.execute(
    """
    SELECT

        token_key,

        new_wallet_count,

        market_net_buy,

        has_overflow

    FROM daily_new_wallets

    WHERE date = ?
    """,
    (
        current_date_text,
    ),
)


today_data = {}


for (
    token_key,
    new_wallet_count,
    market_net_buy,
    has_overflow
) in cursor.fetchall():


    today_data[
        int(
            token_key
        )
    ] = {

        "new_wallets":
            int(
                new_wallet_count
                or 0
            ),

        "net_buy":
            float(
                market_net_buy
                or 0
            ),

        "overflow":
            int(
                has_overflow
                or 0
            ),
    }


# ============================================================
# 19. 获取前4个完整UTC日净买入
#
# 当前日截至现在
#
# +
#
# 前4个完整UTC自然日
#
# =
#
# 5D资金流
#
# 全部来自需求一的本地daily_fund_flow。
# ============================================================

previous_4d_net = {

    token_key: 0.0

    for token_key in tokens
}


four_day_start = (

    current_date

    -

    timedelta(
        days=4
    )
)


cursor.execute(
    """
    SELECT

        chain,

        contract_address,

        netflow_usd

    FROM daily_fund_flow

    WHERE
        date >= ?
        AND date < ?
    """,
    (
        four_day_start.isoformat(),

        current_date_text,
    ),
)


for (
    chain,
    address,
    netflow
) in cursor.fetchall():


    address = normalize_address(
        chain,
        address,
    )


    token_key = (
        identity_to_key.get(
            (
                chain,
                address,
            )
        )
    )


    if token_key in previous_4d_net:

        previous_4d_net[
            token_key
        ] += float(
            netflow
            or 0
        )


# ============================================================
# 20. 正式历史同期样本
#
# 假设现在是UTC 14:30。
#
# 正式基线是：
#
# 昨天截至14:30累计新钱包
# 前天截至14:30累计新钱包
# ...
#
# 最多15天。
#
#
# 但是：
#
# 一个历史日只有从00:00到14:30
# 所有15分钟都完整存在，
# 才能进入正式基线。
# ============================================================

def get_formal_samples(
    token_key,
):


    # 当前UTC日已经走过多少分钟
    minutes_elapsed = (

        latest_end_dt.hour
        * 60

        +

        latest_end_dt.minute
    )


    # 应有多少个15分钟桶
    expected_buckets = (

        minutes_elapsed
        // 15
    )


    # 00:00时没有样本
    if expected_buckets <= 0:

        return []


    samples = []


    # 最多过去15天
    for days_ago in range(
        1,
        BASELINE_DAYS + 1,
    ):


        historical_date = (

            current_date

            -

            timedelta(
                days=days_ago
            )
        )


        historical_start_dt = datetime(

            historical_date.year,

            historical_date.month,

            historical_date.day,

            tzinfo=timezone.utc,
        )


        historical_cutoff_dt = (

            historical_start_dt

            +

            timedelta(
                minutes=minutes_elapsed
            )
        )


        historical_start = (
            format_time(
                historical_start_dt
            )
        )


        historical_cutoff = (
            format_time(
                historical_cutoff_dt
            )
        )


        # ====================================================
        # 检查这个历史日的15分钟运行是否完整
        # ====================================================

        cursor.execute(
            """
            SELECT

                COUNT(*),

                COALESCE(
                    MAX(
                        overflow
                    ),
                    0
                )

            FROM new_wallet_monitor_runs

            WHERE
                interval_start >= ?
                AND interval_start < ?
            """,
            (
                historical_start,

                historical_cutoff,
            ),
        )


        (
            run_count,
            has_overflow
        ) = cursor.fetchone()


        # 必须刚好覆盖全部15分钟
        if int(
            run_count
            or 0
        ) != expected_buckets:

            continue


        # 历史日出现overflow则跳过
        if int(
            has_overflow
            or 0
        ) == 1:

            continue


        # ====================================================
        # 统计该Token截至同一时点累计新钱包
        # ====================================================

        cursor.execute(
            """
            SELECT

                COALESCE(
                    SUM(
                        new_wallet_count
                    ),
                    0
                )

            FROM new_wallet_15m

            WHERE
                token_key = ?
                AND interval_start >= ?
                AND interval_start < ?
                AND is_complete = 1
            """,
            (
                token_key,

                historical_start,

                historical_cutoff,
            ),
        )


        value = (
            cursor.fetchone()[0]
        )


        samples.append(
            int(
                value
                or 0
            )
        )


    return samples


# ============================================================
# 21. 冷启动累计基线
#
# 这是现在没有完整历史日时使用的。
#
#
# 例如今天已经保存：
#
# 13:45累计 144
# 14:00累计 264
# 14:15累计 395
#
# 当前14:30：
#
# 当前值 508
#
# 临时基线：
#
# [144, 264, 395]
#
#
# 注意：
#
# 仍然是在比较“累计新钱包”。
#
# 不再比较：
#
# 单个15分钟新增。
# ============================================================

def get_cold_start_samples(
    token_key,
):


    # 取今天当前检查点之前的15分钟记录
    cursor.execute(
        """
        SELECT

            interval_start,

            new_wallet_count

        FROM new_wallet_15m

        WHERE
            token_key = ?
            AND interval_start >= ?
            AND interval_end < ?
            AND is_complete = 1

        ORDER BY interval_start ASC
        """,
        (
            token_key,

            current_day_start,

            latest_end,
        ),
    )


    rows = (
        cursor.fetchall()
    )


    samples = []


    cumulative = 0


    # 按时间顺序累计
    for (
        _,
        interval_new_wallets
    ) in rows:


        cumulative += int(

            interval_new_wallets

            or 0
        )


        samples.append(
            cumulative
        )


    return samples


# ============================================================
# 22. 等级
# ============================================================

def calc_level(
    net_buy_5d,
):


    if net_buy_5d >= 100000:

        return "重点"


    if net_buy_5d >= 10000:

        return "跟进"


    if net_buy_5d > 0:

        return "观察"


    return None


# ============================================================
# 23. Telegram发送
#
# 没配置时不影响本地运行。
# ============================================================

def send_telegram(
    text,
):


    if (
        not TELEGRAM_BOT_TOKEN

        or

        not TELEGRAM_CHAT_ID
    ):

        return False


    url = (

        "https://api.telegram.org/bot"

        +

        TELEGRAM_BOT_TOKEN

        +

        "/sendMessage"
    )


    try:


        response = requests.post(

            url,

            data={

                "chat_id":
                    TELEGRAM_CHAT_ID,

                "text":
                    text,
            },

            timeout=20,
        )


        response.raise_for_status()


        result = (
            response.json()
        )


        return bool(
            result.get(
                "ok"
            )
        )


    except Exception as error:


        print(
            f"⚠️ Telegram发送失败："
            f"{error}"
        )


        return False


# ============================================================
# 24. 开始计算
# ============================================================

results = []

pending_alerts = []


updated_at = (
    datetime.now()
    .strftime(
        "%Y-%m-%d %H:%M:%S"
    )
)


print("=" * 80)

print(
    "需求二：新钱包启动异常判定"
)

print("=" * 80)

print(
    f"检查点："
    f"{latest_end}"
)

print(
    f"最新15分钟："
    f"{latest_start}"
    f" → "
    f"{latest_end}"
)

print(
    f"Token数量："
    f"{len(latest_rows)}"
)

print(
    "异常主指标：今日截至当前累计15D新钱包"
)

print(
    "Bitquery额外请求：0"
)


if global_overflow:

    print(
        "\n🔥 最新15分钟达到25,000总行上限"
    )

    print(
        "按约定不追加查询。"
    )


# ============================================================
# 25. 遍历所有Token
# ============================================================

for (
    token_key,
    interval_new,
    interval_buy,
    interval_net,
    is_complete
) in latest_rows:


    token_key = int(
        token_key
    )


    if token_key not in tokens:

        continue


    token = (
        tokens[
            token_key
        ]
    )


    interval_new = int(
        interval_new
        or 0
    )


    interval_buy = int(
        interval_buy
        or 0
    )


    # ========================================================
    # 当前15分钟新钱包占比
    #
    # 这是辅助展示指标，
    # 不是主异常指标。
    # ========================================================

    if interval_buy > 0:

        interval_ratio = (

            interval_new

            /

            interval_buy
        )

    else:

        interval_ratio = 0.0


    # ========================================================
    # 今日累计
    #
    # 这才是主指标。
    # ========================================================

    today = (
        today_data.get(
            token_key,
            {}
        )
    )


    today_new = int(

        today.get(
            "new_wallets",
            0,
        )
    )


    today_net = float(

        today.get(
            "net_buy",
            0.0,
        )
    )


    today_overflow = int(

        today.get(
            "overflow",
            0,
        )
    )


    # ========================================================
    # 5D累计净买入
    # ========================================================

    net_buy_5d = (

        previous_4d_net.get(
            token_key,
            0.0,
        )

        +

        today_net
    )


    # ========================================================
    # 先找正式历史同期基线
    # ========================================================

    formal_samples = (
        get_formal_samples(
            token_key
        )
    )


    # ========================================================
    # 有正式历史日
    # ========================================================

    if formal_samples:


        baseline_mode = (
            "历史同期累计"
        )


        samples = (
            formal_samples
        )


    # ========================================================
    # 还没有正式历史日
    #
    # 使用今天前面的累计检查点
    # ========================================================

    else:


        baseline_mode = (
            "冷启动累计"
        )


        samples = (
            get_cold_start_samples(
                token_key
            )
        )


    # ========================================================
    # 无论什么模式：
    #
    # 当前指标永远是：
    #
    # 今日累计新钱包
    # ========================================================

    metric_value = (
        today_new
    )


    # ========================================================
    # 样本数量
    # ========================================================

    sample_count = (
        len(
            samples
        )
    )


    # ========================================================
    # 均值
    # ========================================================

    if sample_count > 0:

        baseline_mean = (
            statistics.mean(
                samples
            )
        )

    else:

        baseline_mean = 0.0


    # ========================================================
    # 标准差
    # ========================================================

    if sample_count >= 2:

        baseline_std = (
            statistics.stdev(
                samples
            )
        )

    else:

        baseline_std = 0.0


    # ========================================================
    # 倍数
    # ========================================================

    if baseline_mean > 0:

        multiple = (

            metric_value

            /

            baseline_mean
        )


    elif metric_value > 0:

        multiple = 999.0


    else:

        multiple = 0.0


    # ========================================================
    # Z-score
    # ========================================================

    if baseline_std > 0:

        z_score = (

            (
                metric_value

                -

                baseline_mean
            )

            /

            baseline_std
        )


    # 历史完全一样
    # 当前突然更高
    elif (
        sample_count >= 2

        and

        metric_value
        >
        baseline_mean
    ):

        z_score = 999.0


    else:

        z_score = 0.0


    # ========================================================
    # 钱包启动异常
    #
    # 基础条件：
    #
    # 今日累计 > 40
    #
    # 且
    #
    # 今日累计 > 基线均值×2
    #
    #
    # 只有1个历史样本：
    #
    # 暂时不要求Z
    #
    #
    # >=2个样本：
    #
    # 再要求 Z > 2
    # ========================================================

    wallet_anomaly = 0


    if sample_count >= 1:


        basic_condition = (

            metric_value
            >
            40

            and

            metric_value
            >
            baseline_mean
            * 2
        )


        # --------------------------------------------
        # 只有1个样本
        # --------------------------------------------

        if sample_count == 1:


            wallet_anomaly = int(
                basic_condition
            )


        # --------------------------------------------
        # 两个及以上样本
        # --------------------------------------------

        else:


            wallet_anomaly = int(

                basic_condition

                and

                z_score
                >
                2
            )


    # ========================================================
    # 资金确认
    #
    # 注意：
    #
    # 钱包异常是第一信号。
    #
    # 资金只负责确认。
    # ========================================================

    fund_confirmed = int(

        today_net
        >
        1000

        and

        net_buy_5d
        >
        0
    )


    # ========================================================
    # 最终入场候选信号
    # ========================================================

    final_signal = int(

        wallet_anomaly
        ==
        1

        and

        fund_confirmed
        ==
        1

        and

        global_overflow
        ==
        0

        and

        today_overflow
        ==
        0

        and

        int(
            is_complete
        )
        ==
        1
    )


    # ========================================================
    # 信号等级
    # ========================================================

    if final_signal:

        level = (
            calc_level(
                net_buy_5d
            )
        )

    else:

        level = None


    # ========================================================
    # 需求一埋伏池
    # ========================================================

    ambush_flag = int(

        (
            token[
                "chain"
            ],

            token[
                "address"
            ]
        )

        in

        ambush_tokens
    )


    # ========================================================
    # 双信号共振
    # ========================================================

    resonance_flag = int(

        final_signal
        ==
        1

        and

        ambush_flag
        ==
        1
    )


    # ========================================================
    # 保存信号结果
    # ========================================================

    cursor.execute(
        """
        INSERT OR REPLACE INTO
        wallet_signal_15m (

            token_key,

            interval_end,

            baseline_mode,

            baseline_samples,

            metric_value,

            baseline_mean,

            baseline_std,

            multiple,

            z_score,

            interval_new_wallets,

            interval_buy_wallets,

            interval_new_wallet_ratio,

            today_new_wallets,

            today_net_buy,

            net_buy_5d,

            wallet_anomaly,

            fund_confirmed,

            final_signal,

            level,

            ambush_flag,

            resonance_flag,

            overflow,

            updated_at

        )

        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            token_key,

            latest_end,

            baseline_mode,

            sample_count,

            metric_value,

            baseline_mean,

            baseline_std,

            multiple,

            z_score,

            interval_new,

            interval_buy,

            interval_ratio,

            today_new,

            today_net,

            net_buy_5d,

            wallet_anomaly,

            fund_confirmed,

            final_signal,

            level,

            ambush_flag,

            resonance_flag,

            global_overflow,

            updated_at,
        ),
    )


    # ========================================================
    # Telegram历史状态
    # ========================================================

    cursor.execute(
        """
        SELECT

            active,

            last_alerted_level

        FROM wallet_alert_state

        WHERE token_key = ?
        """,
        (
            token_key,
        ),
    )


    alert_row = (
        cursor.fetchone()
    )


    if alert_row:


        previous_active = int(
            alert_row[0]
            or 0
        )


        previous_level = (
            alert_row[1]
        )


    else:


        previous_active = 0

        previous_level = None


    # ========================================================
    # 什么时候需要提醒？
    #
    # 1. 新出现入场信号
    #
    # 或
    #
    # 2. 信号等级变化
    # ========================================================

    should_alert = (

        final_signal
        ==
        1

        and

        (
            previous_active
            ==
            0

            or

            previous_level
            !=
            level
        )
    )


    # ========================================================
    # 更新当前active状态
    # ========================================================

    cursor.execute(
        """
        INSERT INTO wallet_alert_state (

            token_key,

            active,

            last_alerted_level,

            last_alerted_at,

            updated_at

        )

        VALUES (
            ?, ?, ?, ?, ?
        )

        ON CONFLICT(token_key)

        DO UPDATE SET

            active =
                excluded.active,

            updated_at =
                excluded.updated_at
        """,
        (
            token_key,

            final_signal,

            previous_level,

            None,

            updated_at,
        ),
    )


    # ========================================================
    # 构建Telegram消息
    # ========================================================

    if should_alert:


        resonance_text = (

            "\n🔥 双信号共振：已进入需求一埋伏池"

            if resonance_flag

            else ""
        )


        telegram_text = (

            "🚨 Binance Alpha 钱包启动信号\n\n"

            f"Token：{token['symbol']}\n"

            f"链：{token['chain']}\n"

            f"等级：{level}\n\n"

            f"基线模式：{baseline_mode}\n"

            f"历史样本：{sample_count}\n\n"

            f"今日累计新钱包：{today_new:,}\n"

            f"历史基线均值：{baseline_mean:.1f}\n"

            f"异常倍数：{multiple:.2f}x\n"

            f"Z-score：{z_score:.2f}\n\n"

            f"本15分钟新增新钱包：{interval_new:,}\n"

            f"本15分钟买入钱包：{interval_buy:,}\n"

            f"新钱包占比：{interval_ratio:.1%}\n\n"

            f"今日净买入：${today_net:+,.2f}\n"

            f"5D净买入：${net_buy_5d:+,.2f}"

            f"{resonance_text}"
        )


        pending_alerts.append(
            {

                "token_key":
                    token_key,

                "symbol":
                    token[
                        "symbol"
                    ],

                "level":
                    level,

                "text":
                    telegram_text,
            }
        )


    # ========================================================
    # 保存终端结果
    # ========================================================

    results.append(
        {

            "symbol":
                token[
                    "symbol"
                ],

            "chain":
                token[
                    "chain"
                ],

            "mode":
                baseline_mode,

            "samples":
                sample_count,

            "today_new":
                today_new,

            "interval_new":
                interval_new,

            "interval_buy":
                interval_buy,

            "ratio":
                interval_ratio,

            "mean":
                baseline_mean,

            "std":
                baseline_std,

            "multiple":
                multiple,

            "z":
                z_score,

            "today_net":
                today_net,

            "net5d":
                net_buy_5d,

            "wallet_anomaly":
                wallet_anomaly,

            "fund":
                fund_confirmed,

            "signal":
                final_signal,

            "level":
                level,

            "ambush":
                ambush_flag,

            "resonance":
                resonance_flag,
        }
    )


# ============================================================
# 26. 保存全部计算
# ============================================================

connection.commit()


# ============================================================
# 27. 输出真正的钱包启动异常
# ============================================================

wallet_anomalies = [

    item

    for item in results

    if item[
        "wallet_anomaly"
    ]
    ==
    1
]


wallet_anomalies.sort(

    key=lambda item: (

        item[
            "multiple"
        ],

        item[
            "today_new"
        ],
    ),

    reverse=True,
)


print(
    "\n"
    + "=" * 80
)

print(
    "钱包启动异常"
)

print("=" * 80)


if not wallet_anomalies:


    print(
        "当前检查点没有检测到钱包启动异常。"
    )


else:


    print(
        f"{'Token':<16}"
        f"{'模式':<14}"
        f"{'样本':>6}"
        f"{'今日累计':>10}"
        f"{'基线均值':>11}"
        f"{'倍数':>9}"
        f"{'Z':>8}"
        f"{'今日净买入':>16}"
        f"{'资金':>8}"
    )


    print(
        "-" * 105
    )


    for item in wallet_anomalies:


        fund_text = (

            "✅"

            if item[
                "fund"
            ]

            else "❌"
        )


        print(
            f"{item['symbol']:<16}"
            f"{item['mode']:<14}"
            f"{item['samples']:>6}"
            f"{item['today_new']:>10}"
            f"{item['mean']:>11.1f}"
            f"{item['multiple']:>9.2f}"
            f"{item['z']:>8.2f}"
            f"${item['today_net']:>+15,.2f}"
            f"{fund_text:>8}"
        )


# ============================================================
# 28. 即使没有异常，也输出一个“诊断TOP10”
#
# 这个不是信号！
#
# 不会Telegram提醒。
#
# 目的只是：
#
# 如果最后又显示0，
# 我们可以看到最接近启动条件的Token，
# 不需要猜程序是不是坏了。
#
#
# 只展示：
#
# 今日累计 > 40
#
# 的Token。
# ============================================================

diagnostics = [

    item

    for item in results

    if item[
        "today_new"
    ]
    >
    40
]


diagnostics.sort(

    key=lambda item: (

        item[
            "multiple"
        ],

        item[
            "z"
        ],

        item[
            "today_new"
        ],
    ),

    reverse=True,
)


print(
    "\n"
    + "=" * 80
)

print(
    "启动条件诊断 TOP10（仅用于验证，不是信号）"
)

print("=" * 80)


if not diagnostics:


    print(
        "当前没有今日累计新钱包超过40的Token。"
    )


else:


    print(
        f"{'Token':<16}"
        f"{'模式':<14}"
        f"{'样本':>6}"
        f"{'今日累计':>10}"
        f"{'基线':>10}"
        f"{'倍数':>9}"
        f"{'Z':>8}"
        f"{'今日净买入':>16}"
    )


    print(
        "-" * 95
    )


    for item in diagnostics[
        :10
    ]:


        print(
            f"{item['symbol']:<16}"
            f"{item['mode']:<14}"
            f"{item['samples']:>6}"
            f"{item['today_new']:>10}"
            f"{item['mean']:>10.1f}"
            f"{item['multiple']:>9.2f}"
            f"{item['z']:>8.2f}"
            f"${item['today_net']:>+15,.2f}"
        )


# ============================================================
# 29. 最终资金确认信号
# ============================================================

signals = [

    item

    for item in results

    if item[
        "signal"
    ]
    ==
    1
]


signals.sort(

    key=lambda item: (

        item[
            "resonance"
        ],

        item[
            "net5d"
        ],

        item[
            "today_new"
        ],
    ),

    reverse=True,
)


print(
    "\n"
    + "=" * 80
)

print(
    "最终入场候选信号"
)

print("=" * 80)


if not signals:


    print(
        "当前没有同时通过"
        "「钱包启动异常 + 资金确认」的Token。"
    )


else:


    print(
        f"{'Token':<16}"
        f"{'等级':<8}"
        f"{'今日新钱包':>12}"
        f"{'倍数':>9}"
        f"{'Z':>8}"
        f"{'今日净买入':>16}"
        f"{'5D净买入':>16}"
        f"{'共振':>8}"
    )


    print(
        "-" * 105
    )


    for item in signals:


        resonance_text = (

            "🔥"

            if item[
                "resonance"
            ]

            else "-"
        )


        print(
            f"{item['symbol']:<16}"
            f"{str(item['level']):<8}"
            f"{item['today_new']:>12}"
            f"{item['multiple']:>9.2f}"
            f"{item['z']:>8.2f}"
            f"${item['today_net']:>+15,.2f}"
            f"${item['net5d']:>+15,.2f}"
            f"{resonance_text:>8}"
        )


# ============================================================
# 30. Telegram
# ============================================================

if (
    TELEGRAM_BOT_TOKEN
    and
    TELEGRAM_CHAT_ID
):


    if pending_alerts:


        print(
            f"\n准备发送Telegram："
            f"{len(pending_alerts)} 条"
        )


        for alert in pending_alerts:


            success = send_telegram(
                alert[
                    "text"
                ]
            )


            # 发送成功才记录提醒等级
            if success:


                sent_at = (
                    datetime.now()
                    .strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                )


                cursor.execute(
                    """
                    UPDATE wallet_alert_state

                    SET

                        last_alerted_level = ?,

                        last_alerted_at = ?,

                        updated_at = ?

                    WHERE token_key = ?
                    """,
                    (
                        alert[
                            "level"
                        ],

                        sent_at,

                        sent_at,

                        alert[
                            "token_key"
                        ],
                    ),
                )


                connection.commit()


                print(
                    f"✅ Telegram已发送："
                    f"{alert['symbol']} "
                    f"{alert['level']}"
                )


            else:


                print(
                    f"⚠️ Telegram发送失败："
                    f"{alert['symbol']}"
                )


    else:


        print(
            "\nTelegram："
            "当前没有新的信号或等级变化需要提醒。"
        )


else:


    print(
        "\nTelegram：尚未配置。"
    )


# ============================================================
# 31. 最终结果
# ============================================================

print(
    "\n"
    + "=" * 80
)

print(
    "✅ 需求二启动异常判定完成"
)

print("=" * 80)

print(
    f"钱包启动异常："
    f"{len(wallet_anomalies)} 个"
)

print(
    f"资金确认后信号："
    f"{len(signals)} 个"
)

print(
    f"待提醒："
    f"{len(pending_alerts)} 个"
)

print(
    "Bitquery额外请求：0"
)


# ============================================================
# 32. 关闭数据库
# ============================================================

connection.close()
