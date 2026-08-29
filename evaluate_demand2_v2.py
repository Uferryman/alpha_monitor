# -*- coding: utf-8 -*-

# ============================================================
# 需求二：完整信号判断 V2
#
# 只做已经确认的内容：
#
# 1. 先检查实时采集是否追到最新完整15分钟
# 2. 计算今天累计首次买入钱包
# 3. 与前15天“同一时刻累计值”比较
# 4. 判断钱包异常
# 5. 计算今天累计净流入
# 6. 计算5日累计净流入
# 7. 判断资金确认
# 8. 输出最终需求二信号
# 9. 判断是否与需求一形成双信号共振
#
# 不包含：
# - cold start / 冷启动
# - Top榜
# - 人工观察榜
# - 额外自定义指标
#
# 全部使用本地数据。
# Bitquery请求：0
# Points消耗：0
# ============================================================


import csv
import os
import sqlite3
import statistics

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from datetime import timezone


# ============================================================
# 1. 配置
# ============================================================

DATABASE_FILE = "alpha_monitor.db"

AMBUSH_POOL_FILE = "alpha_ambush_pool.csv"

HISTORY_DAYS = 15

MIN_WALLET_COUNT = 40

MIN_WALLET_RATIO = 2.0

MIN_Z_SCORE = 2.0

MIN_TODAY_NETFLOW = 1000.0


# ============================================================
# 2. UTC时间工具
# ============================================================

def utc_now():

    return datetime.now(
        timezone.utc
    )


def floor_to_15m(dt):

    dt = dt.astimezone(
        timezone.utc
    ).replace(
        second=0,
        microsecond=0,
    )

    minute = (
        dt.minute // 15
    ) * 15

    return dt.replace(
        minute=minute
    )


def parse_iso(value):

    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00"
        )
    )


# ============================================================
# 3. 合约地址标准化
# ============================================================

def normalize_contract(
    chain,
    contract_address,
):

    chain = str(
        chain or ""
    ).strip().lower()

    contract_address = str(
        contract_address or ""
    ).strip()

    if chain in {
        "bsc",
        "base",
        "eth",
        "ethereum",
        "arbitrum",
        "arb",
    }:

        return contract_address.lower()

    return contract_address


# ============================================================
# 4. 打开数据库
# ============================================================

conn = sqlite3.connect(
    DATABASE_FILE
)

conn.execute(
    "PRAGMA busy_timeout = 30000"
)

cur = conn.cursor()


# ============================================================
# 5. 读取需求二checkpoint
# ============================================================

cur.execute("""
SELECT meta_value
FROM demand2_v2_meta
WHERE meta_key = 'last_success_end'
""")

row = cur.fetchone()


if not row:

    raise RuntimeError(
        "没有找到需求二实时checkpoint"
    )


checkpoint = row[0]

checkpoint_dt = parse_iso(
    checkpoint
)


# ============================================================
# 6. 核实有没有断跑
#
# 信号程序自己不负责补数据。
#
# 补数据由：
# demand2_realtime_v2.py --run
#
# 这里仅负责检查：
#
# checkpoint是否已经等于
# 当前最新完整15分钟。
# ============================================================

latest_complete = floor_to_15m(
    utc_now()
)


if checkpoint_dt != latest_complete:

    gap_minutes = int(
        (
            latest_complete
            -
            checkpoint_dt
        ).total_seconds()
        // 60
    )

    print(
        "=" * 88
    )

    print(
        "❌ 需求二数据没有追到最新，停止信号判断"
    )

    print(
        "=" * 88
    )

    print(
        "当前checkpoint：",
        checkpoint
    )

    print(
        "最新完整15分钟：",
        latest_complete.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )

    print(
        "落后：",
        gap_minutes,
        "分钟"
    )

    print()

    print(
        "请先执行："
    )

    print(
        "python -u demand2_realtime_v2.py --run"
    )

    print()

    print(
        "Bitquery请求：0"
    )

    print(
        "Points消耗：0"
    )

    conn.close()

    raise SystemExit


# ============================================================
# 7. 当前统计日期和时刻
# ============================================================

today = checkpoint_dt.date()

today_text = today.isoformat()

checkpoint_time = checkpoint_dt.strftime(
    "%H:%M:%S"
)

today_start = (
    f"{today_text}T00:00:00Z"
)


# ============================================================
# 8. 读取first_buy历史起点
# ============================================================

cur.execute("""
SELECT meta_value
FROM wallet_first_buy_v2_meta
WHERE meta_key = 'window_start'
""")

row = cur.fetchone()


if not row:

    raise RuntimeError(
        "没有找到first_buy历史开始时间"
    )


tracking_start_date = parse_iso(
    row[0]
).date()


# ============================================================
# 9. 读取当前305个Alpha Token
# ============================================================

cur.execute("""
SELECT
    token_key,
    symbol,
    chain,
    contract_address

FROM alpha_token_registry

ORDER BY token_key
""")


token_rows = cur.fetchall()


if len(token_rows) != 305:

    raise RuntimeError(
        f"当前Alpha Token数量={len(token_rows)}，"
        "预期305，为安全起见停止。"
    )


tokens = {}


for (
    token_key,
    symbol,
    chain,
    contract_address
) in token_rows:

    tokens[
        int(token_key)
    ] = {
        "symbol":
            symbol,

        "chain":
            chain,

        "contract_address":
            contract_address,
    }


# ============================================================
# 10. 今天截至checkpoint累计首次钱包
# ============================================================

cur.execute("""
SELECT
    token_key,
    COUNT(*)

FROM wallet_token_first_buy_v2

WHERE
    first_buy_time >= ?
    AND first_buy_time < ?

GROUP BY token_key
""", (
    today_start,
    checkpoint,
))


today_wallet_counts = {
    int(token_key):
        int(wallet_count)

    for token_key, wallet_count
    in cur.fetchall()
}


# ============================================================
# 11. 构造最多15个真实历史日期
#
# 只使用first_buy底库真正覆盖的历史。
#
# 不使用cold start。
# ============================================================

history_dates = []


for days_ago in range(
    1,
    HISTORY_DAYS + 1
):

    date_value = (
        today
        -
        timedelta(
            days=days_ago
        )
    )

    if date_value < tracking_start_date:

        continue

    history_dates.append(
        date_value.isoformat()
    )


history_sample_count = len(
    history_dates
)


if history_sample_count == 0:

    print(
        "当前没有真实历史样本。"
    )

    print(
        "不启用cold start，本次停止。"
    )

    conn.close()

    raise SystemExit


# ============================================================
# 12. 统计历史每天同一时刻累计首次钱包
#
# 例如checkpoint=06:00：
#
# 今天：
# 00:00 → 06:00
#
# 历史：
# 昨天00:00 → 06:00
# 前天00:00 → 06:00
# ...
#
# 不比较今天不同checkpoint。
# ============================================================

history_start = (
    f"{history_dates[-1]}T00:00:00Z"
)


cur.execute("""
SELECT

    token_key,

    substr(
        first_buy_time,
        1,
        10
    ) AS date_text,

    COUNT(*)

FROM wallet_token_first_buy_v2

WHERE

    first_buy_time >= ?

    AND first_buy_time < ?

    AND substr(
        first_buy_time,
        12,
        8
    ) < ?

GROUP BY

    token_key,

    substr(
        first_buy_time,
        1,
        10
    )
""", (
    history_start,
    today_start,
    checkpoint_time,
))


history_counts = defaultdict(
    dict
)


for (
    token_key,
    date_text,
    wallet_count
) in cur.fetchall():

    history_counts[
        int(token_key)
    ][
        date_text
    ] = int(
        wallet_count
    )


# ============================================================
# 13. 今天实时累计资金
# ============================================================

cur.execute("""
SELECT
    token_key,
    buy_usd,
    sell_usd,
    netflow_usd

FROM demand2_today_flow_v2

WHERE date = ?
""", (
    today_text,
))


today_flow = {}


for (
    token_key,
    buy_usd,
    sell_usd,
    netflow_usd
) in cur.fetchall():

    today_flow[
        int(token_key)
    ] = {
        "buy_usd":
            float(
                buy_usd or 0
            ),

        "sell_usd":
            float(
                sell_usd or 0
            ),

        "netflow_usd":
            float(
                netflow_usd or 0
            ),
    }


# ============================================================
# 14. 5日资金：
#
# 今天实时累计
# +
# 前4个完整UTC自然日
# ============================================================

previous_dates = [

    (
        today
        -
        timedelta(
            days=days_ago
        )
    ).isoformat()

    for days_ago in range(
        1,
        5
    )
]


# ============================================================
# 15. 先检查前4天是不是完整305行
#
# 缺一天都不能把它当0。
# ============================================================

for date_text in previous_dates:

    cur.execute("""
    SELECT COUNT(*)

    FROM daily_fund_flow

    WHERE date = ?
    """, (
        date_text,
    ))

    count = cur.fetchone()[0]

    if count != 305:

        raise RuntimeError(
            f"{date_text} daily_fund_flow "
            f"只有{count}行，不是305行。"
            "本次停止，避免错误计算5日净流入。"
        )


# ============================================================
# 16. 读取前4天每个Token净流入
# ============================================================

placeholders = ",".join(
    "?"
    for _ in previous_dates
)


cur.execute(
    f"""
    SELECT
        date,
        chain,
        contract_address,
        netflow_usd

    FROM daily_fund_flow

    WHERE date IN (
        {placeholders}
    )
    """,
    previous_dates,
)


historical_flow = {}


for (
    date_text,
    chain,
    contract_address,
    netflow_usd
) in cur.fetchall():

    key = (
        date_text,
        str(
            chain or ""
        ).strip().lower(),
        normalize_contract(
            chain,
            contract_address,
        ),
    )

    historical_flow[
        key
    ] = float(
        netflow_usd or 0
    )


# ============================================================
# 17. 读取需求一埋伏池
# ============================================================

ambush_pool = set()


if os.path.exists(
    AMBUSH_POOL_FILE
):

    with open(
        AMBUSH_POOL_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(
            file
        )

        for row in reader:

            chain = str(
                row.get(
                    "chain",
                    ""
                )
                or ""
            ).strip().lower()

            contract_address = normalize_contract(
                chain,
                row.get(
                    "contract_address",
                    ""
                ),
            )

            ambush_pool.add(
                (
                    chain,
                    contract_address,
                )
            )


# ============================================================
# 18. 信号等级
#
# 5日累计净流入：
#
# 0 ～ 20,000
# → 观察
#
# 20,000 ～ 100,000
# → 跟踪
#
# >= 100,000
# → 重点
# ============================================================

def get_level(
    five_day_netflow,
):

    if five_day_netflow >= 100000:

        return "重点"

    if five_day_netflow >= 20000:

        return "跟踪"

    return "观察"


# ============================================================
# 19. 逐Token计算
# ============================================================

results = []


for token_key, info in tokens.items():

    symbol = info[
        "symbol"
    ]

    chain = str(
        info[
            "chain"
        ]
        or ""
    ).strip().lower()

    contract_address = normalize_contract(
        chain,
        info[
            "contract_address"
        ],
    )


    # --------------------------------------------------------
    # 今天累计首次钱包
    # --------------------------------------------------------

    today_wallets = (
        today_wallet_counts.get(
            token_key,
            0
        )
    )


    # --------------------------------------------------------
    # 历史同期值
    # --------------------------------------------------------

    history_values = [

        history_counts[
            token_key
        ].get(
            date_text,
            0
        )

        for date_text
        in history_dates
    ]


    history_mean = statistics.mean(
        history_values
    )


    # --------------------------------------------------------
    # 历史标准差和Z-score
    # --------------------------------------------------------

    history_std = None

    z_score = None


    if len(
        history_values
    ) >= 2:

        std_value = statistics.stdev(
            history_values
        )

        if std_value > 0:

            history_std = std_value

            z_score = (
                today_wallets
                -
                history_mean
            ) / history_std


    # --------------------------------------------------------
    # 钱包倍数
    # --------------------------------------------------------

    if history_mean > 0:

        wallet_ratio = (
            today_wallets
            /
            history_mean
        )

    elif today_wallets > 0:

        wallet_ratio = float(
            "inf"
        )

    else:

        wallet_ratio = 0.0


    # --------------------------------------------------------
    # 钱包异常
    #
    # 有2个及以上历史样本：
    #
    # >40
    # AND >历史均值2倍
    # AND Z>2
    #
    # 只有1个真实历史样本：
    #
    # >40
    # AND >历史值2倍
    #
    # 不创造cold start。
    # --------------------------------------------------------

    if len(
        history_values
    ) == 1:

        wallet_anomaly = (

            today_wallets
            >
            MIN_WALLET_COUNT

            and

            today_wallets
            >
            history_mean
            *
            MIN_WALLET_RATIO
        )

    else:

        wallet_anomaly = (

            today_wallets
            >
            MIN_WALLET_COUNT

            and

            today_wallets
            >
            history_mean
            *
            MIN_WALLET_RATIO

            and

            z_score is not None

            and

            z_score
            >
            MIN_Z_SCORE
        )


    # --------------------------------------------------------
    # 今天累计资金
    # --------------------------------------------------------

    flow = today_flow.get(
        token_key,
        {
            "buy_usd":
                0.0,

            "sell_usd":
                0.0,

            "netflow_usd":
                0.0,
        }
    )


    today_netflow = flow[
        "netflow_usd"
    ]


    # --------------------------------------------------------
    # 前4天资金
    # --------------------------------------------------------

    previous_netflows = []


    for date_text in previous_dates:

        key = (
            date_text,
            chain,
            contract_address,
        )

        previous_netflows.append(
            historical_flow.get(
                key,
                0.0
            )
        )


    # --------------------------------------------------------
    # 5日累计净流入
    # --------------------------------------------------------

    five_day_netflow = (
        today_netflow
        +
        sum(
            previous_netflows
        )
    )


    # --------------------------------------------------------
    # 资金确认
    # --------------------------------------------------------

    funds_confirmed = (

        today_netflow
        >
        MIN_TODAY_NETFLOW

        and

        five_day_netflow
        >
        0
    )


    # --------------------------------------------------------
    # 最终需求二信号
    # --------------------------------------------------------

    final_signal = (

        wallet_anomaly

        and

        funds_confirmed
    )


    # --------------------------------------------------------
    # 信号等级
    # --------------------------------------------------------

    level = (
        get_level(
            five_day_netflow
        )
        if final_signal
        else ""
    )


    # --------------------------------------------------------
    # 是否属于需求一埋伏池
    # --------------------------------------------------------

    in_ambush_pool = (
        (
            chain,
            contract_address,
        )
        in ambush_pool
    )


    resonance = (

        final_signal

        and

        in_ambush_pool
    )


    results.append(
        {
            "symbol":
                symbol,

            "today_wallets":
                today_wallets,

            "history_mean":
                history_mean,

            "wallet_ratio":
                wallet_ratio,

            "z_score":
                z_score,

            "wallet_anomaly":
                wallet_anomaly,

            "today_netflow":
                today_netflow,

            "five_day_netflow":
                five_day_netflow,

            "funds_confirmed":
                funds_confirmed,

            "final_signal":
                final_signal,

            "level":
                level,

            "in_ambush_pool":
                in_ambush_pool,

            "resonance":
                resonance,
        }
    )


# ============================================================
# 20. 钱包异常候选
# ============================================================

wallet_candidates = [

    result

    for result
    in results

    if result[
        "wallet_anomaly"
    ]
]


wallet_candidates.sort(
    key=lambda result:
        result[
            "z_score"
        ]
        if result[
            "z_score"
        ] is not None
        else -999999,
    reverse=True,
)


# ============================================================
# 21. 最终信号
# ============================================================

final_signals = [

    result

    for result
    in results

    if result[
        "final_signal"
    ]
]


final_signals.sort(
    key=lambda result:
        result[
            "five_day_netflow"
        ],
    reverse=True,
)


# ============================================================
# 22. 输出
# ============================================================

print(
    "=" * 88
)

print(
    "需求二信号判断 V2"
)

print(
    "=" * 88
)

print(
    "checkpoint：",
    checkpoint
)

print(
    f"统计口径："
    f"{today_text} 00:00 "
    f"→ {checkpoint_time}"
)

print(
    "历史同期样本：",
    history_sample_count
)


print()

print(
    "=" * 88
)

print(
    "钱包异常候选"
)

print(
    "=" * 88
)


if not wallet_candidates:

    print(
        "当前没有钱包异常候选。"
    )


else:

    for result in wallet_candidates:

        if result[
            "wallet_ratio"
        ] == float(
            "inf"
        ):

            ratio_text = "∞"

        else:

            ratio_text = (
                f"{result['wallet_ratio']:.2f}x"
            )


        z_text = (
            "N/A"
            if result[
                "z_score"
            ] is None
            else
            f"{result['z_score']:.2f}"
        )


        print()

        print(
            result[
                "symbol"
            ]
        )

        print(
            "  今日首次钱包：",
            f"{result['today_wallets']:,}"
        )

        print(
            "  历史同期均值：",
            f"{result['history_mean']:.2f}"
        )

        print(
            "  倍数：",
            ratio_text
        )

        print(
            "  Z-score：",
            z_text
        )

        print(
            "  今日净流入：",
            f"${result['today_netflow']:,.2f}"
        )

        print(
            "  5日净流入：",
            f"${result['five_day_netflow']:,.2f}"
        )

        print(
            "  资金确认：",
            (
                "✅"
                if result[
                    "funds_confirmed"
                ]
                else
                "❌"
            )
        )


print()

print(
    "=" * 88
)

print(
    "最终需求二信号"
)

print(
    "=" * 88
)


if not final_signals:

    print(
        "当前没有满足全部条件的需求二信号。"
    )


else:

    for result in final_signals:

        print()

        if result[
            "resonance"
        ]:

            print(
                "🔥 双信号共振"
            )

        else:

            print(
                "✅ 需求二信号"
            )


        print(
            "  Token：",
            result[
                "symbol"
            ]
        )

        print(
            "  等级：",
            result[
                "level"
            ]
        )

        print(
            "  今日首次钱包：",
            f"{result['today_wallets']:,}"
        )

        print(
            "  今日净流入：",
            f"${result['today_netflow']:,.2f}"
        )

        print(
            "  5日净流入：",
            f"${result['five_day_netflow']:,.2f}"
        )

        print(
            "  需求一埋伏池：",
            (
                "是"
                if result[
                    "in_ambush_pool"
                ]
                else
                "否"
            )
        )


print()

print(
    "=" * 88
)

print(
    "汇总"
)

print(
    "=" * 88
)

print(
    "钱包异常候选：",
    len(
        wallet_candidates
    )
)

print(
    "最终需求二信号：",
    len(
        final_signals
    )
)

print(
    "双信号共振：",
    sum(
        1
        for result
        in final_signals
        if result[
            "resonance"
        ]
    )
)

print()

print(
    "Bitquery请求：0"
)

print(
    "Points消耗：0"
)


conn.close()
