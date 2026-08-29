# ============================================================
# 需求二：first_buy V2 历史底库验证
#
# 这一步：
# 1. 只读取本地 SQLite
# 2. 不请求 Bitquery
# 3. 不消耗 Points
#
# 核心验证口径：
#
# 当前冻结截止时间：
# 2026-08-29 04:15 UTC
#
# 今天：
# 统计 2026-08-29 00:00 ～ 04:15
# 首次买入某 Token 的钱包数
#
# 历史：
# 分别统计前 15 天
# 每天 00:00 ～ 04:15
# 首次买入该 Token 的钱包数
#
# 注意：
# 不是拿今天 04:15 和今天 04:00 / 03:45 比。
#
# 而是：
#
# 今天 04:15累计
# VS
# 昨天 04:15累计
# 前天 04:15累计
# ...
# 前15天 04:15累计
#
# 这就是我们最终需求二的正确比较口径。
# ============================================================

import sqlite3
import statistics

from datetime import datetime
from datetime import timedelta
from collections import defaultdict


# ============================================================
# 1. 数据库
# ============================================================

DATABASE_FILE = "alpha_monitor.db"

conn = sqlite3.connect(
    DATABASE_FILE
)

cur = conn.cursor()


# ============================================================
# 2. 读取 V2 初始化窗口
# ============================================================

cur.execute(
    """
    SELECT
        meta_key,
        meta_value

    FROM wallet_first_buy_v2_meta
    """
)

meta = dict(
    cur.fetchall()
)


window_start = meta.get(
    "window_start"
)

window_end = meta.get(
    "window_end"
)

status = meta.get(
    "status"
)

successful_query_count = meta.get(
    "successful_query_count",
    "0"
)


if not window_start or not window_end:

    raise RuntimeError(
        "没有找到 first_buy V2 初始化窗口"
    )


# ============================================================
# 3. 时间解析
# ============================================================

# 把：
# 2026-08-29T04:15:00Z
#
# 转成 Python datetime
window_end_dt = datetime.fromisoformat(
    window_end.replace(
        "Z",
        "+00:00"
    )
)


window_start_dt = datetime.fromisoformat(
    window_start.replace(
        "Z",
        "+00:00"
    )
)


# 当前比较日期
today = window_end_dt.date()


# 当前同一时刻
#
# 例如：
# 04:15:00
checkpoint_time = window_end_dt.strftime(
    "%H:%M:%S"
)


# ============================================================
# 4. 基础完整性检查
# ============================================================

cur.execute(
    """
    SELECT COUNT(*)
    FROM wallet_token_first_buy_v2
    """
)

total_rows = cur.fetchone()[0]


cur.execute(
    """
    SELECT COUNT(
        DISTINCT token_key
    )
    FROM wallet_token_first_buy_v2
    """
)

distinct_tokens = cur.fetchone()[0]


# 检查 first_buy_time 是否为空
cur.execute(
    """
    SELECT COUNT(*)

    FROM wallet_token_first_buy_v2

    WHERE
        first_buy_time IS NULL
        OR first_buy_time = ''
    """
)

empty_time_count = cur.fetchone()[0]


# 检查有没有早于冻结窗口的数据
cur.execute(
    """
    SELECT COUNT(*)

    FROM wallet_token_first_buy_v2

    WHERE first_buy_time < ?
    """,
    (
        window_start,
    )
)

before_window_count = cur.fetchone()[0]


# 检查有没有达到或超过冻结截止时间的数据
#
# 我们整个统计统一采用：
#
# start <= time < end
#
# 即左闭右开。
cur.execute(
    """
    SELECT COUNT(*)

    FROM wallet_token_first_buy_v2

    WHERE first_buy_time >= ?
    """,
    (
        window_end,
    )
)

after_window_count = cur.fetchone()[0]


# ============================================================
# 5. 显示基础检查
# ============================================================

print(
    "=" * 88
)

print(
    "需求二 first_buy V2 历史底库验证"
)

print(
    "=" * 88
)

print(
    f"初始化状态：{status}"
)

print(
    f"冻结开始：{window_start}"
)

print(
    f"冻结结束：{window_end}"
)

print(
    f"成功Bitquery请求：{successful_query_count}"
)

print(
    f"Token-Wallet总数：{total_rows:,}"
)

print(
    f"有first_buy数据的Token：{distinct_tokens}"
)

print()

print(
    f"first_buy_time为空：{empty_time_count}"
)

print(
    f"早于冻结开始：{before_window_count}"
)

print(
    f"达到/超过冻结结束：{after_window_count}"
)


if (
    empty_time_count == 0
    and before_window_count == 0
    and after_window_count == 0
):

    print(
        "\n✅ first_buy 时间范围检查通过"
    )

else:

    print(
        "\n❌ first_buy 时间范围存在异常"
    )


# ============================================================
# 6. 获取 Token 名称
# ============================================================

cur.execute(
    """
    SELECT
        token_key,
        symbol

    FROM alpha_token_registry
    """
)

token_symbols = {
    int(token_key):
        symbol

    for token_key, symbol
    in cur.fetchall()
}


# ============================================================
# 7. 查询每天“截至同一时刻”的首次钱包数
#
# checkpoint_time：
# 04:15:00
#
# 因此只统计：
#
# 00:00:00 <= first_buy_time < 04:15:00
#
# 每个自然日独立计算。
# ============================================================

cur.execute(
    """
    SELECT

        token_key,

        substr(
            first_buy_time,
            1,
            10
        ) AS first_buy_date,

        COUNT(*) AS wallet_count

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
    """,
    (
        window_start,
        window_end,
        checkpoint_time,
    )
)


# counts[token_key][date] = count
counts = defaultdict(
    dict
)


for (
    token_key,
    date_text,
    wallet_count
) in cur.fetchall():

    counts[
        int(token_key)
    ][
        date_text
    ] = int(
        wallet_count
    )


# ============================================================
# 8. 构造今天 + 前15天日期
# ============================================================

today_text = today.isoformat()


history_dates = []

for days_ago in range(
    1,
    16
):

    history_date = (
        today
        -
        timedelta(
            days=days_ago
        )
    )

    history_dates.append(
        history_date.isoformat()
    )


# ============================================================
# 9. 计算每个 Token 的同期统计
# ============================================================

results = []


for token_key, symbol in token_symbols.items():

    token_counts = counts.get(
        token_key,
        {}
    )


    # 今天截至04:15累计首次钱包
    today_count = token_counts.get(
        today_text,
        0
    )


    # 前15天每天截至04:15累计首次钱包
    history_values = [

        token_counts.get(
            date_text,
            0
        )

        for date_text
        in history_dates
    ]


    # 历史平均值
    history_mean = statistics.mean(
        history_values
    )


    # 15个样本，因此可以计算样本标准差
    if len(
        history_values
    ) >= 2:

        history_std = statistics.stdev(
            history_values
        )

    else:

        history_std = 0


    # 今天 / 历史平均
    if history_mean > 0:

        ratio = (
            today_count
            /
            history_mean
        )

    elif today_count > 0:

        ratio = float(
            "inf"
        )

    else:

        ratio = 0


    # Z-score
    if history_std > 0:

        z_score = (
            today_count
            -
            history_mean
        ) / history_std

    else:

        z_score = 0


    # ========================================================
    # 钱包异常条件
    #
    # 注意：
    # 这里只验证“钱包条件”。
    #
    # 尚未加入：
    # - 今日净买入 > 1000 美元
    # - 5日累计净买入 > 0
    #
    # 所以这里只能叫：
    #
    # 钱包异常候选
    #
    # 不是最终交易信号。
    # ========================================================

    wallet_anomaly = (

        today_count > 40

        and

        today_count
        >
        history_mean * 2

        and

        z_score > 2
    )


    results.append(
        {
            "token_key":
                token_key,

            "symbol":
                symbol,

            "today":
                today_count,

            "history":
                history_values,

            "mean":
                history_mean,

            "std":
                history_std,

            "ratio":
                ratio,

            "z":
                z_score,

            "wallet_anomaly":
                wallet_anomaly,
        }
    )


# ============================================================
# 10. 显示比较口径
# ============================================================

print()

print(
    "=" * 88
)

print(
    "同期累计比较口径"
)

print(
    "=" * 88
)

print(
    f"今天：{today_text} "
    f"00:00 → {checkpoint_time}"
)

print()

print(
    "历史15个样本："
)

for date_text in history_dates:

    print(
        f"  {date_text} "
        f"00:00 → {checkpoint_time}"
    )


# ============================================================
# 11. 查看三个重点样例
#
# 牛来
# P
# Fartcoin
# ============================================================

print()

print(
    "=" * 88
)

print(
    "样例 Token 同期首次买入钱包"
)

print(
    "=" * 88
)


sample_symbols = {
    "牛来",
    "P",
    "Fartcoin",
}


for result in results:

    if result[
        "symbol"
    ] not in sample_symbols:

        continue


    print()

    print(
        "-" * 88
    )

    print(
        f"Token：{result['symbol']}"
    )

    print(
        f"今天截至 {checkpoint_time}："
        f"{result['today']:,}"
    )

    print(
        f"历史15日均值："
        f"{result['mean']:.2f}"
    )

    print(
        f"历史标准差："
        f"{result['std']:.2f}"
    )


    if result[
        "ratio"
    ] == float(
        "inf"
    ):

        ratio_text = "∞"

    else:

        ratio_text = (
            f"{result['ratio']:.2f}x"
        )


    print(
        f"倍数：{ratio_text}"
    )

    print(
        f"Z-score："
        f"{result['z']:.2f}"
    )

    print(
        "钱包异常候选："
        +
        (
            "✅ 是"
            if result[
                "wallet_anomaly"
            ]
            else
            "否"
        )
    )

    print()

    print(
        "前15天同一时刻："
    )


    for (
        date_text,
        value
    ) in zip(
        history_dates,
        result[
            "history"
        ]
    ):

        print(
            f"  {date_text}："
            f"{value:,}"
        )


# ============================================================
# 12. 今天首次钱包最多的 Top 20
# ============================================================

print()

print(
    "=" * 88
)

print(
    f"今天截至 {checkpoint_time} "
    "首次买入钱包 Top 20"
)

print(
    "=" * 88
)


top_results = sorted(
    results,
    key=lambda x:
        x[
            "today"
        ],
    reverse=True,
)[:20]


print(
    f"{'Token':<20}"
    f"{'今日':>8}"
    f"{'历史均值':>12}"
    f"{'倍数':>10}"
    f"{'Z':>10}"
    f"{'候选':>8}"
)


for result in top_results:

    if result[
        "ratio"
    ] == float(
        "inf"
    ):

        ratio_text = "inf"

    else:

        ratio_text = (
            f"{result['ratio']:.2f}"
        )


    flag = (
        "YES"
        if result[
            "wallet_anomaly"
        ]
        else
        ""
    )


    print(
        f"{str(result['symbol'])[:18]:<20}"
        f"{result['today']:>8,}"
        f"{result['mean']:>12.2f}"
        f"{ratio_text:>10}"
        f"{result['z']:>10.2f}"
        f"{flag:>8}"
    )


# ============================================================
# 13. 钱包异常候选列表
# ============================================================

candidates = [

    result

    for result
    in results

    if result[
        "wallet_anomaly"
    ]
]


candidates.sort(
    key=lambda x:
        x[
            "z"
        ],
    reverse=True,
)


print()

print(
    "=" * 88
)

print(
    "仅钱包维度异常候选"
)

print(
    "=" * 88
)


if not candidates:

    print(
        "当前没有满足钱包异常条件的 Token。"
    )

else:

    for result in candidates:

        print(
            f"{result['symbol']:<20} "
            f"今日={result['today']:,}  "
            f"均值={result['mean']:.2f}  "
            f"倍数={result['ratio']:.2f}x  "
            f"Z={result['z']:.2f}"
        )


print()

print(
    f"钱包异常候选数量："
    f"{len(candidates)}"
)

print()

print(
    "注意：以上还不是最终信号。"
)

print(
    "最终还要叠加："
)

print(
    "1. 今日累计市场净买入 > $1,000"
)

print(
    "2. 5日累计市场净买入 > 0"
)

print()

print(
    "Bitquery请求：0"
)

print(
    "Points消耗：0"
)


conn.close()
