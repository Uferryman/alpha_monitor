import sqlite3
import statistics
from datetime import datetime, timedelta, timezone

DB = "alpha_monitor.db"
TOKEN_KEY = 34

# 回看这4天
TARGET_DAYS = [
    "2026-08-25",
    "2026-08-26",
    "2026-08-27",
    "2026-08-28",
]

conn = sqlite3.connect(DB)

# 读取龙虾所有已保存的 first_buy_time
rows = conn.execute(
    """
    SELECT first_buy_time
    FROM wallet_token_first_buy_v2
    WHERE token_key = ?
    ORDER BY first_buy_time
    """,
    (TOKEN_KEY,),
).fetchall()

conn.close()

times = [
    datetime.fromisoformat(x[0].replace("Z", "+00:00"))
    for x in rows
]

print("=" * 78)
print("龙虾：15分钟同期钱包异常历史回放")
print("规则：钱包>40、>历史同期均值2倍、Z>2")
print("Bitquery：0次")
print("=" * 78)

for target_text in TARGET_DAYS:

    target_day = datetime.fromisoformat(
        target_text + "T00:00:00+00:00"
    )

    # 最多取目标日前15个自然日
    history_days = [
        target_day - timedelta(days=i)
        for i in range(15, 0, -1)
    ]

    # 但数据库初始化以前没有数据，不能把缺失历史错误当成0
    earliest_day = min(times).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    history_days = [
        d for d in history_days
        if d >= earliest_day
    ]

    triggers = []

    # 每天96个15分钟检查点
    for slot in range(1, 97):

        checkpoint = target_day + timedelta(minutes=15 * slot)
        seconds = 15 * slot * 60

        # 今天截至该checkpoint的首买钱包数
        today_count = sum(
            1
            for t in times
            if t.date() == target_day.date()
            and (
                t.hour * 3600
                + t.minute * 60
                + t.second
            ) < seconds
        )

        # 前15天同一时刻累计钱包
        history_counts = []

        for d in history_days:

            count = sum(
                1
                for t in times
                if t.date() == d.date()
                and (
                    t.hour * 3600
                    + t.minute * 60
                    + t.second
                ) < seconds
            )

            history_counts.append(count)

        if not history_counts:
            continue

        mean = statistics.mean(history_counts)

        if len(history_counts) >= 2:
            std = statistics.pstdev(history_counts)
            z = (
                (today_count - mean) / std
                if std > 0
                else float("inf")
            )
        else:
            z = None

        # 完全按照现有钱包异常条件
        condition_1 = today_count > 40
        condition_2 = today_count > mean * 2

        if len(history_counts) >= 2:
            condition_3 = z > 2
        else:
            condition_3 = True

        if condition_1 and condition_2 and condition_3:
            triggers.append(
                (
                    checkpoint.strftime("%H:%M"),
                    today_count,
                    mean,
                    z,
                )
            )

    print()
    print(target_text)

    if not triggers:
        print("  ❌ 全天没有触发钱包异常")
        continue

    first = triggers[0]
    last = triggers[-1]

    print(
        f"  🔥 首次触发：{first[0]} UTC"
        f"｜钱包 {first[1]}"
        f"｜同期均值 {first[2]:.2f}"
        f"｜2倍 {first[2]*2:.2f}"
        f"｜Z {first[3]:.2f}"
    )

    print(
        f"  最后触发：{last[0]} UTC"
        f"｜钱包 {last[1]}"
    )

    print(
        f"  当天触发checkpoint：{len(triggers)} 个"
    )

print()
print("=" * 78)
print("完成：只读本地SQLite，Bitquery请求 0 次")
