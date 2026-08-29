# -*- coding: utf-8 -*-

# ============================================================
# 需求一：使用需求二日资金数据重算埋伏池
#
# 不请求 Bitquery。
#
# 数据来源：
# daily_fund_flow
#
# 规则保持不变：
#
# 1. 最近30个完整UTC自然日
# 2. 每日净买入 = buy_usd - sell_usd
# 3. 30天中净买入为正的天数 >= 20天
# 4. 30天累计净买入 > 50,000美元
#
# 本次先输出测试文件：
#
# alpha_ambush_pool_v2_test.csv
#
# 不覆盖正式：
#
# alpha_ambush_pool.csv
# ============================================================

import csv
import os
import sqlite3

from datetime import datetime
from datetime import timedelta
from datetime import timezone


DATABASE_FILE = "alpha_monitor.db"

OUTPUT_FILE = (
    "alpha_ambush_pool_v2_test.csv"
)

CURRENT_POOL_FILE = (
    "alpha_ambush_pool.csv"
)

WINDOW_DAYS = 30

MIN_POSITIVE_DAYS = 20

MIN_NETFLOW_30D = 50000.0


# ============================================================
# 1. 打开数据库
# ============================================================

conn = sqlite3.connect(
    DATABASE_FILE
)

conn.execute(
    "PRAGMA busy_timeout = 30000"
)

cur = conn.cursor()


# ============================================================
# 2. 读取当前Alpha Token
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


# 使用：
#
# chain + contract_address
#
# 作为Token唯一识别。
active_tokens = {}


for (
    token_key,
    symbol,
    chain,
    contract_address
) in token_rows:

    key = (
        str(
            chain or ""
        ).strip(),
        str(
            contract_address or ""
        ).strip(),
    )

    active_tokens[
        key
    ] = {
        "token_key":
            int(token_key),

        "symbol":
            symbol,

        "chain":
            chain,

        "contract_address":
            contract_address,
    }


# ============================================================
# 3. 找到最新完整UTC自然日
#
# 一个完整日必须有305行。
# ============================================================

cur.execute("""
SELECT
    date,
    COUNT(*) AS token_count

FROM daily_fund_flow

GROUP BY date

HAVING COUNT(*) = 305

ORDER BY date DESC

LIMIT 1
""")


row = cur.fetchone()


if not row:

    raise RuntimeError(
        "daily_fund_flow中没有完整305行的自然日"
    )


latest_date_text = row[0]


latest_date = datetime.strptime(
    latest_date_text,
    "%Y-%m-%d"
).date()


# ============================================================
# 4. 构造最近30个连续UTC自然日
#
# 不是随便找30个有数据的日期。
#
# 必须是：
#
# latest_date - 29天
# ...
# latest_date
#
# 连续30天。
# ============================================================

window_dates = [

    (
        latest_date
        -
        timedelta(
            days=days_back
        )
    ).isoformat()

    for days_back in range(
        WINDOW_DAYS - 1,
        -1,
        -1
    )
]


window_start = window_dates[0]

window_end = window_dates[-1]


# ============================================================
# 5. 检查这30天每天是否完整305行
#
# 有一天缺失都停止。
#
# 不允许偷偷把缺失日当0。
# ============================================================

for date_text in window_dates:

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
            f"{date_text}只有{count}行，"
            "不是完整305行。"
            "本次停止计算埋伏池。"
        )


# ============================================================
# 6. 读取最近30天全部资金数据
# ============================================================

cur.execute("""
SELECT
    date,
    symbol,
    chain,
    contract_address,
    buy_usd,
    sell_usd,
    netflow_usd

FROM daily_fund_flow

WHERE
    date >= ?
    AND date <= ?

ORDER BY
    date,
    chain,
    contract_address
""", (
    window_start,
    window_end,
))


fund_rows = cur.fetchall()


expected_rows = (
    WINDOW_DAYS
    *
    305
)


if len(fund_rows) != expected_rows:

    raise RuntimeError(
        f"30天理论应有{expected_rows:,}行，"
        f"实际{len(fund_rows):,}行。"
    )


# ============================================================
# 7. 初始化每个Token的30天统计
# ============================================================

stats = {}


for key, token in active_tokens.items():

    stats[
        key
    ] = {
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
                "contract_address"
            ],

        "days":
            0,

        "positive_days":
            0,

        "netflow_30d":
            0.0,
    }


# ============================================================
# 8. 累计30天资金
# ============================================================

unknown_rows = 0


for (
    date_text,
    symbol,
    chain,
    contract_address,
    buy_usd,
    sell_usd,
    netflow_usd
) in fund_rows:

    key = (
        str(
            chain or ""
        ).strip(),
        str(
            contract_address or ""
        ).strip(),
    )


    if key not in stats:

        unknown_rows += 1

        continue


    netflow = float(
        netflow_usd or 0
    )


    stats[
        key
    ][
        "days"
    ] += 1


    stats[
        key
    ][
        "netflow_30d"
    ] += netflow


    if netflow > 0:

        stats[
            key
        ][
            "positive_days"
        ] += 1


# ============================================================
# 9. 检查当前305个Token是否都有30天记录
# ============================================================

incomplete_tokens = []


for key, item in stats.items():

    if item[
        "days"
    ] != WINDOW_DAYS:

        incomplete_tokens.append(
            (
                item[
                    "symbol"
                ],
                item[
                    "days"
                ],
            )
        )


if incomplete_tokens:

    print(
        "以下当前Token没有完整30天记录："
    )

    for (
        symbol,
        days
    ) in incomplete_tokens:

        print(
            symbol,
            days
        )


    raise RuntimeError(
        "当前Token存在不完整30天数据，"
        "本次停止。"
    )


# ============================================================
# 10. 按需求一规则筛选
# ============================================================

qualified = []


for item in stats.values():

    positive_days = item[
        "positive_days"
    ]


    netflow_30d = item[
        "netflow_30d"
    ]


    if (
        positive_days
        >=
        MIN_POSITIVE_DAYS

        and

        netflow_30d
        >
        MIN_NETFLOW_30D
    ):

        qualified.append(
            item
        )


# ============================================================
# 11. 按30日累计净流入从高到低排序
# ============================================================

qualified.sort(
    key=lambda item:
        item[
            "netflow_30d"
        ],
    reverse=True,
)


# ============================================================
# 12. 输出测试CSV
# ============================================================

updated_at = datetime.now(
    timezone.utc
).strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)


with open(
    OUTPUT_FILE,
    "w",
    encoding="utf-8-sig",
    newline=""
) as file:

    writer = csv.writer(
        file
    )


    writer.writerow(
        [
            "rank",
            "symbol",
            "chain",
            "contract_address",
            "positive_days",
            "positive_ratio",
            "netflow_30d",
            "updated_at",
        ]
    )


    for rank, item in enumerate(
        qualified,
        start=1
    ):

        positive_days = item[
            "positive_days"
        ]


        writer.writerow(
            [
                rank,

                item[
                    "symbol"
                ],

                item[
                    "chain"
                ],

                item[
                    "contract_address"
                ],

                positive_days,

                (
                    f"{positive_days / WINDOW_DAYS * 100:.2f}%"
                ),

                round(
                    item[
                        "netflow_30d"
                    ],
                    2
                ),

                updated_at,
            ]
        )


# ============================================================
# 13. 与现有正式埋伏池做简单对比
# ============================================================

old_pool = set()


if os.path.exists(
    CURRENT_POOL_FILE
):

    with open(
        CURRENT_POOL_FILE,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(
            file
        )


        for row in reader:

            old_pool.add(
                (
                    str(
                        row.get(
                            "chain",
                            ""
                        )
                    ).strip(),

                    str(
                        row.get(
                            "contract_address",
                            ""
                        )
                    ).strip(),
                )
            )


new_pool = {
    (
        str(
            item[
                "chain"
            ]
        ).strip(),

        str(
            item[
                "contract_address"
            ]
        ).strip(),
    )

    for item in qualified
}


added = (
    new_pool
    -
    old_pool
)


removed = (
    old_pool
    -
    new_pool
)


# ============================================================
# 14. 输出结果
# ============================================================

print(
    "=" * 80
)

print(
    "需求一：使用需求二数据重算测试"
)

print(
    "=" * 80
)

print(
    "30日窗口：",
    window_start,
    "→",
    window_end
)

print(
    "完整自然日：",
    WINDOW_DAYS
)

print(
    "Alpha Token：",
    len(
        active_tokens
    )
)

print(
    "资金数据行：",
    f"{len(fund_rows):,}"
)

print(
    "未知历史行：",
    unknown_rows
)

print()

print(
    "满足埋伏池条件：",
    len(
        qualified
    )
)

print(
    "当前正式埋伏池：",
    len(
        old_pool
    )
)

print(
    "相比当前新增：",
    len(
        added
    )
)

print(
    "相比当前移出：",
    len(
        removed
    )
)

print()

print(
    "测试文件：",
    OUTPUT_FILE
)

print()

print(
    "Bitquery请求：0"
)

print(
    "Points消耗：0"
)


conn.close()
