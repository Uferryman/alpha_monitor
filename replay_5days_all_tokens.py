# -*- coding: utf-8 -*-

# ============================================================
# 需求二历史粗回测：
# 1. 回看 2026-08-24 ~ 2026-08-28 五个完整 UTC 自然日。
# 2. 每天按 15 分钟 checkpoint 回放钱包异常。
# 3. 钱包异常规则：
#    - 当前累计 first-buy 钱包 > 40
#    - 当前累计 > 前最多15个可用历史日同期均值 × 2
#    - Z-score > 2
# 4. 历史资金没有保存15分钟快照，因此“粗略”使用完整自然日资金：
#    - 当日最终净买入 > 1000 美元
#    - 截至当日的5D累计净买入 > 0
# 5. 只读 SQLite，不请求 Bitquery，不修改数据库。
# ============================================================

import sqlite3
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta

DB = "alpha_monitor.db"

START_DAY = date(2026, 8, 24)
END_DAY = date(2026, 8, 28)

MIN_WALLETS = 40
MULTIPLE = 2.0
MIN_Z = 2.0
MIN_DAY_NETFLOW = 1000.0

# ------------------------------------------------------------
# 打开数据库，只执行读取。
# ------------------------------------------------------------
conn = sqlite3.connect(DB)

# ------------------------------------------------------------
# 读取当前 Alpha Token 注册表。
# token_key -> (symbol, chain, contract_address)
# ------------------------------------------------------------
registry_rows = conn.execute(
    """
    SELECT token_key, symbol, chain, contract_address
    FROM alpha_token_registry
    ORDER BY token_key
    """
).fetchall()

registry = {
    int(token_key): {
        "symbol": symbol or "",
        "chain": chain or "",
        "contract": contract or "",
    }
    for token_key, symbol, chain, contract in registry_rows
}

# ------------------------------------------------------------
# 获取当前 first-buy 底库真正覆盖的最早日期。
# 早于这个日期的数据不能当成0，只能视为“无历史样本”。
# ------------------------------------------------------------
first_day_row = conn.execute(
    """
    SELECT MIN(substr(first_buy_time, 1, 10))
    FROM wallet_token_first_buy_v2
    """
).fetchone()

if not first_day_row or not first_day_row[0]:
    raise RuntimeError("wallet_token_first_buy_v2 没有数据")

DATA_START_DAY = date.fromisoformat(first_day_row[0])

# ------------------------------------------------------------
# 为了回看8/24，需要读取它前面最多15天的数据。
# 实际只能从当前数据库最早覆盖日期开始。
# ------------------------------------------------------------
read_start = max(
    DATA_START_DAY,
    START_DAY - timedelta(days=15),
)

# ------------------------------------------------------------
# 先按：
# Token + UTC日期 + 15分钟slot
# 聚合 first-buy 钱包数量。
#
# slot:
# 1  = 00:00~00:15
# 2  = 00:15~00:30
# ...
# 96 = 23:45~24:00
# ------------------------------------------------------------
wallet_rows = conn.execute(
    """
    SELECT
        token_key,
        substr(first_buy_time, 1, 10) AS day,
        (
            CAST(substr(first_buy_time, 12, 2) AS INTEGER) * 4
            +
            CAST(
                CAST(substr(first_buy_time, 15, 2) AS INTEGER) / 15
                AS INTEGER
            )
            + 1
        ) AS slot,
        COUNT(*) AS wallets
    FROM wallet_token_first_buy_v2
    WHERE substr(first_buy_time, 1, 10) >= ?
      AND substr(first_buy_time, 1, 10) <= ?
    GROUP BY token_key, day, slot
    ORDER BY token_key, day, slot
    """,
    (
        read_start.isoformat(),
        END_DAY.isoformat(),
    ),
).fetchall()

# ------------------------------------------------------------
# slot_counts[token_key][day] = 长度96数组。
# 每个位置先保存“这个15分钟新发生的 first-buy 数”。
# ------------------------------------------------------------
slot_counts = defaultdict(lambda: defaultdict(lambda: [0] * 96))

for token_key, day_text, slot, wallets in wallet_rows:
    slot_num = int(slot)
    if 1 <= slot_num <= 96:
        slot_counts[int(token_key)][day_text][slot_num - 1] = int(wallets)

# ------------------------------------------------------------
# 把每个自然日的“15分钟新增”转换成“截至checkpoint累计”。
# ------------------------------------------------------------
cumulative = defaultdict(dict)

for token_key, by_day in slot_counts.items():
    for day_text, counts in by_day.items():
        running = 0
        arr = []
        for value in counts:
            running += value
            arr.append(running)
        cumulative[token_key][day_text] = arr

# ------------------------------------------------------------
# 某 Token 某天完全没有 first-buy 时，也必须得到96个0。
# ------------------------------------------------------------
ZERO_96 = [0] * 96

# ------------------------------------------------------------
# 读取日资金。
# 通过 registry 的 chain + contract 与 daily_fund_flow 对齐。
# 为计算8/24的5D，需要从8/20开始。
# ------------------------------------------------------------
fund_start = START_DAY - timedelta(days=4)

fund_rows = conn.execute(
    """
    SELECT
        r.token_key,
        f.date,
        f.netflow_usd
    FROM alpha_token_registry AS r
    JOIN daily_fund_flow AS f
      ON f.chain = r.chain
     AND f.contract_address = r.contract_address
    WHERE f.date >= ?
      AND f.date <= ?
    ORDER BY r.token_key, f.date
    """,
    (
        fund_start.isoformat(),
        END_DAY.isoformat(),
    ),
).fetchall()

funds = defaultdict(dict)

for token_key, day_text, netflow in fund_rows:
    funds[int(token_key)][day_text] = float(netflow or 0.0)

# ------------------------------------------------------------
# 输出级别函数。
# ------------------------------------------------------------
def signal_level(netflow_5d):
    if netflow_5d >= 100000:
        return "重点"
    if netflow_5d >= 20000:
        return "跟踪"
    return "观察"

# ------------------------------------------------------------
# 逐日、逐Token、逐15分钟checkpoint回放。
# ------------------------------------------------------------
all_final = []

print("=" * 118)
print("需求二：最近5个完整UTC日历史粗回测")
print(f"回测日期：{START_DAY} ~ {END_DAY}")
print(f"first-buy底库最早日期：{DATA_START_DAY}")
print("钱包：每15分钟同期回放")
print("资金：使用完整自然日净流入作粗略过滤")
print("Bitquery请求：0")
print("=" * 118)

target_day = START_DAY

while target_day <= END_DAY:

    wallet_triggered = []
    final_triggered = []

    # --------------------------------------------------------
    # 历史样本日期：
    # 目标日前最多15个实际有数据覆盖的自然日。
    # 没有交易的Token当天可计0；
    # 数据库尚未覆盖的更早日期不计入样本。
    # --------------------------------------------------------
    hist_start = max(
        DATA_START_DAY,
        target_day - timedelta(days=15),
    )

    history_days = []
    cursor = hist_start

    while cursor < target_day:
        history_days.append(cursor)
        cursor += timedelta(days=1)

    # --------------------------------------------------------
    # 至少两个实际历史日才做 Z-score。
    # --------------------------------------------------------
    if len(history_days) < 2:
        print()
        print(target_day.isoformat())
        print("  历史样本不足2天，跳过")
        target_day += timedelta(days=1)
        continue

    for token_key, info in registry.items():

        target_arr = cumulative.get(
            token_key,
            {},
        ).get(
            target_day.isoformat(),
            ZERO_96,
        )

        first_trigger = None

        # ----------------------------------------------------
        # 一天96个checkpoint逐个看。
        # ----------------------------------------------------
        for slot_idx in range(96):

            today_count = target_arr[slot_idx]

            # 钱包数不足40时直接跳过，减少运算。
            if today_count <= MIN_WALLETS:
                continue

            history_counts = []

            for hist_day in history_days:
                hist_arr = cumulative.get(
                    token_key,
                    {},
                ).get(
                    hist_day.isoformat(),
                    ZERO_96,
                )

                history_counts.append(hist_arr[slot_idx])

            mean_value = statistics.mean(history_counts)

            # 必须超过历史同期均值2倍。
            if today_count <= mean_value * MULTIPLE:
                continue

            std_value = statistics.pstdev(history_counts)

            if std_value > 0:
                z_value = (today_count - mean_value) / std_value
            else:
                # 历史全部一样，而今天明显更高时视为无穷大Z。
                z_value = float("inf") if today_count > mean_value else 0.0

            if z_value <= MIN_Z:
                continue

            # ------------------------------------------------
            # 找到当天第一次满足钱包异常的checkpoint就记录。
            # ------------------------------------------------
            slot_number = slot_idx + 1
            total_minutes = slot_number * 15
            hour = total_minutes // 60
            minute = total_minutes % 60

            if hour == 24:
                checkpoint_text = "24:00"
            else:
                checkpoint_text = f"{hour:02d}:{minute:02d}"

            first_trigger = {
                "date": target_day.isoformat(),
                "token_key": token_key,
                "symbol": info["symbol"],
                "chain": info["chain"],
                "checkpoint": checkpoint_text,
                "wallets": today_count,
                "mean": mean_value,
                "z": z_value,
                "history_n": len(history_counts),
            }

            break

        if first_trigger is None:
            continue

        wallet_triggered.append(first_trigger)

        # ----------------------------------------------------
        # 粗略资金过滤1：
        # 使用当天完整UTC自然日净流入 > $1,000。
        # ----------------------------------------------------
        day_net = funds.get(
            token_key,
            {},
        ).get(
            target_day.isoformat(),
        )

        # 没有对应完整日资金时不能猜。
        if day_net is None:
            continue

        if day_net <= MIN_DAY_NETFLOW:
            continue

        # ----------------------------------------------------
        # 粗略资金过滤2：
        # 当前日 + 前4个完整自然日 = 5D累计净流入 > 0。
        # ----------------------------------------------------
        five_day_values = []
        five_day_complete = True

        for back in range(4, -1, -1):
            d = target_day - timedelta(days=back)
            value = funds.get(
                token_key,
                {},
            ).get(
                d.isoformat(),
            )

            if value is None:
                five_day_complete = False
                break

            five_day_values.append(value)

        if not five_day_complete:
            continue

        net_5d = sum(five_day_values)

        if net_5d <= 0:
            continue

        first_trigger["day_net"] = day_net
        first_trigger["net_5d"] = net_5d
        first_trigger["level"] = signal_level(net_5d)

        final_triggered.append(first_trigger)
        all_final.append(first_trigger)

    # --------------------------------------------------------
    # 输出当天摘要。
    # --------------------------------------------------------
    print()
    print("-" * 118)
    print(target_day.isoformat())
    print(
        f"钱包异常：{len(wallet_triggered)} 个"
        f"｜通过日净流入>$1,000 + 5D>0：{len(final_triggered)} 个"
    )

    if final_triggered:
        print(
            f"{'时间':<7}"
            f"{'Token':<16}"
            f"{'链':<10}"
            f"{'钱包':>8}"
            f"{'同期均值':>12}"
            f"{'Z':>9}"
            f"{'当日净流入':>18}"
            f"{'5D净流入':>18}"
            f"{'级别':>8}"
        )

        final_triggered.sort(
            key=lambda x: (
                x["checkpoint"],
                -x["net_5d"],
            )
        )

        for item in final_triggered:
            z_text = (
                "inf"
                if item["z"] == float("inf")
                else f'{item["z"]:.2f}'
            )

            print(
                f'{item["checkpoint"]:<7}'
                f'{item["symbol"][:14]:<16}'
                f'{item["chain"][:8]:<10}'
                f'{item["wallets"]:>8}'
                f'{item["mean"]:>12.2f}'
                f'{z_text:>9}'
                f'${item["day_net"]:>16,.2f}'
                f'${item["net_5d"]:>16,.2f}'
                f'{item["level"]:>8}'
            )

    target_day += timedelta(days=1)

# ------------------------------------------------------------
# 总结：同一Token如果多天触发，逐日保留。
# ------------------------------------------------------------
print()
print("=" * 118)
print(f"5天最终触发记录：{len(all_final)} 条")
print("说明：资金使用“全天最终净流入”，只能作为粗回测，不能还原触发时点的实时资金状态。")
print("数据库修改：0")
print("Bitquery请求：0")
print("Points消耗：0")
print("=" * 118)

conn.close()
