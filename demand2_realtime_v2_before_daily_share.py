# -*- coding: utf-8 -*-
"""
需求二：实时增量采集 V2

核心规则：
1. 正常运行每15分钟一次。
2. 不重复查询“今天00:00到现在”，只查询 checkpoint 后的缺口。
3. 断机恢复最多2小时一段；跨 UTC 00:00 强制切开。
4. first-buy 聚合达到20,000行：
   - 大于1小时 -> 改成最多1小时段；
   - 大于15分钟 -> 改成15分钟段；
   - 15分钟仍达到20,000 -> 停止，不猜数据。
5. Token + Wallet 永久保存最早 first_buy_time。
6. 资金只保存当天累计和完整自然日汇总，不保存15分钟明细。
7. 一段数据全部写入成功后才推进 checkpoint。
8. 不包含任何 cold start / 冷启动逻辑。
"""

import argparse
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv


# =========================
# 基础配置
# =========================
DATABASE_FILE = "alpha_monitor.db"
BITQUERY_URL = "https://asia.streaming.bitquery.io/graphql"

FIRST_BUY_LIMIT = 20000
FLOW_LIMIT = 1000
MAX_CATCHUP_HOURS = 2
HTTP_TIMEOUT = 120


# =========================
# 单次HTTP请求同时查询：
# 1. Token + Wallet 的最早Buy时间
# 2. 每个Token的Buy/Sell USD
#
# 时间使用 [since, before)
# 避免相邻区间边界重复。
# =========================
QUERY = """
query Demand2Interval(
  $tokens: [String!]!
  $since: DateTime!
  $before: DateTime!
) {
  Trading {

    FirstBuys: Trades(
      limit: { count: 20000 }
      where: {
        Block: {
          Time: {
            since: $since
            before: $before
          }
        }
        Pair: {
          Token: {
            Id: { in: $tokens }
          }
        }
        Side: { is: "Buy" }
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
        first_buy_time: Time(minimum: Block_Time)
      }
    }

    Flow: Trades(
      limit: { count: 1000 }
      where: {
        Block: {
          Time: {
            since: $since
            before: $before
          }
        }
        Pair: {
          Token: {
            Id: { in: $tokens }
          }
        }
      }
    ) {
      Pair {
        Token {
          Id
        }
      }
      buy_usd: sum(
        of: AmountsInUsd_Quote
        if: { Side: { is: "Buy" } }
      )
      sell_usd: sum(
        of: AmountsInUsd_Quote
        if: { Side: { is: "Sell" } }
      )
    }
  }
}
"""


# =========================
# UTC时间工具
# =========================
def utc_now():
    return datetime.now(timezone.utc)


def to_iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def floor_to_15m(dt):
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute)


def utc_day_start(dt):
    dt = dt.astimezone(timezone.utc)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


# =========================
# SQLite工具
# =========================
def connect_db():
    conn = sqlite3.connect(DATABASE_FILE)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def ensure_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS demand2_v2_meta (
            meta_key TEXT PRIMARY KEY,
            meta_value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS demand2_today_flow_v2 (
            date TEXT NOT NULL,
            token_key INTEGER NOT NULL,
            buy_usd REAL NOT NULL DEFAULT 0,
            sell_usd REAL NOT NULL DEFAULT 0,
            netflow_usd REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, token_key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS demand2_daily_flow_v2 (
            date TEXT NOT NULL,
            token_key INTEGER NOT NULL,
            buy_usd REAL NOT NULL DEFAULT 0,
            sell_usd REAL NOT NULL DEFAULT 0,
            netflow_usd REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, token_key)
        ) WITHOUT ROWID;
        """
    )
    conn.commit()


def get_meta(conn, key):
    row = conn.execute(
        """
        SELECT meta_value
        FROM demand2_v2_meta
        WHERE meta_key = ?
        """,
        (key,),
    ).fetchone()

    return row[0] if row else None


def set_meta(conn, key, value):
    conn.execute(
        """
        INSERT INTO demand2_v2_meta (meta_key, meta_value)
        VALUES (?, ?)
        ON CONFLICT(meta_key)
        DO UPDATE SET meta_value = excluded.meta_value
        """,
        (key, str(value)),
    )


def get_first_buy_meta(conn, key):
    row = conn.execute(
        """
        SELECT meta_value
        FROM wallet_first_buy_v2_meta
        WHERE meta_key = ?
        """,
        (key,),
    ).fetchone()

    return row[0] if row else None


# =========================
# Token列表
# =========================
def load_tokens(conn):
    rows = conn.execute(
        """
        SELECT token_key, token_id, symbol
        FROM alpha_token_registry
        ORDER BY token_key
        """
    ).fetchall()

    token_ids = []
    by_id = {}

    for token_key, token_id, symbol in rows:
        if not token_id:
            continue

        token_id = str(token_id)

        token_ids.append(token_id)
        by_id[token_id] = (
            int(token_key),
            symbol or "",
        )

    # 现在正式监控的Alpha Token应为305个。
    # 数量变化时先停，不悄悄漏币。
    if len(token_ids) != 305:
        raise RuntimeError(
            f"当前有效Token数量为 {len(token_ids)}，预期305。"
            "为避免漏币，本次停止。"
        )

    return token_ids, by_id


# =========================
# 钱包标准化
# =========================
def normalize_wallet(token_id, wallet):
    wallet = str(wallet or "").strip()

    evm_prefixes = (
        "bid:bsc:",
        "bid:base:",
        "bid:eth:",
        "bid:arbitrum:",
    )

    if token_id.startswith(evm_prefixes):
        return wallet.lower()

    return wallet


# =========================
# 初始化实时采集
#
# first_buy历史已补到04:15，
# 但今天00:00~04:15资金尚未进入需求二累计。
#
# 所以实时资金从该UTC自然日00:00重新采集。
# 这段first-buy即使重复查询，也会被主键自动去重。
# =========================
def prepare(conn):
    ensure_tables(conn)

    existing = get_meta(conn, "last_success_end")

    if existing:
        print("需求二实时采集已经初始化，不覆盖现有checkpoint。")
        print("last_success_end：", existing)
        print("Bitquery请求：0")
        return

    first_buy_status = get_first_buy_meta(conn, "status")
    first_buy_end = get_first_buy_meta(conn, "window_end")

    if first_buy_status != "complete" or not first_buy_end:
        raise RuntimeError("first_buy V2历史底库尚未完成")

    start = utc_day_start(parse_iso(first_buy_end))

    set_meta(conn, "last_success_end", to_iso(start))
    set_meta(conn, "collector_status", "prepared")
    set_meta(conn, "successful_http_responses", "0")

    conn.commit()

    print("=" * 72)
    print("需求二实时采集 V2 初始化")
    print("=" * 72)
    print("首次资金累计起点：", to_iso(start))
    print("first_buy历史已到：", first_buy_end)
    print("说明：00:00之后重复first-buy会自动去重。")
    print("Bitquery请求：0")
    print("Points消耗：0")


# =========================
# 查看状态
# =========================
def show_status(conn):
    ensure_tables(conn)

    last_success_end = get_meta(conn, "last_success_end")
    latest_complete = floor_to_15m(utc_now())

    print("=" * 72)
    print("需求二实时采集 V2 状态")
    print("=" * 72)

    print(
        "状态：",
        get_meta(conn, "collector_status") or "未初始化",
    )
    print(
        "last_success_end：",
        last_success_end or "未初始化",
    )
    print(
        "最新完整15分钟：",
        to_iso(latest_complete),
    )

    if last_success_end:
        gap = latest_complete - parse_iso(last_success_end)
        gap_minutes = max(0, int(gap.total_seconds() // 60))
        print("待补时长：", gap_minutes, "分钟")

    first_buy_total = conn.execute(
        "SELECT COUNT(*) FROM wallet_token_first_buy_v2"
    ).fetchone()[0]

    today_flow_rows = conn.execute(
        "SELECT COUNT(*) FROM demand2_today_flow_v2"
    ).fetchone()[0]

    daily_flow_rows = conn.execute(
        "SELECT COUNT(*) FROM demand2_daily_flow_v2"
    ).fetchone()[0]

    print("first_buy总数：", f"{first_buy_total:,}")
    print("当前临时资金行：", today_flow_rows)
    print("需求二完整日资金行：", daily_flow_rows)
    print(
        "成功HTTP响应：",
        get_meta(conn, "successful_http_responses") or "0",
    )
    print("Bitquery请求：0")


# =========================
# 调用Bitquery
#
# 最多尝试2次：
# - 402：立即停止
# - 429：等15秒，再试1次
# - 网络错误：等5秒，再试1次
# =========================
def query_interval(token_ids, start, end):
    load_dotenv()

    bitquery_token = os.getenv("BITQUERY_TOKEN")

    if not bitquery_token:
        raise RuntimeError("没有读取到BITQUERY_TOKEN")

    payload = {
        "query": QUERY,
        "variables": {
            "tokens": token_ids,
            "since": to_iso(start),
            "before": to_iso(end),
        },
    }

    for attempt in range(1, 3):
        try:
            response = requests.post(
                BITQUERY_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {bitquery_token}",
                },
                json=payload,
                timeout=HTTP_TIMEOUT,
            )

        except requests.RequestException as error:
            if attempt == 2:
                raise RuntimeError(f"网络错误：{error}") from error

            print("⚠️ 网络错误，5秒后重试一次")
            time.sleep(5)
            continue

        if response.status_code == 402:
            raise RuntimeError("Bitquery 402：额度或计费不可用")

        if response.status_code == 429:
            if attempt == 2:
                raise RuntimeError("Bitquery 429：请求频率受限")

            print("⚠️ Bitquery 429，15秒后重试一次")
            time.sleep(15)
            continue

        if response.status_code != 200:
            raise RuntimeError(
                f"Bitquery HTTP {response.status_code}："
                f"{response.text[:500]}"
            )

        result = response.json()

        if result.get("errors"):
            raise RuntimeError(
                f"GraphQL错误：{result['errors']}"
            )

        trading = result.get("data", {}).get("Trading", {})

        return (
            trading.get("FirstBuys", []),
            trading.get("Flow", []),
        )

    raise RuntimeError("Bitquery请求失败")


# =========================
# 记录收到有效结果的HTTP次数
# 只用于运行审计，不等于Points。
# =========================
def record_successful_response(conn):
    old_count = int(
        get_meta(conn, "successful_http_responses") or 0
    )

    set_meta(
        conn,
        "successful_http_responses",
        old_count + 1,
    )

    conn.commit()


# =========================
# 固化一个完整UTC自然日
#
# - 305个Token全部写入日汇总；
# - 没交易的Token写0；
# - 删除当天临时累计；
# - 日汇总只保留35天。
# =========================
def finalize_day(
    conn,
    date_text,
    all_token_keys,
    updated_at,
):
    for token_key in all_token_keys:
        row = conn.execute(
            """
            SELECT buy_usd, sell_usd, netflow_usd
            FROM demand2_today_flow_v2
            WHERE date = ? AND token_key = ?
            """,
            (date_text, token_key),
        ).fetchone()

        if row:
            buy_usd, sell_usd, netflow_usd = row
        else:
            buy_usd = 0.0
            sell_usd = 0.0
            netflow_usd = 0.0

        conn.execute(
            """
            INSERT INTO demand2_daily_flow_v2 (
                date,
                token_key,
                buy_usd,
                sell_usd,
                netflow_usd,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, token_key)
            DO UPDATE SET
                buy_usd = excluded.buy_usd,
                sell_usd = excluded.sell_usd,
                netflow_usd = excluded.netflow_usd,
                updated_at = excluded.updated_at
            """,
            (
                date_text,
                token_key,
                buy_usd,
                sell_usd,
                netflow_usd,
                updated_at,
            ),
        )

    # 完整日已经固化，删除临时累计。
    conn.execute(
        """
        DELETE FROM demand2_today_flow_v2
        WHERE date = ?
        """,
        (date_text,),
    )

    # 需求一只需要最近30天，留35天缓冲。
    date_dt = datetime.fromisoformat(date_text)
    cutoff_date = (
        date_dt - timedelta(days=35)
    ).date().isoformat()

    conn.execute(
        """
        DELETE FROM demand2_daily_flow_v2
        WHERE date < ?
        """,
        (cutoff_date,),
    )

    set_meta(conn, "last_finalized_date", date_text)


# =========================
# 原子保存一个查询区间
#
# first-buy、资金、跨日固化、checkpoint
# 全部在同一个SQLite事务里。
# =========================
def save_interval(
    conn,
    by_id,
    first_rows,
    flow_rows,
    start,
    end,
):
    # 保存函数绝不能收到跨UTC自然日区间。
    last_moment = end - timedelta(microseconds=1)

    if start.date() != last_moment.date():
        raise RuntimeError("内部错误：收到跨日保存区间")

    date_text = start.date().isoformat()
    updated_at = to_iso(utc_now())

    # 先整理first-buy。
    first_values = []

    for row in first_rows:
        token_id = str(
            row.get("Pair", {})
            .get("Token", {})
            .get("Id", "")
            or ""
        )

        token_info = by_id.get(token_id)

        if not token_info:
            continue

        token_key, _symbol = token_info

        wallet = normalize_wallet(
            token_id,
            row.get("Trader", {}).get("Address", ""),
        )

        first_buy_time = (
            row.get("Block", {})
            .get("first_buy_time")
        )

        if wallet and first_buy_time:
            first_values.append(
                (
                    token_key,
                    wallet,
                    first_buy_time,
                    updated_at,
                )
            )

    # 再整理资金。
    flow_values = []

    for row in flow_rows:
        token_id = str(
            row.get("Pair", {})
            .get("Token", {})
            .get("Id", "")
            or ""
        )

        token_info = by_id.get(token_id)

        if not token_info:
            continue

        token_key, _symbol = token_info

        buy_usd = float(row.get("buy_usd") or 0)
        sell_usd = float(row.get("sell_usd") or 0)
        netflow_usd = buy_usd - sell_usd

        flow_values.append(
            (
                date_text,
                token_key,
                buy_usd,
                sell_usd,
                netflow_usd,
                updated_at,
            )
        )

    all_token_keys = sorted(
        {
            token_key
            for token_key, _symbol in by_id.values()
        }
    )

    try:
        conn.execute("BEGIN")

        # 永久first-buy表：后续买入永远不能覆盖更早时间。
        if first_values:
            conn.executemany(
                """
                INSERT INTO wallet_token_first_buy_v2 (
                    token_key,
                    wallet,
                    first_buy_time,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(token_key, wallet)
                DO UPDATE SET
                    first_buy_time = CASE
                        WHEN excluded.first_buy_time
                             < wallet_token_first_buy_v2.first_buy_time
                        THEN excluded.first_buy_time
                        ELSE wallet_token_first_buy_v2.first_buy_time
                    END,
                    updated_at = excluded.updated_at
                """,
                first_values,
            )

        # 资金只累计，不保存本区间明细。
        if flow_values:
            conn.executemany(
                """
                INSERT INTO demand2_today_flow_v2 (
                    date,
                    token_key,
                    buy_usd,
                    sell_usd,
                    netflow_usd,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, token_key)
                DO UPDATE SET
                    buy_usd =
                        demand2_today_flow_v2.buy_usd
                        + excluded.buy_usd,
                    sell_usd =
                        demand2_today_flow_v2.sell_usd
                        + excluded.sell_usd,
                    netflow_usd =
                        demand2_today_flow_v2.netflow_usd
                        + excluded.netflow_usd,
                    updated_at = excluded.updated_at
                """,
                flow_values,
            )

        # 如果刚好补到UTC 00:00，上一自然日已经完整。
        if end == utc_day_start(end):
            finalize_day(
                conn,
                date_text,
                all_token_keys,
                updated_at,
            )

        # 最后才推进checkpoint。
        set_meta(conn, "last_success_end", to_iso(end))
        set_meta(conn, "collector_status", "running")

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    return len(first_values), len(flow_values)


# =========================
# 超过20,000时的简单降级
# =========================
def smaller_segments(start, end):
    duration = end - start

    # 大于1小时 -> 最多1小时一段。
    if duration > timedelta(hours=1):
        step = timedelta(hours=1)

    # 15分钟~1小时 -> 最多15分钟一段。
    elif duration > timedelta(minutes=15):
        step = timedelta(minutes=15)

    # 已经15分钟或更短 -> 不再自动拆。
    else:
        return None

    segments = []
    cursor = start

    while cursor < end:
        next_end = min(cursor + step, end)
        segments.append((cursor, next_end))
        cursor = next_end

    return segments


# =========================
# 查询并处理一个区间
# =========================
def process_interval(
    conn,
    token_ids,
    by_id,
    start,
    end,
):
    print(
        f"查询：{to_iso(start)} → {to_iso(end)}"
    )

    started = time.time()

    first_rows, flow_rows = query_interval(
        token_ids,
        start,
        end,
    )

    elapsed = time.time() - started

    # 先记这次成功HTTP响应。
    record_successful_response(conn)

    print(
        f"返回：first-buy {len(first_rows):,} 行；"
        f"资金 {len(flow_rows):,} Token；"
        f"{elapsed:.2f} 秒"
    )

    # 资金按Token聚合，正常最多305行。
    if len(flow_rows) >= FLOW_LIMIT:
        raise RuntimeError(
            "资金结果达到1000行保护线。"
            "本段未写入、checkpoint未推进。"
        )

    # first-buy达到上限，当前结果作废并缩小时间范围。
    if len(first_rows) >= FIRST_BUY_LIMIT:
        fallback = smaller_segments(start, end)

        if not fallback:
            raise RuntimeError(
                f"{to_iso(start)} → {to_iso(end)} "
                "在15分钟内仍达到20,000行。"
                "本段未写入、checkpoint未推进。"
            )

        print(
            "⚠️ first-buy达到20,000保护线，"
            "当前结果作废，按更小时间段重新补。"
        )

        for child_start, child_end in fallback:
            process_interval(
                conn,
                token_ids,
                by_id,
                child_start,
                child_end,
            )

        return

    saved_first, saved_flow = save_interval(
        conn,
        by_id,
        first_rows,
        flow_rows,
        start,
        end,
    )

    print(
        f"✅ 保存：first-buy聚合 {saved_first:,} 行；"
        f"资金 {saved_flow:,} Token；"
        f"checkpoint → {to_iso(end)}"
    )


# =========================
# 顶层补数区间：
# - 最多2小时
# - 不能跨UTC 00:00
# =========================
def build_catchup_segments(start, end):
    segments = []
    cursor = start

    while cursor < end:
        next_midnight = (
            utc_day_start(cursor)
            + timedelta(days=1)
        )

        next_end = min(
            cursor + timedelta(hours=MAX_CATCHUP_HOURS),
            next_midnight,
            end,
        )

        segments.append((cursor, next_end))
        cursor = next_end

    return segments


# =========================
# 执行采集
#
# --run-one：
# 只处理checkpoint后的第一个15分钟，
# 用来先验证查询语法和实际返回结构。
#
# --run：
# 一次追到最新完整15分钟。
# =========================
def run_collector(conn, run_one=False):
    ensure_tables(conn)

    last_success_end = get_meta(
        conn,
        "last_success_end",
    )

    if not last_success_end:
        raise RuntimeError("请先执行 --prepare")

    token_ids, by_id = load_tokens(conn)

    start = parse_iso(last_success_end)
    target = floor_to_15m(utc_now())

    if start >= target:
        print("✅ 已追到最新完整15分钟，无需查询Bitquery。")
        return

    if run_one:
        # 首次验证严格只取一个15分钟。
        segments = [
            (
                start,
                min(
                    start + timedelta(minutes=15),
                    target,
                ),
            )
        ]
    else:
        segments = build_catchup_segments(
            start,
            target,
        )

    print("=" * 72)
    print("需求二实时增量采集 V2")
    print("=" * 72)
    print("当前checkpoint：", to_iso(start))
    print("目标时间：", to_iso(target))
    print("本次顶层区间：", len(segments))

    for segment_start, segment_end in segments:
        print("-" * 72)

        process_interval(
            conn,
            token_ids,
            by_id,
            segment_start,
            segment_end,
        )

    print("=" * 72)
    print("✅ 本次采集完成")
    print(
        "last_success_end：",
        get_meta(conn, "last_success_end"),
    )


# =========================
# CLI入口
# =========================
def main():
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--prepare",
        action="store_true",
    )

    group.add_argument(
        "--status",
        action="store_true",
    )

    group.add_argument(
        "--run-one",
        action="store_true",
    )

    group.add_argument(
        "--run",
        action="store_true",
    )

    args = parser.parse_args()

    conn = connect_db()

    try:
        if args.prepare:
            prepare(conn)

        elif args.status:
            show_status(conn)

        elif args.run_one:
            run_collector(
                conn,
                run_one=True,
            )

        elif args.run:
            run_collector(
                conn,
                run_one=False,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
