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
        last_buy_time: Time(maximum: Block_Time)
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

        CREATE TABLE IF NOT EXISTS demand2_recent_interval_v2 (

            interval_start TEXT PRIMARY KEY,

            interval_end TEXT NOT NULL,

            updated_at TEXT NOT NULL

        ) WITHOUT ROWID;


        CREATE TABLE IF NOT EXISTS demand2_recent_first_buy_v2 (

            interval_start TEXT NOT NULL,

            interval_end TEXT NOT NULL,

            token_key INTEGER NOT NULL,

            wallet TEXT NOT NULL,

            first_buy_time TEXT NOT NULL,

            updated_at TEXT NOT NULL,

            PRIMARY KEY (
                interval_start,
                token_key,
                wallet
            )

        ) WITHOUT ROWID;


        CREATE TABLE IF NOT EXISTS demand2_recent_flow_v2 (

            interval_start TEXT NOT NULL,

            interval_end TEXT NOT NULL,

            token_key INTEGER NOT NULL,

            buy_usd REAL NOT NULL DEFAULT 0,

            sell_usd REAL NOT NULL DEFAULT 0,

            netflow_usd REAL NOT NULL DEFAULT 0,

            updated_at TEXT NOT NULL,

            PRIMARY KEY (
                interval_start,
                token_key
            )

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

    latest_daily_flow = conn.execute(
        """
        SELECT
            date,
            COUNT(*)
        FROM daily_fund_flow
        GROUP BY date
        ORDER BY date DESC
        LIMIT 1
        """
    ).fetchone()

    print("first_buy总数：", f"{first_buy_total:,}")
    print("当前临时资金行：", today_flow_rows)
    if latest_daily_flow:
        print(
            "daily_fund_flow最新完整日：",
            latest_daily_flow[0],
            f"({latest_daily_flow[1]}行)",
        )
    else:
        print(
            "daily_fund_flow最新完整日：无"
        )
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
    # ========================================================
    # 读取当天需求二累计资金
    #
    # 有交易的Token存在记录，
    # 没交易的Token后面自动补0。
    # ========================================================

    flow_rows = conn.execute(
        """
        SELECT
            token_key,
            buy_usd,
            sell_usd,
            netflow_usd

        FROM demand2_today_flow_v2

        WHERE date = ?
        """,
        (
            date_text,
        ),
    ).fetchall()


    flow_by_token_key = {
        int(token_key): (
            float(buy_usd or 0),
            float(sell_usd or 0),
            float(netflow_usd or 0),
        )

        for (
            token_key,
            buy_usd,
            sell_usd,
            netflow_usd
        ) in flow_rows
    }


    # ========================================================
    # 读取当天当前305个Alpha Token基础信息
    # ========================================================

    registry_rows = conn.execute(
        """
        SELECT
            token_key,
            symbol,
            chain,
            contract_address

        FROM alpha_token_registry

        ORDER BY token_key
        """
    ).fetchall()


    if len(registry_rows) != 305:

        raise RuntimeError(
            f"跨日固化时Alpha Token数量="
            f"{len(registry_rows)}，"
            "预期305。"
            "为避免日资金缺失，本次停止。"
        )


    # ========================================================
    # 构造完整305行
    #
    # 当天没交易的Token：
    #
    # buy = 0
    # sell = 0
    # net = 0
    # ========================================================

    daily_rows = []


    for (
        token_key,
        symbol,
        chain,
        contract_address
    ) in registry_rows:

        (
            buy_usd,
            sell_usd,
            netflow_usd
        ) = flow_by_token_key.get(
            int(token_key),
            (
                0.0,
                0.0,
                0.0,
            ),
        )


        daily_rows.append(
            (
                date_text,
                symbol,
                chain,
                contract_address,
                buy_usd,
                sell_usd,
                netflow_usd,
                updated_at,
            )
        )


    # ========================================================
    # 直接写入需求一已经使用的 daily_fund_flow
    #
    # 从这里开始：
    #
    # 需求二 = 唯一Bitquery资金采集入口
    #
    # 需求一以后只读取 daily_fund_flow
    # ========================================================

    conn.executemany(
        """
        INSERT INTO daily_fund_flow (

            date,
            symbol,
            chain,
            contract_address,
            buy_usd,
            sell_usd,
            netflow_usd,
            updated_at

        )

        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )

        ON CONFLICT(
            date,
            chain,
            contract_address
        )

        DO UPDATE SET

            symbol =
                excluded.symbol,

            buy_usd =
                excluded.buy_usd,

            sell_usd =
                excluded.sell_usd,

            netflow_usd =
                excluded.netflow_usd,

            updated_at =
                excluded.updated_at
        """,
        daily_rows,
    )


    # ========================================================
    # 当天已经固化成完整自然日。
    #
    # 临时累计表中的当天数据立即删除。
    # ========================================================

    conn.execute(
        """
        DELETE FROM demand2_today_flow_v2
        WHERE date = ?
        """,
        (
            date_text,
        ),
    )


    # ========================================================
    # daily_fund_flow只保留最近35天
    #
    # 需求一使用30天，
    # 多留5天缓冲。
    # ========================================================

    date_dt = datetime.fromisoformat(
        date_text
    )

    cutoff_date = (
        date_dt
        -
        timedelta(
            days=35
        )
    ).date().isoformat()


    conn.execute(
        """
        DELETE FROM daily_fund_flow
        WHERE date < ?
        """,
        (
            cutoff_date,
        ),
    )


    # ========================================================
    # ========================================================
    # 每天一次：first 详情和 last 状态最多保留30天。
    # ========================================================
    finalized_end = (
        datetime.fromisoformat(date_text)
        .replace(tzinfo=timezone.utc)
        + timedelta(days=1)
    )

    wallet_cutoff = to_iso(
        finalized_end - timedelta(days=30)
    )

    conn.execute(
        """
        DELETE FROM wallet_token_first_buy_v2
        WHERE first_buy_time < ?
        """,
        (wallet_cutoff,),
    )

    conn.execute(
        """
        DELETE FROM wallet_token_state
        WHERE last_buy_time < ?
        """,
        (wallet_cutoff,),
    )

    # 保存最后完成的UTC自然日
    # ========================================================

    set_meta(
        conn,
        "last_finalized_date",
        date_text,
    )

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

    # 先整理本区间每个 Token + Wallet 的最早/最后买入时间。
    # first_buy_time：本区间第一笔 Buy。
    # last_buy_time ：本区间最后一笔 Buy。
    wallet_values = []

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

        last_buy_time = (
            row.get("Block", {})
            .get("last_buy_time")
        )

        if not wallet or not first_buy_time:
            continue

        if not last_buy_time:
            last_buy_time = first_buy_time

        wallet_values.append(
            (
                token_key,
                wallet,
                first_buy_time,
                last_buy_time,
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

    # ========================================================
    # 准备最近15分钟小时校正快照
    #
    # 只有标准15分钟查询才进入小时校正缓存。
    #
    # 2小时断点补采等大区间继续按原逻辑执行，
    # 不改变原来的省Points策略。
    # ========================================================

    interval_start_text = to_iso(
        start
    )

    interval_end_text = to_iso(
        end
    )

    keep_recent_snapshot = (
        end - start
        ==
        timedelta(
            minutes=15
        )
    )


    recent_first_values = [
        (
            interval_start_text,
            interval_end_text,
            token_key,
            wallet,
            first_buy_time,
            row_updated_at,
        )
        for (
            token_key,
            wallet,
            first_buy_time,
            _last_buy_time,
            row_updated_at,
        ) in wallet_values
    ]

    recent_flow_values = [

        (
            interval_start_text,
            interval_end_text,
            token_key,
            buy_usd,
            sell_usd,
            netflow_usd,
            row_updated_at,
        )

        for (
            _date_text,
            token_key,
            buy_usd,
            sell_usd,
            netflow_usd,
            row_updated_at,
        ) in flow_values
    ]


    all_token_keys = sorted(
        {
            token_key
            for token_key, _symbol in by_id.values()
        }
    )

    try:
        conn.execute("BEGIN")

        # ====================================================
        # 保存最近15分钟聚合快照
        #
        # 实时值仍然立即进入正式累计，
        # 这里额外留一份很小的临时快照，
        # 供下一小时统一校正。
        # ====================================================

        if keep_recent_snapshot:

            # 如果同一区间因为异常被重新执行，
            # 先删除旧快照，再写新值。

            conn.execute(
                """
                DELETE FROM demand2_recent_first_buy_v2
                WHERE interval_start = ?
                """,
                (
                    interval_start_text,
                ),
            )

            conn.execute(
                """
                DELETE FROM demand2_recent_flow_v2
                WHERE interval_start = ?
                """,
                (
                    interval_start_text,
                ),
            )


            conn.execute(
                """
                INSERT INTO demand2_recent_interval_v2 (

                    interval_start,
                    interval_end,
                    updated_at

                )

                VALUES (?, ?, ?)

                ON CONFLICT(interval_start)

                DO UPDATE SET

                    interval_end =
                        excluded.interval_end,

                    updated_at =
                        excluded.updated_at
                """,
                (
                    interval_start_text,
                    interval_end_text,
                    updated_at,
                ),
            )


            if recent_first_values:

                conn.executemany(
                    """
                    INSERT OR REPLACE INTO
                    demand2_recent_first_buy_v2 (

                        interval_start,
                        interval_end,
                        token_key,
                        wallet,
                        first_buy_time,
                        updated_at

                    )

                    VALUES (
                        ?, ?, ?, ?, ?, ?
                    )
                    """,
                    recent_first_values,
                )


            if recent_flow_values:

                conn.executemany(
                    """
                    INSERT OR REPLACE INTO
                    demand2_recent_flow_v2 (

                        interval_start,
                        interval_end,
                        token_key,
                        buy_usd,
                        sell_usd,
                        netflow_usd,
                        updated_at

                    )

                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    recent_flow_values,
                )


        # ====================================================
        # 滚动15天 FIRST BUY
        # first 表用于统计；last 状态只用于判断15天沉寂。
        # ====================================================
        new_first_count = 0

        for (
            token_key,
            wallet,
            first_buy_time,
            last_buy_time,
            row_updated_at,
        ) in wallet_values:

            existing_state = conn.execute(
                """
                SELECT last_buy_time
                FROM wallet_token_state
                WHERE token_key = ? AND wallet = ?
                """,
                (token_key, wallet),
            ).fetchone()

            previous_last = (
                existing_state[0]
                if existing_state
                else None
            )

            # 旧 state 和现有 first 之间可能有不到一天的切换缺口。
            # state 缺失时，用近15天已有 first 做一次保守兜底，
            # 只为了避免切换当天重复计数，不重算历史。
            if previous_last is None:
                existing_first = conn.execute(
                    """
                    SELECT first_buy_time
                    FROM wallet_token_first_buy_v2
                    WHERE token_key = ? AND wallet = ?
                    """,
                    (token_key, wallet),
                ).fetchone()

                if existing_first:
                    old_first_time = existing_first[0]
                    gap = (
                        parse_iso(first_buy_time)
                        - parse_iso(old_first_time)
                    )
                    if (
                        gap >= timedelta(0)
                        and gap < timedelta(days=15)
                    ):
                        previous_last = old_first_time

            if previous_last is None:
                is_new_first = True
            else:
                is_new_first = (
                    parse_iso(first_buy_time)
                    >= parse_iso(previous_last)
                    + timedelta(days=15)
                )

            # 只有真正的新 FIRST BUY 才刷新 first。
            if is_new_first:
                conn.execute(
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
                        first_buy_time = excluded.first_buy_time,
                        updated_at = excluded.updated_at
                    """,
                    (
                        token_key,
                        wallet,
                        first_buy_time,
                        row_updated_at,
                    ),
                )
                new_first_count += 1

            # 所有本区间买过的钱包都只更新隐藏 last 状态。
            conn.execute(
                """
                INSERT INTO wallet_token_state (
                    token_key,
                    wallet,
                    last_buy_time,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(token_key, wallet)
                DO UPDATE SET
                    last_buy_time = CASE
                        WHEN excluded.last_buy_time
                             > wallet_token_state.last_buy_time
                        THEN excluded.last_buy_time
                        ELSE wallet_token_state.last_buy_time
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    token_key,
                    wallet,
                    last_buy_time,
                    row_updated_at,
                ),
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

    return new_first_count, len(flow_values)


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

    # 计算本查询区间的市场资金总量
    # 这里只用于日志显示，不新增任何数据库存储。
    interval_buy = sum(
        float(row.get("buy_usd") or 0)
        for row in flow_rows
    )

    interval_sell = sum(
        float(row.get("sell_usd") or 0)
        for row in flow_rows
    )

    interval_net = (
        interval_buy
        -
        interval_sell
    )

    print(
        f"返回：买入钱包聚合 {len(first_rows):,} 行；"
        f"资金 {len(flow_rows):,} Token；"
        f"{elapsed:.2f} 秒"
    )

    print(
        f"区间买入：${interval_buy:,.2f}；"
        f"区间卖出：${interval_sell:,.2f}；"
        f"区间净流入：${interval_net:,.2f}"
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
            "⚠️ 买入钱包聚合达到20,000保护线，"
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
        f"✅ 保存：滚动FIRST BUY {saved_first:,} 个；"
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
