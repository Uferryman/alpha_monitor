# -*- coding: utf-8 -*-

# ============================================================
# 需求二 —— 每小时数据校正 V2
#
# 核心原则：
#
# 1. +2分钟实时数据继续立即用于信号。
#
# 2. 每小时只额外重查一次上一完整UTC小时。
#
# 3. 钱包和资金一起校正。
#
# 4. 钱包允许：
#    - 新增
#    - 撤回
#    - first_buy_time修正
#
# 5. 资金允许：
#    - 正向修正
#    - 负向修正
#
# 6. 不保存原始Swap。
#
# 7. 校正完成后删除已经不需要的15分钟临时快照。
# ============================================================

from datetime import timedelta

from demand2_realtime_v2 import (
    FIRST_BUY_LIMIT,
    FLOW_LIMIT,
    connect_db,
    ensure_tables,
    get_meta,
    set_meta,
    load_tokens,
    normalize_wallet,
    query_interval,
    record_successful_response,
    utc_now,
    to_iso,
)


# ============================================================
# 把Bitquery first-buy返回整理成：
#
# (token_key, wallet)
# ->
# first_buy_time
# ============================================================

def build_first_map(
    rows,
    by_id,
):

    result = {}

    for row in rows:

        token_id = str(
            row.get(
                "Pair",
                {},
            )
            .get(
                "Token",
                {},
            )
            .get(
                "Id",
                "",
            )
            or ""
        )

        token_info = by_id.get(
            token_id
        )

        if not token_info:
            continue

        token_key, _symbol = (
            token_info
        )

        wallet = normalize_wallet(
            token_id,
            row.get(
                "Trader",
                {},
            ).get(
                "Address",
                "",
            ),
        )

        first_time = (
            row.get(
                "Block",
                {},
            )
            .get(
                "first_buy_time"
            )
        )

        if (
            not wallet
            or
            not first_time
        ):
            continue

        key = (
            int(token_key),
            wallet,
        )

        old = result.get(
            key
        )

        # 同一个小时里，
        # 同Token+Wallet只保留最早买入时间。
        if (
            old is None
            or
            first_time < old
        ):

            result[key] = (
                first_time
            )

    return result


# ============================================================
# 把资金返回整理成：
#
# token_key
# ->
# (buy_usd, sell_usd)
# ============================================================

def build_flow_map(
    rows,
    by_id,
):

    result = {}

    for row in rows:

        token_id = str(
            row.get(
                "Pair",
                {},
            )
            .get(
                "Token",
                {},
            )
            .get(
                "Id",
                "",
            )
            or ""
        )

        token_info = by_id.get(
            token_id
        )

        if not token_info:
            continue

        token_key, _symbol = (
            token_info
        )

        token_key = int(
            token_key
        )

        buy_usd = float(
            row.get(
                "buy_usd"
            )
            or 0
        )

        sell_usd = float(
            row.get(
                "sell_usd"
            )
            or 0
        )

        old_buy, old_sell = (
            result.get(
                token_key,
                (
                    0.0,
                    0.0,
                ),
            )
        )

        result[token_key] = (
            old_buy
            +
            buy_usd,

            old_sell
            +
            sell_usd,
        )

    return result


# ============================================================
# 发一次Bitquery请求，并记录成功HTTP次数
# ============================================================

def request_once(
    conn,
    token_ids,
    start,
    end,
):

    print(
        "小时校正查询：",
        to_iso(start),
        "→",
        to_iso(end),
    )

    first_rows, flow_rows = (
        query_interval(
            token_ids,
            start,
            end,
        )
    )

    record_successful_response(
        conn
    )

    conn.commit()

    return (
        first_rows,
        flow_rows,
    )


# ============================================================
# 查询上一完整小时
#
# 正常只需要1次请求。
#
# 如果整个1小时 first-buy 碰到20,000保护线，
# 才退回4个15分钟重新查。
# ============================================================

def query_corrected_hour(
    conn,
    token_ids,
    by_id,
    hour_start,
    hour_end,
):

    request_count = 1

    first_rows, flow_rows = (
        request_once(
            conn,
            token_ids,
            hour_start,
            hour_end,
        )
    )


    if len(flow_rows) >= FLOW_LIMIT:

        raise RuntimeError(
            "小时校正资金结果达到1000行保护线，停止校正。"
        )


    # 正常情况：
    # 1小时没有碰到20,000行。
    if len(first_rows) < FIRST_BUY_LIMIT:

        return (
            build_first_map(
                first_rows,
                by_id,
            ),

            build_flow_map(
                flow_rows,
                by_id,
            ),

            request_count,
        )


    print(
        "⚠️ 1小时first-buy达到20,000保护线，"
        "改为4个15分钟重新校正。"
    )


    corrected_first = {}

    corrected_flow = {}


    cursor = hour_start


    while cursor < hour_end:

        child_end = min(
            cursor
            +
            timedelta(
                minutes=15
            ),
            hour_end,
        )


        child_first, child_flow = (
            request_once(
                conn,
                token_ids,
                cursor,
                child_end,
            )
        )

        request_count += 1


        if (
            len(child_first)
            >=
            FIRST_BUY_LIMIT
        ):

            raise RuntimeError(
                f"{to_iso(cursor)} → "
                f"{to_iso(child_end)} "
                "15分钟仍达到20,000行，停止校正。"
            )


        if (
            len(child_flow)
            >=
            FLOW_LIMIT
        ):

            raise RuntimeError(
                "15分钟资金达到1000行保护线，停止校正。"
            )


        child_first_map = (
            build_first_map(
                child_first,
                by_id,
            )
        )


        for key, first_time in (
            child_first_map.items()
        ):

            old_time = (
                corrected_first.get(
                    key
                )
            )

            if (
                old_time is None
                or
                first_time < old_time
            ):

                corrected_first[
                    key
                ] = first_time


        child_flow_map = (
            build_flow_map(
                child_flow,
                by_id,
            )
        )


        for (
            token_key,
            (
                buy_usd,
                sell_usd,
            )
        ) in child_flow_map.items():

            old_buy, old_sell = (
                corrected_flow.get(
                    token_key,
                    (
                        0.0,
                        0.0,
                    ),
                )
            )

            corrected_flow[
                token_key
            ] = (
                old_buy
                +
                buy_usd,

                old_sell
                +
                sell_usd,
            )


        cursor = child_end


    return (
        corrected_first,
        corrected_flow,
        request_count,
    )


# ============================================================
# 主程序
# ============================================================

def main():

    conn = connect_db()

    ensure_tables(
        conn
    )


    # ========================================================
    # 找到最近一个完整UTC小时
    # ========================================================

    now = utc_now()

    hour_end = now.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    hour_start = (
        hour_end
        -
        timedelta(
            hours=1
        )
    )


    hour_start_text = (
        to_iso(
            hour_start
        )
    )

    hour_end_text = (
        to_iso(
            hour_end
        )
    )


    print(
        "=" * 72
    )

    print(
        "需求二每小时校正 V2"
    )

    print(
        "=" * 72
    )

    print(
        "目标小时：",
        hour_start_text,
        "→",
        hour_end_text,
    )


    # ========================================================
    # 已经校正过就直接结束。
    #
    # 因此虽然每15分钟都会调用本脚本，
    # 实际每小时最多校正成功一次。
    # ========================================================

    last_corrected = get_meta(
        conn,
        "last_hourly_correction_end",
    )


    if (
        last_corrected
        ==
        hour_end_text
    ):

        print(
            "✅ 该小时已经校正过"
        )

        print(
            "Bitquery请求：0"
        )

        conn.close()

        return


    # ========================================================
    # 必须确认这一小时4个15分钟实时快照齐全。
    #
    # 如果电脑之前关机，
    # 使用2小时断点补采得到的数据不强行做小时校正。
    #
    # 这样不会为了校正破坏原来的省Points补采规则。
    # ========================================================

    interval_rows = conn.execute(
        """
        SELECT

            interval_start,
            interval_end

        FROM demand2_recent_interval_v2

        WHERE
            interval_start >= ?
            AND interval_end <= ?

        ORDER BY interval_start
        """,
        (
            hour_start_text,
            hour_end_text,
        ),
    ).fetchall()


    actual_intervals = set(
        interval_rows
    )


    expected_intervals = set()


    cursor = hour_start


    while cursor < hour_end:

        child_end = (
            cursor
            +
            timedelta(
                minutes=15
            )
        )

        expected_intervals.add(
            (
                to_iso(
                    cursor
                ),
                to_iso(
                    child_end
                ),
            )
        )

        cursor = child_end


    missing = (
        expected_intervals
        -
        actual_intervals
    )


    if missing:

        print(
            "本小时没有完整4个实时15分钟快照，"
            "跳过小时校正。"
        )

        print(
            "缺少区间：",
            len(
                missing
            )
        )

        print(
            "Bitquery请求：0"
        )

        conn.close()

        return


    # ========================================================
    # 当前Alpha
    # ========================================================

    token_ids, by_id = (
        load_tokens(
            conn
        )
    )


    # ========================================================
    # 读取这一小时原实时快照
    # ========================================================

    provisional_first_rows = (
        conn.execute(
            """
            SELECT

                token_key,
                wallet,
                MIN(first_buy_time)

            FROM demand2_recent_first_buy_v2

            WHERE
                interval_start >= ?
                AND interval_end <= ?

            GROUP BY
                token_key,
                wallet
            """,
            (
                hour_start_text,
                hour_end_text,
            ),
        ).fetchall()
    )


    provisional_first = {

        (
            int(
                token_key
            ),
            wallet,
        ):
            first_time

        for (
            token_key,
            wallet,
            first_time,
        )
        in provisional_first_rows
    }


    provisional_flow_rows = (
        conn.execute(
            """
            SELECT

                token_key,

                SUM(
                    buy_usd
                ),

                SUM(
                    sell_usd
                )

            FROM demand2_recent_flow_v2

            WHERE
                interval_start >= ?
                AND interval_end <= ?

            GROUP BY token_key
            """,
            (
                hour_start_text,
                hour_end_text,
            ),
        ).fetchall()
    )


    provisional_flow = {

        int(
            token_key
        ):
        (
            float(
                buy_usd
                or 0
            ),

            float(
                sell_usd
                or 0
            ),
        )

        for (
            token_key,
            buy_usd,
            sell_usd,
        )
        in provisional_flow_rows
    }


    # ========================================================
    # Bitquery重新查询上一完整小时
    # ========================================================

    (
        corrected_first,
        corrected_flow,
        request_count,
    ) = query_corrected_hour(
        conn,
        token_ids,
        by_id,
        hour_start,
        hour_end,
    )


    corrected_keys = set(
        corrected_first
    )

    provisional_keys = set(
        provisional_first
    )


    added_keys = (
        corrected_keys
        -
        provisional_keys
    )


    removed_keys = (
        provisional_keys
        -
        corrected_keys
    )


    common_keys = (
        corrected_keys
        &
        provisional_keys
    )


    changed_time_keys = {

        key

        for key in common_keys

        if (
            corrected_first[
                key
            ]
            !=
            provisional_first[
                key
            ]
        )
    }


    # ========================================================
    # 资金差额
    # ========================================================

    flow_token_keys = (
        set(
            provisional_flow
        )
        |
        set(
            corrected_flow
        )
    )


    flow_deltas = []


    for token_key in (
        flow_token_keys
    ):

        old_buy, old_sell = (
            provisional_flow.get(
                token_key,
                (
                    0.0,
                    0.0,
                ),
            )
        )

        new_buy, new_sell = (
            corrected_flow.get(
                token_key,
                (
                    0.0,
                    0.0,
                ),
            )
        )

        delta_buy = (
            new_buy
            -
            old_buy
        )

        delta_sell = (
            new_sell
            -
            old_sell
        )

        delta_net = (
            delta_buy
            -
            delta_sell
        )


        if (
            abs(
                delta_buy
            )
            >
            1e-9

            or

            abs(
                delta_sell
            )
            >
            1e-9
        ):

            flow_deltas.append(
                (
                    token_key,
                    delta_buy,
                    delta_sell,
                    delta_net,
                )
            )


    total_buy_delta = sum(
        row[1]
        for row in flow_deltas
    )

    total_sell_delta = sum(
        row[2]
        for row in flow_deltas
    )

    total_net_delta = (
        total_buy_delta
        -
        total_sell_delta
    )


    updated_at = to_iso(
        utc_now()
    )


    date_text = (
        hour_start
        .date()
        .isoformat()
    )


    last_finalized_date = (
        get_meta(
            conn,
            "last_finalized_date",
        )
    )


    # ========================================================
    # 开始一个SQLite事务统一修正
    # ========================================================

    try:

        conn.execute(
            "BEGIN"
        )


        # ====================================================
        # A. 钱包校正
        #
        # corrected_first里存在的Token+Wallet：
        #
        # 如果数据库里原来有更早历史，
        # 更早历史绝不能被覆盖。
        #
        # 如果数据库里的时间就在当前校正小时，
        # 则允许把时间向前或向后修正。
        # ====================================================

        for (
            token_key,
            wallet,
        ), first_time in (
            corrected_first.items()
        ):

            conn.execute(
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
                                wallet_token_first_buy_v2.first_buy_time
                                < ?

                            THEN
                                wallet_token_first_buy_v2.first_buy_time

                            ELSE
                                excluded.first_buy_time

                        END,

                    updated_at =
                        excluded.updated_at
                """,
                (
                    token_key,
                    wallet,
                    first_time,
                    updated_at,
                    hour_start_text,
                ),
            )


        # ====================================================
        # Bitquery校正后消失的钱包组合
        #
        # 只允许修改“首次时间就在当前小时”的记录。
        #
        # 如果这个钱包在更早历史已经出现，
        # 永远不能删除。
        #
        # 如果它在下一小时已经再次买入，
        # 把首次时间改成下一次看到的时间，
        # 而不是直接删除。
        # ====================================================

        for (
            token_key,
            wallet,
        ) in removed_keys:

            existing = conn.execute(
                """
                SELECT first_buy_time

                FROM wallet_token_first_buy_v2

                WHERE
                    token_key = ?
                    AND wallet = ?
                """,
                (
                    token_key,
                    wallet,
                ),
            ).fetchone()


            if not existing:
                continue


            existing_time = (
                existing[0]
            )


            # 更早的真实历史不能碰。
            if (
                existing_time
                <
                hour_start_text
            ):
                continue


            # 已经是这个小时之后看到的，
            # 也不属于本次撤回范围。
            if (
                existing_time
                >=
                hour_end_text
            ):
                continue


            later = conn.execute(
                """
                SELECT MIN(first_buy_time)

                FROM demand2_recent_first_buy_v2

                WHERE
                    token_key = ?
                    AND wallet = ?
                    AND interval_start >= ?
                """,
                (
                    token_key,
                    wallet,
                    hour_end_text,
                ),
            ).fetchone()


            later_time = (
                later[0]
                if later
                else None
            )


            if later_time:

                conn.execute(
                    """
                    UPDATE wallet_token_first_buy_v2

                    SET
                        first_buy_time = ?,
                        updated_at = ?

                    WHERE
                        token_key = ?
                        AND wallet = ?
                    """,
                    (
                        later_time,
                        updated_at,
                        token_key,
                        wallet,
                    ),
                )

            else:

                conn.execute(
                    """
                    DELETE FROM wallet_token_first_buy_v2

                    WHERE
                        token_key = ?
                        AND wallet = ?
                        AND first_buy_time >= ?
                        AND first_buy_time < ?
                    """,
                    (
                        token_key,
                        wallet,
                        hour_start_text,
                        hour_end_text,
                    ),
                )


        # ====================================================
        # B. 资金校正
        #
        # 普通小时：
        # 修正 demand2_today_flow_v2
        #
        # 如果上一自然日已经跨日固化：
        # 直接修正 daily_fund_flow
        # ====================================================

        use_daily_fund_flow = (
            last_finalized_date
            is not None
            and
            date_text
            <=
            last_finalized_date
        )


        if use_daily_fund_flow:

            registry_rows = (
                conn.execute(
                    """
                    SELECT

                        token_key,
                        symbol,
                        chain,
                        contract_address

                    FROM alpha_token_registry
                    """
                ).fetchall()
            )


            registry_map = {

                int(
                    token_key
                ):
                (
                    symbol,
                    chain,
                    contract_address,
                )

                for (
                    token_key,
                    symbol,
                    chain,
                    contract_address,
                )
                in registry_rows
            }


            for (
                token_key,
                delta_buy,
                delta_sell,
                delta_net,
            ) in flow_deltas:

                token_info = (
                    registry_map.get(
                        token_key
                    )
                )

                if not token_info:
                    continue

                (
                    symbol,
                    chain,
                    contract_address,
                ) = token_info


                conn.execute(
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

                        buy_usd =
                            daily_fund_flow.buy_usd
                            +
                            excluded.buy_usd,

                        sell_usd =
                            daily_fund_flow.sell_usd
                            +
                            excluded.sell_usd,

                        netflow_usd =
                            daily_fund_flow.netflow_usd
                            +
                            excluded.netflow_usd,

                        updated_at =
                            excluded.updated_at
                    """,
                    (
                        date_text,
                        symbol,
                        chain,
                        contract_address,
                        delta_buy,
                        delta_sell,
                        delta_net,
                        updated_at,
                    ),
                )


        else:

            for (
                token_key,
                delta_buy,
                delta_sell,
                delta_net,
            ) in flow_deltas:

                conn.execute(
                    """
                    INSERT INTO demand2_today_flow_v2 (

                        date,
                        token_key,
                        buy_usd,
                        sell_usd,
                        netflow_usd,
                        updated_at

                    )

                    VALUES (
                        ?, ?, ?, ?, ?, ?
                    )

                    ON CONFLICT(
                        date,
                        token_key
                    )

                    DO UPDATE SET

                        buy_usd =
                            demand2_today_flow_v2.buy_usd
                            +
                            excluded.buy_usd,

                        sell_usd =
                            demand2_today_flow_v2.sell_usd
                            +
                            excluded.sell_usd,

                        netflow_usd =
                            demand2_today_flow_v2.netflow_usd
                            +
                            excluded.netflow_usd,

                        updated_at =
                            excluded.updated_at
                    """,
                    (
                        date_text,
                        token_key,
                        delta_buy,
                        delta_sell,
                        delta_net,
                        updated_at,
                    ),
                )


        # ====================================================
        # C. 当前小时已经校正完成。
        #
        # 删除它以及更早的15分钟临时快照。
        # 下一小时的快照继续保留。
        # ====================================================

        conn.execute(
            """
            DELETE FROM demand2_recent_first_buy_v2
            WHERE interval_start < ?
            """,
            (
                hour_end_text,
            ),
        )

        conn.execute(
            """
            DELETE FROM demand2_recent_flow_v2
            WHERE interval_start < ?
            """,
            (
                hour_end_text,
            ),
        )

        conn.execute(
            """
            DELETE FROM demand2_recent_interval_v2
            WHERE interval_start < ?
            """,
            (
                hour_end_text,
            ),
        )


        set_meta(
            conn,
            "last_hourly_correction_end",
            hour_end_text,
        )


        conn.commit()


    except Exception:

        conn.rollback()

        raise


    # ========================================================
    # 输出
    # ========================================================

    print()

    print(
        "=" * 72
    )

    print(
        "✅ 小时校正完成"
    )

    print(
        "=" * 72
    )


    print(
        "原实时钱包组合：",
        len(
            provisional_first
        )
    )

    print(
        "校正后钱包组合：",
        len(
            corrected_first
        )
    )

    print(
        "新增组合：",
        len(
            added_keys
        )
    )

    print(
        "撤回组合：",
        len(
            removed_keys
        )
    )

    print(
        "时间修正：",
        len(
            changed_time_keys
        )
    )


    print(
        "资金变化Token：",
        len(
            flow_deltas
        )
    )

    print(
        "买入修正：",
        f"${total_buy_delta:+,.2f}"
    )

    print(
        "卖出修正：",
        f"${total_sell_delta:+,.2f}"
    )

    print(
        "净流入修正：",
        f"${total_net_delta:+,.2f}"
    )

    print(
        "Bitquery小时校正请求：",
        request_count,
        "次"
    )


    conn.close()


if __name__ == "__main__":

    main()
