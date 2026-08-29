# 导入 os，用于读取 .env 文件中的 Bitquery Token
import os

# 导入 requests，用于向 Bitquery 发送 HTTP 请求
import requests

# 导入日期相关工具，用于自动计算最近 30 个完整自然日
from datetime import datetime, timedelta, timezone

# 导入 dotenv，用于读取 .env 文件
from dotenv import load_dotenv


# ============================================================
# 1. 基础设置
# ============================================================

# 加载当前目录下的 .env 文件
load_dotenv()

# 从 .env 中读取 Bitquery Token
BITQUERY_TOKEN = os.getenv("BITQUERY_TOKEN")

# 如果没有读取到 Token，就停止程序
if not BITQUERY_TOKEN:
    raise ValueError("没有读取到 BITQUERY_TOKEN，请检查 .env 文件")


# Bitquery GraphQL API 地址
BITQUERY_URL = "https://streaming.bitquery.io/graphql"


# 当前测试的代币名称
TOKEN_NAME = "UB"

# UB 在 BSC 上的合约地址
TOKEN_ADDRESS = "0x40b8129b786d766267a7a118cf8c07e31cdb6fde"

# 转成小写
# Bitquery 对 EVM 地址建议使用全小写格式
TOKEN_ADDRESS = TOKEN_ADDRESS.lower()


# ============================================================
# 2. 自动计算最近 30 个完整 UTC 自然日
# ============================================================

# 获取当前 UTC 时间
now_utc = datetime.now(timezone.utc)

# 得到今天 UTC 00:00:00
today_utc = datetime(
    year=now_utc.year,
    month=now_utc.month,
    day=now_utc.day,
    tzinfo=timezone.utc,
)

# 查询截止时间设置为今天 00:00
# 这样不会把“今天还没走完的数据”算进去
end_time = today_utc

# 从截止时间往前推 30 天
start_time = end_time - timedelta(days=30)

# 转成 Bitquery 接受的 ISO 时间格式
start_time_str = start_time.isoformat().replace("+00:00", "Z")

# 转成 Bitquery 接受的 ISO 时间格式
end_time_str = end_time.isoformat().replace("+00:00", "Z")


# ============================================================
# 3. GraphQL 查询
# ============================================================

# 这条查询做的事情：
#
# 1. 只查询 BSC
# 2. 只查询 UB
# 3. 查询最近 30 个完整自然日
# 4. 按天分组
# 5. 计算每天买入 USD
# 6. 计算每天卖出 USD
#
QUERY = """
query DailyNetflow(
  $token: String!
  $since: DateTime!
  $till: DateTime!
) {
  Trading {
    Trades(
      where: {
        Block: {
          Time: {
            since: $since
            till: $till
          }
        }
        Pair: {
          Token: {
            Address: {
              is: $token
            }
          }
          Market: {
            NetworkBid: {
              is: "bid:bsc"
            }
          }
        }
      }
      orderBy: {
        ascending: Block_Date
      }
    ) {
      Block {
        Date
      }

      buy_usd: sum(
        of: AmountsInUsd_Base
        if: {
          Side: {
            is: "Buy"
          }
        }
      )

      sell_usd: sum(
        of: AmountsInUsd_Base
        if: {
          Side: {
            is: "Sell"
          }
        }
      )

      trades: count
    }
  }
}
"""


# ============================================================
# 4. GraphQL 参数
# ============================================================

# 把代币地址和时间传给 GraphQL
variables = {
    "token": TOKEN_ADDRESS,
    "since": start_time_str,
    "till": end_time_str,
}


# ============================================================
# 5. 请求头
# ============================================================

# 设置 HTTP 请求头
headers = {
    # 告诉 Bitquery 我们发送的是 JSON
    "Content-Type": "application/json",

    # 使用 .env 中的 Token 登录 Bitquery
    "Authorization": f"Bearer {BITQUERY_TOKEN}",
}


# ============================================================
# 6. 开始请求 Bitquery
# ============================================================

# 打印当前查询信息
print("=" * 70)

# 打印代币名称
print(f"代币：{TOKEN_NAME}")

# 打印链
print("链：BSC")

# 打印合约地址
print(f"合约：{TOKEN_ADDRESS}")

# 打印查询日期范围
print(
    f"统计区间：{start_time.date()} 至 "
    f"{(end_time - timedelta(days=1)).date()}"
)

# 提醒这是完整自然日
print("口径：最近 30 个完整 UTC 自然日")

# 打印分隔线
print("=" * 70)

# 打印提示
print("\n正在查询 Bitquery，请稍候...\n")


try:

    # 向 Bitquery 发送 POST 请求
    response = requests.post(
        BITQUERY_URL,
        headers=headers,
        json={
            "query": QUERY,
            "variables": variables,
        },
        timeout=60,
    )

    # 打印 HTTP 状态码
    print("HTTP 状态码：", response.status_code)

    # 如果 HTTP 请求本身失败，就抛出异常
    response.raise_for_status()

    # 将返回结果转换成 Python 字典
    result = response.json()


    # ========================================================
    # 7. 检查 Bitquery 是否返回 GraphQL 错误
    # ========================================================

    # 如果返回内容中包含 errors
    if "errors" in result:

        # 打印错误提示
        print("\n❌ Bitquery 返回错误：")

        # 逐个打印错误
        for error in result["errors"]:
            print(error)

        # 退出程序
        exit()


    # ========================================================
    # 8. 获取 Bitquery 返回的每日数据
    # ========================================================

    # 取出每天的交易统计
    rows = result["data"]["Trading"]["Trades"]


    # ========================================================
    # 9. 创建完整的 30 天日期表
    # ========================================================

    # 创建一个空字典
    daily_data = {}

    # 循环生成 30 个日期
    for i in range(30):

        # 当前日期
        current_date = (start_time + timedelta(days=i)).date()

        # 转成字符串，例如 2026-08-01
        date_str = str(current_date)

        # 默认这一天没有交易
        daily_data[date_str] = {
            "buy_usd": 0.0,
            "sell_usd": 0.0,
            "trades": 0,
        }


    # ========================================================
    # 10. 把 Bitquery 数据填入日期表
    # ========================================================

    # 遍历 Bitquery 返回的数据
    for row in rows:

        # 获取日期
        date_str = row["Block"]["Date"]

        # 有时 Date 可能带时间，这里只保留 YYYY-MM-DD
        date_str = date_str[:10]

        # 如果日期属于我们最近 30 天
        if date_str in daily_data:

            # 获取买入金额
            buy_usd = float(row.get("buy_usd") or 0)

            # 获取卖出金额
            sell_usd = float(row.get("sell_usd") or 0)

            # 获取交易笔数
            trades = int(row.get("trades") or 0)

            # 保存数据
            daily_data[date_str] = {
                "buy_usd": buy_usd,
                "sell_usd": sell_usd,
                "trades": trades,
            }


    # ========================================================
    # 11. 计算每日净买入
    # ========================================================

    # 正净买入天数
    positive_days = 0

    # 30 天累计买入
    total_buy = 0.0

    # 30 天累计卖出
    total_sell = 0.0

    # 30 天累计净买入
    total_netflow = 0.0


    # 打印表头
    print("\n")

    print(
        f"{'日期':<12}"
        f"{'买入USD':>16}"
        f"{'卖出USD':>16}"
        f"{'净买入USD':>16}"
    )

    # 打印横线
    print("-" * 60)


    # 按日期顺序循环
    for date_str in sorted(daily_data.keys()):

        # 获取当天数据
        data = daily_data[date_str]

        # 获取当天买入金额
        buy_usd = data["buy_usd"]

        # 获取当天卖出金额
        sell_usd = data["sell_usd"]

        # 计算当天净买入
        netflow = buy_usd - sell_usd

        # 累计买入金额
        total_buy += buy_usd

        # 累计卖出金额
        total_sell += sell_usd

        # 累计净买入
        total_netflow += netflow

        # 如果当天净买入大于 0
        if netflow > 0:

            # 正净买入天数 +1
            positive_days += 1


        # 打印当天数据
        print(
            f"{date_str:<12}"
            f"${buy_usd:>15,.2f}"
            f"${sell_usd:>15,.2f}"
            f"${netflow:>+15,.2f}"
        )


    # ========================================================
    # 12. 计算 30 天埋伏池指标
    # ========================================================

    # 计算正净买入天数占比
    positive_ratio = positive_days / 30

    # 判断是否超过 65%
    ratio_pass = positive_ratio > 0.65

    # 判断 30 天累计净买入是否大于 0
    netflow_pass = total_netflow > 0

    # 同时满足两个条件才进入埋伏池
    ambush_pass = ratio_pass and netflow_pass


    # ========================================================
    # 13. 输出最终结果
    # ========================================================

    # 打印分隔线
    print("\n" + "=" * 70)

    # 打印标题
    print(f"{TOKEN_NAME} 最近 30 天统计")

    # 打印分隔线
    print("=" * 70)

    # 输出累计买入
    print(f"30天总买入：     ${total_buy:,.2f}")

    # 输出累计卖出
    print(f"30天总卖出：     ${total_sell:,.2f}")

    # 输出累计净买入
    print(f"30天总净买入：   ${total_netflow:+,.2f}")

    # 输出正净买入天数
    print(f"净买入 > 0：     {positive_days} 天")

    # 输出正净买入占比
    print(f"正净买入天数占比：{positive_ratio:.2%}")

    # 输出判断结果
    if ambush_pass:
        print("\n✅ 符合埋伏池条件")

    else:
        print("\n❌ 暂不符合埋伏池条件")

    # 输出具体原因
    print("\n判断条件：")

    # 输出第一个条件
    print(
        f"1. 正净买入天数占比 > 65%："
        f"{'✅' if ratio_pass else '❌'}"
    )

    # 输出第二个条件
    print(
        f"2. 30天累计净买入 > 0："
        f"{'✅' if netflow_pass else '❌'}"
    )


# ============================================================
# 14. 网络请求错误处理
# ============================================================

except requests.exceptions.RequestException as error:

    # 打印错误提示
    print("\n❌ 网络请求失败：")

    # 打印具体错误
    print(error)

