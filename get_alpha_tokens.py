# 导入 csv，用于保存 Alpha 代币清单
import csv

# 导入 requests，用于请求 Binance API
import requests

# 导入 datetime，用于记录更新时间
from datetime import datetime


# ============================================================
# 1. Binance Alpha 官方接口
# ============================================================

# Binance Alpha Token List
ALPHA_URL = (
    "https://www.binance.com/"
    "bapi/defi/v1/public/wallet-direct/buw/wallet/"
    "cex/alpha/all/token/list"
)


# ============================================================
# 2. HTTP 请求头
# ============================================================

# 模拟普通浏览器访问
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


# ============================================================
# 3. 判断 Binance 返回的值是否为 true
# ============================================================

def is_true(value):

    # 如果本身就是 Python 的 True
    if value is True:
        return True

    # 如果 Binance 返回的是字符串 "true"
    if isinstance(value, str):
        return value.lower() == "true"

    # 其他情况都认为不是 True
    return False


# ============================================================
# 4. 开始获取 Binance Alpha Token
# ============================================================

print("正在获取 Binance Alpha 代币列表...")


try:

    # 请求 Binance Alpha API
    response = requests.get(
        ALPHA_URL,
        headers=headers,
        timeout=30,
    )

    # 如果 HTTP 请求失败，直接报错
    response.raise_for_status()

    # 将返回内容转换成 JSON
    result = response.json()


    # ========================================================
    # 5. 检查 Binance 接口是否成功
    # ========================================================

    # success 正常情况下应该为 True
    if not result.get("success"):

        # 打印错误
        print("❌ Binance Alpha API 返回失败")

        # 打印 Binance 原始返回结果
        print(result)

        # 停止程序
        exit()


    # 获取 Binance 返回的全部 Alpha Token
    alpha_tokens = result.get("data", [])


    # ========================================================
    # 6. 准备有效列表和排除列表
    # ========================================================

    # 最终有效 Alpha
    active_tokens = []

    # 被排除的 Alpha
    excluded_tokens = []

    # 用来防止重复合约
    seen_tokens = set()


    # ========================================================
    # 7. 开始逐个筛选
    # ========================================================

    for token in alpha_tokens:

        # 获取 Alpha ID
        alpha_id = str(
            token.get("alphaId", "")
        ).strip()

        # 获取链 ID
        chain_id = str(
            token.get("chainId", "")
        ).strip()

        # 获取合约地址
        contract_address = str(
            token.get("contractAddress", "")
        ).strip()

        # 合约地址统一转成小写
        contract_lower = contract_address.lower()

        # 获取是否下线
        offline = token.get("offline")

        # 获取是否停止卖出
        offsell = token.get("offsell")

        # 获取 Binance 股票/证券标记
        stock_state = token.get("stockState")


        # ====================================================
        # 8. 判断排除原因
        # ====================================================

        # 默认不排除
        exclude_reason = ""


        # 没有 Alpha ID
        if not alpha_id:

            exclude_reason = "no_alpha_id"


        # 没有合约地址
        elif not contract_address:

            exclude_reason = "no_contract"


        # Binance 已标记下线
        elif is_true(offline):

            exclude_reason = "offline"


        # Binance 已停止卖出
        elif is_true(offsell):

            exclude_reason = "offsell"


        # Binance 标记为股票/证券代币
        elif is_true(stock_state):

            exclude_reason = "stock"


        # ====================================================
        # 9. 如果需要排除
        # ====================================================

        if exclude_reason:

            # 复制 Token 数据
            excluded_token = token.copy()

            # 写入排除原因
            excluded_token["excludeReason"] = exclude_reason

            # 加入排除列表
            excluded_tokens.append(excluded_token)

            # 不继续处理
            continue


        # ====================================================
        # 10. 对有效 Alpha 去重
        # ====================================================

        # 使用链 ID + 合约地址作为唯一键
        token_key = (
            chain_id,
            contract_lower,
        )

        # 如果已经存在，就跳过
        if token_key in seen_tokens:
            continue

        # 记录这个 Token
        seen_tokens.add(token_key)

        # 加入有效 Alpha
        active_tokens.append(token)


    # ========================================================
    # 11. CSV 保存字段
    # ========================================================

    # 这里只保存后面真正可能用得到的字段
    fields = [
        "symbol",
        "name",
        "alphaId",
        "chainName",
        "chainId",
        "contractAddress",
        "listingTime",
        "offline",
        "offsell",
        "stockState",
    ]


    # ========================================================
    # 12. 保存全部 Alpha
    # ========================================================

    with open(
        "alpha_tokens_all.csv",
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        # 创建 CSV 写入器
        writer = csv.DictWriter(
            file,
            fieldnames=fields,
            extrasaction="ignore",
        )

        # 写入表头
        writer.writeheader()

        # 写入所有 Alpha
        for token in alpha_tokens:
            writer.writerow(token)


    # ========================================================
    # 13. 保存有效 Alpha
    # ========================================================

    with open(
        "alpha_tokens_active.csv",
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        # 创建 CSV 写入器
        writer = csv.DictWriter(
            file,
            fieldnames=fields,
            extrasaction="ignore",
        )

        # 写入表头
        writer.writeheader()

        # 写入有效 Alpha
        for token in active_tokens:
            writer.writerow(token)


    # ========================================================
    # 14. 保存被排除 Alpha
    # ========================================================

    # 排除表多一列排除原因
    excluded_fields = fields + [
        "excludeReason"
    ]

    with open(
        "alpha_tokens_excluded.csv",
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        # 创建 CSV 写入器
        writer = csv.DictWriter(
            file,
            fieldnames=excluded_fields,
            extrasaction="ignore",
        )

        # 写入表头
        writer.writeheader()

        # 写入被排除 Token
        for token in excluded_tokens:
            writer.writerow(token)


    # ========================================================
    # 15. 统计排除原因
    # ========================================================

    # 创建排除原因统计
    reason_counts = {}

    # 遍历被排除 Token
    for token in excluded_tokens:

        # 获取排除原因
        reason = token.get(
            "excludeReason",
            "unknown",
        )

        # 数量 +1
        reason_counts[reason] = (
            reason_counts.get(reason, 0)
            + 1
        )


    # ========================================================
    # 16. 统计各链 Alpha 数量
    # ========================================================

    # 创建链统计字典
    chain_counts = {}

    # 遍历有效 Alpha
    for token in active_tokens:

        # 获取链名称
        chain_name = token.get(
            "chainName",
            "UNKNOWN",
        )

        # 数量 +1
        chain_counts[chain_name] = (
            chain_counts.get(chain_name, 0)
            + 1
        )


    # ========================================================
    # 17. 输出最终结果
    # ========================================================

    print("\n" + "=" * 60)

    print("Binance Alpha 清单处理完成")

    print("=" * 60)

    # 原始 Alpha 数量
    print(
        f"Alpha 接口总数：    "
        f"{len(alpha_tokens)}"
    )

    # 被排除数量
    print(
        f"排除数量：          "
        f"{len(excluded_tokens)}"
    )

    # 最终有效数量
    print(
        f"最终有效 Alpha：    "
        f"{len(active_tokens)}"
    )

    # 更新时间
    print(
        "更新时间：          "
        + datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


    # ========================================================
    # 18. 打印排除原因
    # ========================================================

    print("\n排除原因：")

    for reason, count in sorted(
        reason_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    ):

        print(
            f"  {reason:<15}"
            f"{count}"
        )


    # ========================================================
    # 19. 打印有效 Alpha 链分布
    # ========================================================

    print("\n有效 Alpha 链分布：")

    for chain_name, count in sorted(
        chain_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    ):

        print(
            f"  {chain_name:<15}"
            f"{count}"
        )


    # ========================================================
    # 20. 打印被排除的证券代币
    # ========================================================

    print("\n被排除的股票/证券 Alpha：")

    # 遍历排除列表
    for token in excluded_tokens:

        # 只显示 stock
        if token.get("excludeReason") == "stock":

            print(
                f"  "
                f"{token.get('symbol', ''):<15}"
                f"{token.get('chainName', ''):<12}"
                f"{token.get('contractAddress', '')}"
            )


    # ========================================================
    # 21. 文件提示
    # ========================================================

    print("\n生成文件：")

    print("✅ alpha_tokens_all.csv")

    print("✅ alpha_tokens_active.csv")

    print("✅ alpha_tokens_excluded.csv")


# ============================================================
# 22. 网络错误
# ============================================================

except requests.exceptions.RequestException as error:

    print("\n❌ Binance API 请求失败：")

    print(error)


# ============================================================
# 23. 其他错误
# ============================================================

except Exception as error:

    print("\n❌ 程序运行失败：")

    print(error)
