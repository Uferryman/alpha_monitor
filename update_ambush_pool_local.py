# -*- coding: utf-8 -*-

# ============================================================
# 需求一本地更新入口
#
# 数据来源：
# daily_fund_flow
#
# Bitquery请求：
# 0
#
# 流程：
# 1. 调用已经验证通过的本地重算程序
# 2. 只有重算成功，才替换正式埋伏池CSV
#
# 如果重算失败：
# 正式 alpha_ambush_pool.csv 保持原样
# ============================================================

import os
import subprocess
import sys


PYTHON = sys.executable

TEST_FILE = "alpha_ambush_pool_v2_test.csv"
PRODUCTION_FILE = "alpha_ambush_pool.csv"


print("=" * 80)
print("需求一本地更新")
print("=" * 80)

print()
print("① 使用 daily_fund_flow 重算最近30个完整UTC自然日")
print()


# ============================================================
# 重算失败会直接停止，
# 不会碰正式CSV。
# ============================================================

subprocess.run(
    [
        PYTHON,
        "rebuild_ambush_pool_from_demand2.py",
    ],
    check=True,
)


# ============================================================
# 确认测试CSV已经生成
# ============================================================

if not os.path.exists(
    TEST_FILE
):

    raise RuntimeError(
        f"没有生成 {TEST_FILE}，"
        "正式埋伏池未修改。"
    )


# ============================================================
# 原子替换正式CSV
#
# os.replace：
# 成功就是完整新文件，
# 不会出现写到一半的CSV。
# ============================================================

os.replace(
    TEST_FILE,
    PRODUCTION_FILE,
)


print()
print("=" * 80)
print("✅ 正式埋伏池已更新")
print("=" * 80)

print(
    "文件：",
    PRODUCTION_FILE
)

print()
print("Bitquery请求：0")
print("Points消耗：0")
