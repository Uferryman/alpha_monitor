# 导入 os，用于读取环境变量
import os

# 导入 requests，用于访问 Bitquery
import requests

# 导入 dotenv，用于读取 .env 文件
from dotenv import load_dotenv


# 加载当前目录的 .env 文件
load_dotenv()

# 从 .env 文件读取 Bitquery Token
token = os.getenv("BITQUERY_TOKEN")

# 如果没有读取到 Token，就直接提示错误
if not token:
    print("❌ 没有读取到 BITQUERY_TOKEN")
    exit()


# Bitquery API 地址
url = "https://streaming.bitquery.io/graphql"


# 先执行一个非常简单的 BSC 查询
query = """
query {
  EVM(network: bsc) {
    Blocks(limit: {count: 1}) {
      Block {
        Number
        Time
      }
    }
  }
}
"""


# 设置请求头
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}


# 告诉自己程序开始请求
print("正在连接 Bitquery...")


# 向 Bitquery 发送请求
response = requests.post(
    url,
    headers=headers,
    json={"query": query},
    timeout=30,
)


# 打印 HTTP 状态码
print("HTTP 状态码：", response.status_code)


# 打印 Bitquery 返回的内容
print(response.text)
