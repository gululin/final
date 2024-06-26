import requests as r
import pandas as pd
import pyodbc

#期末專題影片連結"https://drive.google.com/file/d/1kyvmbmzTFuq54CiresQh9WKoRn_O9glE/view?usp=drivesdk"


# 要抓資料的網站
url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20240201&stockNo=2330&response=json&_=1711447677331"

# 獲取網站數據
res = r.get(url)

# 轉換網站數據格式轉為python格式
stock_json = res.json()

# 從數據中創建資料表
stock_df = pd.DataFrame.from_dict(stock_json['data'])

# 設置列名
stock_df.columns = stock_json["fields"]

# 選擇指定的欄位並打印
selected_columns = ['日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
formatted_df = stock_df[selected_columns]

# 打印對齊的表格
print(formatted_df.to_markdown(index=False))

# 連接到SQL Server
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=LAPTOP-H73TTGJC\SQLSERVER2022;DATABASE=MySchoolIBD;UID=sa;PWD=gg1294887'

# 建立連接
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# 創建表格（如果不存在）
create_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='StockData' AND xtype='U')
CREATE TABLE StockData (
    日期 VARCHAR(50),
    成交股數 VARCHAR(50),
    成交金額 VARCHAR(50),
    開盤價 VARCHAR(50),
    最高價 VARCHAR(50),
    最低價 VARCHAR(50),
    收盤價 VARCHAR(50),
    漲跌價差 VARCHAR(50),
    成交筆數 VARCHAR(50)
)
"""
cursor.execute(create_table_query)

# 插入數據到SQL Server
for index, row in stock_df[selected_columns].iterrows():
    insert_query = """
    INSERT INTO StockData (日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, row['日期'], row['成交股數'], row['成交金額'], row['開盤價'], row['最高價'], row['最低價'], row['收盤價'], row['漲跌價差'], row['成交筆數'])

# 更改保存
conn.commit()
cursor.close()
conn.close()


