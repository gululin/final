import pandas as pd
import pyodbc
from flask import Flask, render_template
import requests as r

#期末報告影片網址https://drive.google.com/file/d/1kX4LwGS_SAUCTsXaViKxtuQ6lzmRs_rz/view?usp=drivesdk

app = Flask(__name__)

# 連接到 SQL Server
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=LAPTOP-H73TTGJC\\SQLSERVER2022;DATABASE=MySchoolIBD;UID=sa;PWD=gg1294887'

# 定義函式從網站抓取資料並存入 SQL Server
def fetch_and_store_stock_data():
    # 要抓資料的網站
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20240201&stockNo=2330&response=json&_=1711447677331"
    
    # 獲取網站數據
    res = r.get(url)
    
    # 轉換為 JSON 格式
    stock_json = res.json()
    
    # 從數據中創建 DataFrame
    stock_df = pd.DataFrame.from_dict(stock_json['data'])
    
    # 設置列名
    stock_df.columns = stock_json["fields"]
    
    # 連接到 SQL Server
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
    
    # 插入數據到 SQL Server
    for index, row in stock_df.iterrows():
        insert_query = """
        INSERT INTO StockData (日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, row['日期'], row['成交股數'], row['成交金額'], row['開盤價'], row['最高價'], row['最低價'], row['收盤價'], row['漲跌價差'], row['成交筆數'])
    
    # 提交更改並關閉連接
    conn.commit()
    cursor.close()
    conn.close()

# Flask 路由
@app.route('/')
def index():
    # 呼叫函式抓取資料並存入 SQL Server
    fetch_and_store_stock_data()
    
    # 連接 SQL Server 取得資料
    conn = pyodbc.connect(connection_string)
    query = "SELECT * FROM StockData"
    stock_df = pd.read_sql(query, conn)
    conn.close()
    
    # 選擇指定的欄位並打印
    selected_columns = ['日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
    formatted_df = stock_df[selected_columns]
    
    # 將數據傳遞給模板
    return render_template('index.html', tables=[formatted_df.to_html(classes='data', index=False).replace('\n', '')], titles=formatted_df.columns.values)

if __name__ == '__main__':
    app.run(debug=True)


