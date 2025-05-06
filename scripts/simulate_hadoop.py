#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script mô phỏng việc tải dữ liệu lên HDFS và tạo bảng Hive
"""

import os
import shutil
import time
import pandas as pd
import sqlite3
from datetime import datetime

# Các thông số cấu hình
INPUT_FILE = "pandemic_data.csv"
HDFS_DIR = "hdfs_simulation"
HIVE_DB_FILE = "hive_simulation.db"

def print_with_timestamp(message):
    """In thông báo kèm timestamp"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def simulate_hdfs_upload():
    """Mô phỏng việc tải file lên HDFS"""
    print_with_timestamp("===== Mô phỏng việc tải file lên HDFS =====")
    
    # Kiểm tra file đầu vào tồn tại
    if not os.path.exists(INPUT_FILE):
        print_with_timestamp(f"Lỗi: Không tìm thấy file dữ liệu {INPUT_FILE}")
        return False
    
    # Tạo thư mục mô phỏng HDFS
    if not os.path.exists(HDFS_DIR):
        os.makedirs(HDFS_DIR)
        print_with_timestamp(f"Đã tạo thư mục {HDFS_DIR} để mô phỏng HDFS")
    
    # Tạo thư mục con cho dữ liệu
    hdfs_data_dir = os.path.join(HDFS_DIR, "user", "pandemic_data")
    if not os.path.exists(hdfs_data_dir):
        os.makedirs(hdfs_data_dir)
        print_with_timestamp(f"Đã tạo thư mục {hdfs_data_dir}")
    
    # Mô phỏng lệnh upload file lên HDFS
    print_with_timestamp(f"Đang tải file {INPUT_FILE} lên HDFS...")
    
    # Mô phỏng độ trễ của việc upload file lớn
    file_size_mb = os.path.getsize(INPUT_FILE) / (1024 * 1024)
    estimated_time = min(file_size_mb * 0.05, 2)  # Giả lập thời gian upload
    
    # Hiển thị thanh tiến trình
    for i in range(10):
        progress = (i + 1) * 10
        print(f"\rTiến trình: [{progress}%] [{'=' * (i + 1)}{' ' * (9 - i)}]", end="")
        time.sleep(estimated_time / 10)
    print("\r", end="")
    
    # Copy file vào thư mục mô phỏng HDFS
    destination = os.path.join(hdfs_data_dir, INPUT_FILE)
    shutil.copy2(INPUT_FILE, destination)
    
    print_with_timestamp(f"Đã tải file lên thành công: {destination}")
    print_with_timestamp(f"Kích thước file: {file_size_mb:.2f} MB")
    
    # Hiển thị lệnh giả lập
    print("\nLệnh tương đương trên HDFS thực tế:")
    print(f"hdfs dfs -mkdir -p /user/pandemic_data")
    print(f"hdfs dfs -put {INPUT_FILE} /user/pandemic_data/")
    
    return True

def create_hive_table():
    """Mô phỏng việc tạo bảng Hive bằng SQLite"""
    print_with_timestamp("\n===== Mô phỏng việc tạo bảng Hive =====")
    
    # Kết nối đến SQLite
    conn = sqlite3.connect(HIVE_DB_FILE)
    cursor = conn.cursor()
    
    # Xóa bảng cũ nếu tồn tại
    cursor.execute("DROP TABLE IF EXISTS pandemic_data")
    
    # Tạo bảng mới
    create_table_sql = """
    CREATE TABLE pandemic_data (
        date TEXT,
        country TEXT,
        continent TEXT,
        new_cases INTEGER,
        new_deaths INTEGER,
        new_recovered INTEGER,
        total_cases INTEGER,
        total_deaths INTEGER,
        total_recovered INTEGER
    )
    """
    cursor.execute(create_table_sql)
    print_with_timestamp("Đã tạo bảng pandemic_data trong Hive (mô phỏng SQLite)")
    
    # Hiển thị lệnh Hive tương đương
    print("\nLệnh tương đương trên Hive thực tế:")
    print("""CREATE EXTERNAL TABLE pandemic_data (
    date STRING,
    country STRING,
    continent STRING,
    new_cases INT,
    new_deaths INT,
    new_recovered INT,
    total_cases INT,
    total_deaths INT,
    total_recovered INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/pandemic_data/'
TBLPROPERTIES ("skip.header.line.count"="1");""")
    
    # Đọc dữ liệu từ CSV và đưa vào SQLite
    print_with_timestamp(f"Đang đọc dữ liệu từ {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Hiển thị thông tin về DataFrame
    print_with_timestamp(f"Số dòng dữ liệu: {len(df)}")
    print_with_timestamp(f"Các cột: {', '.join(df.columns)}")
    
    # Đưa dữ liệu vào SQLite
    print_with_timestamp("Đang import dữ liệu vào bảng...")
    df.to_sql('pandemic_data', conn, if_exists='replace', index=False)
    
    # Kiểm tra dữ liệu đã được import
    cursor.execute("SELECT COUNT(*) FROM pandemic_data")
    count = cursor.fetchone()[0]
    print_with_timestamp(f"Đã import {count:,} dòng dữ liệu vào bảng pandemic_data")
    
    # Hiển thị một số dòng đầu tiên
    cursor.execute("SELECT * FROM pandemic_data LIMIT 5")
    rows = cursor.fetchall()
    
    print_with_timestamp("Mẫu dữ liệu (5 dòng đầu tiên):")
    for row in rows:
        print(row)
    
    # Đóng kết nối
    conn.close()
    
    return True

def main():
    """Hàm chính thực hiện mô phỏng"""
    print_with_timestamp("===== BẮT ĐẦU MÔ PHỎNG HDFS VÀ HIVE =====")
    
    # Mô phỏng tải lên HDFS
    if simulate_hdfs_upload():
        # Mô phỏng tạo bảng Hive
        create_hive_table()
    
    print_with_timestamp("===== KẾT THÚC MÔ PHỎNG =====")
    print_with_timestamp("Ghi chú: Đây chỉ là mô phỏng, không phải môi trường Hadoop/Hive thực tế.")
    print_with_timestamp("Các file mô phỏng đã được tạo trong các thư mục:")
    print(f"  - HDFS: {os.path.abspath(HDFS_DIR)}")
    print(f"  - Hive: {os.path.abspath(HIVE_DB_FILE)}")

if __name__ == "__main__":
    main() 