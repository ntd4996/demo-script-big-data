#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script mô phỏng các truy vấn phân tích trên dữ liệu dịch bệnh
"""

import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time

# Các thông số cấu hình
HIVE_DB_FILE = "hive_simulation.db"
VISUALIZATIONS_DIR = "visualizations"

def print_with_timestamp(message):
    """In thông báo kèm timestamp"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def execute_query(cursor, query_num, query, description):
    """Thực thi một truy vấn và hiển thị kết quả"""
    print_with_timestamp(f"\n===== Truy vấn {query_num}: {description} =====")
    print(f"SQL: {query}")
    
    # Mô phỏng thời gian thực thi truy vấn
    time.sleep(0.5)
    
    # Thực thi truy vấn
    start_time = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    end_time = time.time()
    
    # Hiển thị kết quả
    print_with_timestamp(f"Kết quả truy vấn (thời gian: {(end_time - start_time):.3f} giây):")
    
    if len(rows) > 0:
        # Lấy tên cột
        column_names = [desc[0] for desc in cursor.description]
        
        # Tạo DataFrame từ kết quả
        df = pd.DataFrame(rows, columns=column_names)
        
        # Hiển thị DataFrame
        print(df.head(10).to_string(index=False))
        
        if len(rows) > 10:
            print(f"... (còn {len(rows) - 10} dòng khác)")
            
        return df
    else:
        print("Không có kết quả nào.")
        return None

def visualize_results(query_num, df, visualization_type, title, xlabel, ylabel, filename):
    """Tạo và lưu biểu đồ từ kết quả truy vấn"""
    if df is None or len(df) == 0:
        return
    
    # Tạo thư mục nếu chưa tồn tại
    if not os.path.exists(VISUALIZATIONS_DIR):
        os.makedirs(VISUALIZATIONS_DIR)
    
    plt.figure(figsize=(12, 6))
    
    if visualization_type == 'bar':
        # Lấy tên cột đầu tiên và cuối cùng của DataFrame
        x_column = df.columns[0]
        y_column = df.columns[1]
        
        # Vẽ biểu đồ cột
        plt.bar(df[x_column], df[y_column])
        
    elif visualization_type == 'line':
        # Lấy tên cột đầu tiên và cuối cùng của DataFrame
        x_column = df.columns[0]
        y_column = df.columns[1]
        
        # Vẽ biểu đồ đường
        plt.plot(df[x_column], df[y_column], marker='o')
        
    elif visualization_type == 'pie':
        # Lấy tên cột đầu tiên và cuối cùng của DataFrame
        labels = df[df.columns[0]]
        sizes = df[df.columns[1]]
        
        # Vẽ biểu đồ tròn
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    
    # Xoay nhãn trục x nếu cần
    if visualization_type == 'bar' and len(df) > 5:
        plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    output_path = os.path.join(VISUALIZATIONS_DIR, filename)
    plt.savefig(output_path)
    plt.close()
    
    print_with_timestamp(f"Đã lưu biểu đồ: {output_path}")

def run_analysis():
    """Thực hiện các truy vấn phân tích"""
    # Kiểm tra file database tồn tại
    if not os.path.exists(HIVE_DB_FILE):
        print_with_timestamp(f"Lỗi: Không tìm thấy file database {HIVE_DB_FILE}")
        print_with_timestamp("Hãy chạy script simulate_hadoop.py trước.")
        return
    
    # Kết nối đến SQLite
    conn = sqlite3.connect(HIVE_DB_FILE)
    cursor = conn.cursor()
    
    # Danh sách các truy vấn phân tích
    analyses = [
        (1, "Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới", """
        SELECT 
            SUM(new_cases) AS total_cases_worldwide,
            SUM(new_deaths) AS total_deaths_worldwide,
            SUM(new_recovered) AS total_recovered_worldwide
        FROM pandemic_data;
        """),
        
        (2, "Tổng số ca theo từng quốc gia", """
        SELECT 
            country,
            SUM(new_cases) AS total_cases,
            SUM(new_deaths) AS total_deaths,
            SUM(new_recovered) AS total_recovered
        FROM pandemic_data
        GROUP BY country
        ORDER BY total_cases DESC;
        """),
        
        (3, "Top 10 quốc gia có số ca mắc cao nhất", """
        SELECT 
            country,
            SUM(new_cases) AS total_cases
        FROM pandemic_data
        GROUP BY country
        ORDER BY total_cases DESC
        LIMIT 10;
        """),
        
        (4, "Top 10 quốc gia có số ca tử vong cao nhất", """
        SELECT 
            country,
            SUM(new_deaths) AS total_deaths
        FROM pandemic_data
        GROUP BY country
        ORDER BY total_deaths DESC
        LIMIT 10;
        """),
        
        (5, "Tính tỷ lệ tử vong trung bình của từng quốc gia", """
        SELECT 
            country,
            ROUND(SUM(new_deaths) * 100.0 / SUM(new_cases), 2) AS mortality_rate
        FROM pandemic_data
        GROUP BY country
        HAVING SUM(new_cases) > 0
        ORDER BY mortality_rate DESC;
        """),
        
        (6, "Tổng số ca mắc theo châu lục", """
        SELECT 
            continent,
            SUM(new_cases) AS total_cases,
            SUM(new_deaths) AS total_deaths,
            SUM(new_recovered) AS total_recovered
        FROM pandemic_data
        GROUP BY continent
        ORDER BY total_cases DESC;
        """),
        
        (7, "Phân tích số ca mắc trung bình mỗi ngày", """
        SELECT 
            date,
            AVG(new_cases) AS avg_daily_cases,
            AVG(new_deaths) AS avg_daily_deaths,
            AVG(new_recovered) AS avg_daily_recovered
        FROM pandemic_data
        GROUP BY date
        ORDER BY date
        LIMIT 20;
        """),
        
        (8, "Truy vấn số ca tử vong theo từng tháng", """
        SELECT 
            SUBSTR(date, 1, 7) AS month,
            SUM(new_deaths) AS monthly_deaths
        FROM pandemic_data
        GROUP BY SUBSTR(date, 1, 7)
        ORDER BY month;
        """),
        
        (9, "Phân tích xu hướng ca mắc theo từng năm", """
        SELECT 
            SUBSTR(date, 1, 4) AS year,
            SUM(new_cases) AS yearly_cases,
            SUM(new_deaths) AS yearly_deaths,
            SUM(new_recovered) AS yearly_recovered
        FROM pandemic_data
        GROUP BY SUBSTR(date, 1, 4)
        ORDER BY year;
        """),
        
        (10, "Top 5 quốc gia có tỷ lệ hồi phục cao nhất", """
        SELECT 
            country,
            ROUND(SUM(new_recovered) * 100.0 / SUM(new_cases), 2) AS recovery_rate
        FROM pandemic_data
        GROUP BY country
        HAVING SUM(new_cases) > 1000
        ORDER BY recovery_rate DESC
        LIMIT 5;
        """)
    ]
    
    # Thực thi từng truy vấn và tạo biểu đồ
    for idx, (query_num, description, query) in enumerate(analyses):
        df = execute_query(cursor, query_num, query, description)
        
        # Tạo biểu đồ cho một số truy vấn
        if query_num == 3:  # Top 10 quốc gia có số ca mắc cao nhất
            visualize_results(
                query_num, df, 'bar', 
                'Top 10 quốc gia có số ca mắc cao nhất', 
                'Quốc gia', 'Tổng số ca mắc', 
                'top10_countries_cases.png'
            )
        
        elif query_num == 4:  # Top 10 quốc gia có số ca tử vong cao nhất
            visualize_results(
                query_num, df, 'bar', 
                'Top 10 quốc gia có số ca tử vong cao nhất', 
                'Quốc gia', 'Tổng số ca tử vong', 
                'top10_countries_deaths.png'
            )
        
        elif query_num == 6:  # Tổng số ca mắc theo châu lục
            visualize_results(
                query_num, df, 'pie', 
                'Tỷ lệ ca mắc theo châu lục', 
                '', '', 
                'continent_cases_pie.png'
            )
            
            visualize_results(
                query_num, df, 'bar', 
                'Tổng số ca mắc theo châu lục', 
                'Châu lục', 'Tổng số ca mắc', 
                'continent_cases_bar.png'
            )
        
        elif query_num == 8:  # Truy vấn số ca tử vong theo từng tháng
            visualize_results(
                query_num, df, 'line', 
                'Số ca tử vong theo tháng', 
                'Tháng', 'Số ca tử vong', 
                'monthly_deaths.png'
            )
        
        elif query_num == 9:  # Phân tích xu hướng ca mắc theo từng năm
            # Tạo biểu đồ cho xu hướng ca mắc theo năm
            if df is not None and len(df) > 0:
                plt.figure(figsize=(12, 6))
                
                plt.plot(df['year'], df['yearly_cases'], marker='o', label='Ca mắc')
                plt.plot(df['year'], df['yearly_deaths'], marker='s', label='Tử vong')
                plt.plot(df['year'], df['yearly_recovered'], marker='^', label='Hồi phục')
                
                plt.title("Xu hướng dịch bệnh theo năm")
                plt.xlabel("Năm")
                plt.ylabel("Số ca")
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
                
                output_path = os.path.join(VISUALIZATIONS_DIR, 'yearly_trend.png')
                plt.savefig(output_path)
                plt.close()
                
                print_with_timestamp(f"Đã lưu biểu đồ: {output_path}")
    
    # Đóng kết nối
    conn.close()

def main():
    """Hàm chính thực hiện phân tích"""
    print_with_timestamp("===== BẮT ĐẦU PHÂN TÍCH DỮ LIỆU DỊCH BỆNH =====")
    
    # Thực hiện các truy vấn phân tích
    run_analysis()
    
    print_with_timestamp("\n===== KẾT THÚC PHÂN TÍCH =====")
    print_with_timestamp(f"Các biểu đồ đã được lưu trong thư mục: {os.path.abspath(VISUALIZATIONS_DIR)}")

if __name__ == "__main__":
    main() 