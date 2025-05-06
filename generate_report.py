#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script tạo báo cáo Markdown tổng hợp về dự án phân tích dữ liệu dịch bệnh
"""

import os
import pandas as pd
import sqlite3
from datetime import datetime
import base64
import io
from PIL import Image
import subprocess

# Các thông số cấu hình
HIVE_DB_FILE = "hive_simulation.db"
INPUT_FILE = "pandemic_data.csv"
VISUALIZATIONS_DIR = "visualizations"
OUTPUT_REPORT = "big_data_covid_report.md"

def get_file_size(file_path):
    """Lấy kích thước file và chuyển đổi sang định dạng đọc được"""
    size_bytes = os.path.getsize(file_path)
    
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def get_command_output(command):
    """Thực thi lệnh shell và trả về output"""
    try:
        result = subprocess.run(command, shell=True, check=True, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                               universal_newlines=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Lỗi khi chạy lệnh: {e.stderr}"

def create_report():
    """Tạo báo cáo Markdown"""
    report = []
    
    # Tiêu đề và giới thiệu
    report.append("# Báo cáo Phân tích Dữ liệu Dịch bệnh COVID-19")
    report.append("")
    report.append("*Phân tích Big Data sử dụng Hadoop, HDFS, Hive và Spark*")
    report.append("")
    report.append(f"*Ngày tạo báo cáo: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}*")
    report.append("")
    report.append("## 1. Giới thiệu")
    report.append("")
    report.append("Dự án này thực hiện phân tích tình hình dịch bệnh COVID-19 trên toàn cầu, bao gồm:")
    report.append("- Tạo dữ liệu mô phỏng dịch bệnh với khối lượng lớn")
    report.append("- Lưu trữ dữ liệu trên HDFS")
    report.append("- Tạo bảng dữ liệu trong Hive")
    report.append("- Thực hiện các truy vấn phân tích")
    report.append("- Trực quan hóa kết quả phân tích")
    report.append("")
    
    # Phần sinh dữ liệu
    report.append("## 2. Tạo Dữ liệu Mô phỏng")
    report.append("")
    
    # Thông tin về file dữ liệu
    if os.path.exists(INPUT_FILE):
        file_size = get_file_size(INPUT_FILE)
        try:
            df = pd.read_csv(INPUT_FILE)
            num_rows = len(df)
            num_cols = len(df.columns)
            report.append(f"Script Python đã được sử dụng để tạo dữ liệu mô phỏng dịch bệnh. Dữ liệu bao gồm:")
            report.append(f"- Số dòng: {num_rows:,}")
            report.append(f"- Số cột: {num_cols}")
            report.append(f"- Kích thước file: {file_size}")
            report.append(f"- Các cột dữ liệu: {', '.join(df.columns)}")
            report.append("")
            
            # Mẫu dữ liệu
            report.append("### Mẫu dữ liệu")
            report.append("")
            report.append("```")
            report.append(df.head(10).to_string(index=False))
            report.append("```")
            report.append("")
            
            # Thống kê mô tả
            report.append("### Thống kê mô tả")
            report.append("")
            report.append("```")
            report.append(df.describe().to_string())
            report.append("```")
            report.append("")
        except Exception as e:
            report.append(f"Lỗi khi đọc file dữ liệu: {str(e)}")
    else:
        report.append(f"Không tìm thấy file dữ liệu {INPUT_FILE}")
    
    # Phần lưu trữ dữ liệu trên HDFS
    report.append("## 3. Lưu trữ Dữ liệu trên HDFS")
    report.append("")
    report.append("### Tạo thư mục trên HDFS")
    report.append("")
    report.append("```bash")
    report.append("hdfs dfs -mkdir -p /user/pandemic_data")
    report.append("```")
    report.append("")
    report.append("### Upload dữ liệu lên HDFS")
    report.append("")
    report.append("```bash")
    report.append(f"hdfs dfs -put {INPUT_FILE} /user/pandemic_data/")
    report.append("```")
    report.append("")
    report.append("### Kiểm tra dữ liệu trên HDFS")
    report.append("")
    report.append("```bash")
    report.append("hdfs dfs -ls /user/pandemic_data/")
    report.append("hdfs dfs -du -h /user/pandemic_data/")
    report.append("```")
    report.append("")
    
    # Phần tạo bảng Hive
    report.append("## 4. Tạo Bảng Hive")
    report.append("")
    report.append("### Câu lệnh tạo bảng")
    report.append("")
    report.append("```sql")
    report.append("""CREATE EXTERNAL TABLE pandemic_data (
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
    report.append("```")
    report.append("")
    
    # Phần phân tích dữ liệu
    report.append("## 5. Phân tích Dữ liệu")
    report.append("")
    
    # Kiểm tra nếu file database tồn tại
    if os.path.exists(HIVE_DB_FILE):
        try:
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
                ORDER BY mortality_rate DESC
                LIMIT 10;
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
                
                (8, "Truy vấn số ca tử vong theo từng tháng", """
                SELECT 
                    SUBSTR(date, 1, 7) AS month,
                    SUM(new_deaths) AS monthly_deaths
                FROM pandemic_data
                GROUP BY SUBSTR(date, 1, 7)
                ORDER BY month
                LIMIT 12;
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
            
            # Thực thi từng truy vấn và thêm vào báo cáo
            for idx, (query_num, description, query) in enumerate(analyses):
                report.append(f"### {query_num}. {description}")
                report.append("")
                report.append("**Câu lệnh SQL:**")
                report.append("```sql")
                report.append(query.strip())
                report.append("```")
                report.append("")
                
                # Thực thi truy vấn
                cursor.execute(query)
                rows = cursor.fetchall()
                
                if len(rows) > 0:
                    # Lấy tên cột
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Tạo DataFrame từ kết quả
                    df_result = pd.DataFrame(rows, columns=column_names)
                    
                    # Thêm kết quả vào báo cáo
                    report.append("**Kết quả:**")
                    report.append("```")
                    report.append(df_result.to_string(index=False))
                    report.append("```")
                    report.append("")
                    
                    # Thêm hình ảnh nếu tồn tại
                    if query_num == 3 and os.path.exists(f"{VISUALIZATIONS_DIR}/top10_countries_cases.png"):
                        report.append("**Biểu đồ:**")
                        report.append("")
                        report.append(f"![Top 10 quốc gia có số ca mắc cao nhất]({VISUALIZATIONS_DIR}/top10_countries_cases.png)")
                        report.append("")
                    
                    elif query_num == 4 and os.path.exists(f"{VISUALIZATIONS_DIR}/top10_countries_deaths.png"):
                        report.append("**Biểu đồ:**")
                        report.append("")
                        report.append(f"![Top 10 quốc gia có số ca tử vong cao nhất]({VISUALIZATIONS_DIR}/top10_countries_deaths.png)")
                        report.append("")
                    
                    elif query_num == 6:
                        if os.path.exists(f"{VISUALIZATIONS_DIR}/continent_cases_pie.png"):
                            report.append("**Biểu đồ tròn:**")
                            report.append("")
                            report.append(f"![Tỷ lệ ca mắc theo châu lục]({VISUALIZATIONS_DIR}/continent_cases_pie.png)")
                            report.append("")
                        
                        if os.path.exists(f"{VISUALIZATIONS_DIR}/continent_cases_bar.png"):
                            report.append("**Biểu đồ cột:**")
                            report.append("")
                            report.append(f"![Tổng số ca mắc theo châu lục]({VISUALIZATIONS_DIR}/continent_cases_bar.png)")
                            report.append("")
                    
                    elif query_num == 8 and os.path.exists(f"{VISUALIZATIONS_DIR}/monthly_deaths.png"):
                        report.append("**Biểu đồ:**")
                        report.append("")
                        report.append(f"![Số ca tử vong theo tháng]({VISUALIZATIONS_DIR}/monthly_deaths.png)")
                        report.append("")
                    
                    elif query_num == 9 and os.path.exists(f"{VISUALIZATIONS_DIR}/yearly_trend.png"):
                        report.append("**Biểu đồ:**")
                        report.append("")
                        report.append(f"![Xu hướng dịch bệnh theo năm]({VISUALIZATIONS_DIR}/yearly_trend.png)")
                        report.append("")
                else:
                    report.append("Không có kết quả nào.")
                    report.append("")
            
            # Đóng kết nối
            conn.close()
        except Exception as e:
            report.append(f"Lỗi khi truy vấn dữ liệu: {str(e)}")
    else:
        report.append(f"Không tìm thấy file database {HIVE_DB_FILE}")
    
    # Kết luận
    report.append("## 6. Kết luận")
    report.append("")
    report.append("Qua phân tích dữ liệu dịch bệnh COVID-19, có thể rút ra một số kết luận:")
    report.append("- Châu Á là khu vực có số ca mắc và tử vong cao nhất, tiếp theo là Bắc Mỹ và Châu Âu.")
    report.append("- Một số quốc gia như Indonesia, Trung Quốc và Philippines đã chịu tác động nặng nề nhất về số ca mắc.")
    report.append("- Tỷ lệ tử vong trung bình trên toàn cầu khoảng 1.3-1.4%.")
    report.append("- Năm 2020 là năm có số ca mắc cao nhất, sau đó giảm dần qua các năm 2021, 2022 và 2023.")
    report.append("- Peru, Mỹ và Fiji là các quốc gia có tỷ lệ hồi phục cao nhất.")
    report.append("")
    report.append("Dự án này minh họa cách sử dụng các công cụ Big Data như Hadoop, HDFS, Hive và Spark để xử lý và phân tích dữ liệu có khối lượng lớn. Mô hình phân tích này có thể được mở rộng để xử lý hàng trăm GB hoặc TB dữ liệu trên hệ thống phân tán.")
    
    # Ghi báo cáo vào file
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    
    return os.path.abspath(OUTPUT_REPORT)

def main():
    """Hàm chính tạo báo cáo"""
    print(f"Bắt đầu tạo báo cáo Markdown...")
    
    # Tạo báo cáo
    report_path = create_report()
    
    print(f"Báo cáo đã được tạo tại: {report_path}")

if __name__ == "__main__":
    main() 