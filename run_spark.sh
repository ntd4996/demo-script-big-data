#!/bin/bash

#############################################
# Script chạy phân tích dữ liệu COVID-19    #
# sử dụng Apache Spark                      #
#############################################

# Thiết lập màu sắc cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Hiển thị banner
echo -e "${YELLOW}"
echo "================================================================="
echo "      PHÂN TÍCH DỮ LIỆU COVID-19 BẰNG APACHE SPARK"
echo "================================================================="
echo -e "${NC}"

# Kiểm tra Spark có được cài đặt
echo -e "${BLUE}[1/4] Kiểm tra môi trường Spark...${NC}"
command -v spark-submit >/dev/null 2>&1 || { 
    echo -e "${RED}Apache Spark không được cài đặt hoặc không nằm trong PATH.${NC}"
    echo -e "${YELLOW}Script này yêu cầu Apache Spark được cài đặt và cấu hình đúng.${NC}"
    echo -e "${YELLOW}Bạn có muốn tiếp tục với chế độ mô phỏng Spark không? (y/n)${NC}"
    read -r simulate
    if [[ "$simulate" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo -e "${GREEN}Chuyển sang chế độ mô phỏng Spark.${NC}"
        SIMULATE_MODE=true
    else
        echo -e "${RED}Thoát phân tích.${NC}"
        exit 1
    fi
}

# Tạo script Spark Python nếu chưa tồn tại
SPARK_SCRIPT="scripts/spark_analysis.py"
echo -e "${BLUE}[2/4] Kiểm tra script phân tích Spark...${NC}"

if [ ! -f "$SPARK_SCRIPT" ]; then
    echo -e "${YELLOW}Script $SPARK_SCRIPT không tồn tại. Đang tạo script mẫu...${NC}"
    mkdir -p scripts
    cat > "$SPARK_SCRIPT" << 'EOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script phân tích dữ liệu COVID-19 sử dụng Apache Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, col, substring, avg, desc, when
from pyspark.sql.types import IntegerType, DoubleType
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime

# Các thông số cấu hình
INPUT_FILE = "pandemic_data.csv"
HDFS_PATH = "/user/pandemic_data/pandemic_data.csv"
VISUALIZATIONS_DIR = "visualizations"

# Tạo thư mục visualizations nếu chưa tồn tại
if not os.path.exists(VISUALIZATIONS_DIR):
    os.makedirs(VISUALIZATIONS_DIR)

def print_with_timestamp(message):
    """In thông báo kèm timestamp"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def create_spark_session():
    """Tạo và cấu hình Spark Session"""
    print_with_timestamp("Đang khởi tạo Spark Session...")
    
    spark = SparkSession.builder \
        .appName("COVID-19 Data Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    print_with_timestamp("Đã khởi tạo Spark Session thành công")
    return spark

def load_data(spark, use_hdfs=False):
    """Đọc dữ liệu từ HDFS hoặc file local"""
    print_with_timestamp("Đang đọc dữ liệu...")
    
    file_path = HDFS_PATH if use_hdfs else INPUT_FILE
    print_with_timestamp(f"Đọc dữ liệu từ: {file_path}")
    
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        # Chuyển đổi kiểu dữ liệu để đảm bảo tính toán chính xác
        df = df.withColumn("new_cases", col("new_cases").cast(IntegerType())) \
              .withColumn("new_deaths", col("new_deaths").cast(IntegerType())) \
              .withColumn("new_recovered", col("new_recovered").cast(IntegerType())) \
              .withColumn("total_cases", col("total_cases").cast(IntegerType())) \
              .withColumn("total_deaths", col("total_deaths").cast(IntegerType())) \
              .withColumn("total_recovered", col("total_recovered").cast(IntegerType()))
        
        print_with_timestamp(f"Đã đọc dữ liệu thành công. Số dòng: {df.count()}, Số cột: {len(df.columns)}")
        return df
    except Exception as e:
        print_with_timestamp(f"Lỗi khi đọc dữ liệu: {str(e)}")
        print_with_timestamp("Đang thử đọc từ file local...")
        try:
            df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)
            
            # Chuyển đổi kiểu dữ liệu
            df = df.withColumn("new_cases", col("new_cases").cast(IntegerType())) \
                  .withColumn("new_deaths", col("new_deaths").cast(IntegerType())) \
                  .withColumn("new_recovered", col("new_recovered").cast(IntegerType())) \
                  .withColumn("total_cases", col("total_cases").cast(IntegerType())) \
                  .withColumn("total_deaths", col("total_deaths").cast(IntegerType())) \
                  .withColumn("total_recovered", col("total_recovered").cast(IntegerType()))
            
            print_with_timestamp(f"Đã đọc dữ liệu local thành công. Số dòng: {df.count()}")
            return df
        except Exception as e2:
            print_with_timestamp(f"Lỗi khi đọc dữ liệu local: {str(e2)}")
            raise

def execute_query(spark, df, query_num, description, sql_func):
    """Thực hiện một truy vấn và đo thời gian thực thi"""
    print_with_timestamp(f"\n===== Truy vấn {query_num}: {description} =====")
    
    start_time = time.time()
    result_df = sql_func(df)
    end_time = time.time()
    
    # In kết quả
    print_with_timestamp(f"Kết quả (thời gian: {(end_time - start_time):.3f} giây):")
    result_df.show(10, False)
    
    return result_df

def save_visualization(result_df, query_num, visualization_type, title, xlabel, ylabel, filename):
    """Lưu kết quả truy vấn thành biểu đồ"""
    if result_df is None or result_df.count() == 0:
        print_with_timestamp("Không có dữ liệu để vẽ biểu đồ")
        return
    
    # Chuyển Spark DataFrame sang Pandas DataFrame để vẽ biểu đồ
    pdf = result_df.toPandas()
    
    plt.figure(figsize=(12, 6))
    
    if visualization_type == 'bar':
        x_col = pdf.columns[0]
        y_col = pdf.columns[1]
        plt.bar(pdf[x_col], pdf[y_col])
        
    elif visualization_type == 'line':
        x_col = pdf.columns[0]
        y_col = pdf.columns[1]
        plt.plot(pdf[x_col], pdf[y_col], marker='o')
        
    elif visualization_type == 'pie':
        labels = pdf[pdf.columns[0]]
        sizes = pdf[pdf.columns[1]]
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    
    if visualization_type == 'bar' and len(pdf) > 5:
        plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Lưu biểu đồ
    output_path = os.path.join(VISUALIZATIONS_DIR, filename)
    plt.savefig(output_path)
    plt.close()
    
    print_with_timestamp(f"Đã lưu biểu đồ: {output_path}")

def run_analysis(spark, df):
    """Thực hiện các truy vấn phân tích"""
    # Đăng ký DataFrame như một bảng tạm thời để có thể sử dụng SQL
    df.createOrReplaceTempView("pandemic_data")
    
    # 1. Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới
    result_1 = execute_query(
        spark, df, 
        1, "Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới",
        lambda df: df.select(
            sum("new_cases").alias("total_cases_worldwide"),
            sum("new_deaths").alias("total_deaths_worldwide"),
            sum("new_recovered").alias("total_recovered_worldwide")
        )
    )
    
    # 2. Tổng số ca theo từng quốc gia
    result_2 = execute_query(
        spark, df,
        2, "Tổng số ca theo từng quốc gia",
        lambda df: df.groupBy("country").agg(
            sum("new_cases").alias("total_cases"),
            sum("new_deaths").alias("total_deaths"),
            sum("new_recovered").alias("total_recovered")
        ).orderBy(col("total_cases").desc())
    )
    
    # 3. Top 10 quốc gia có số ca mắc cao nhất
    result_3 = execute_query(
        spark, df,
        3, "Top 10 quốc gia có số ca mắc cao nhất",
        lambda df: df.groupBy("country").agg(
            sum("new_cases").alias("total_cases")
        ).orderBy(col("total_cases").desc()).limit(10)
    )
    
    # Vẽ biểu đồ cho truy vấn 3
    save_visualization(
        result_3, 3, 'bar',
        'Top 10 quốc gia có số ca mắc cao nhất',
        'Quốc gia', 'Tổng số ca mắc',
        'spark_top10_countries_cases.png'
    )
    
    # 4. Top 10 quốc gia có số ca tử vong cao nhất
    result_4 = execute_query(
        spark, df,
        4, "Top 10 quốc gia có số ca tử vong cao nhất",
        lambda df: df.groupBy("country").agg(
            sum("new_deaths").alias("total_deaths")
        ).orderBy(col("total_deaths").desc()).limit(10)
    )
    
    # Vẽ biểu đồ cho truy vấn 4
    save_visualization(
        result_4, 4, 'bar',
        'Top 10 quốc gia có số ca tử vong cao nhất',
        'Quốc gia', 'Tổng số ca tử vong',
        'spark_top10_countries_deaths.png'
    )
    
    # 5. Tính tỷ lệ tử vong trung bình của từng quốc gia
    result_5 = execute_query(
        spark, df,
        5, "Tỷ lệ tử vong trung bình của từng quốc gia",
        lambda df: df.groupBy("country").agg(
            sum("new_cases").alias("total_cases"),
            sum("new_deaths").alias("total_deaths"),
            (round(sum("new_deaths") * 100.0 / sum("new_cases"), 2)).alias("mortality_rate")
        ).filter(col("total_cases") > 0).orderBy(col("mortality_rate").desc())
    )
    
    # 6. Tổng số ca mắc theo châu lục
    result_6 = execute_query(
        spark, df,
        6, "Tổng số ca mắc theo châu lục",
        lambda df: df.groupBy("continent").agg(
            sum("new_cases").alias("total_cases"),
            sum("new_deaths").alias("total_deaths"),
            sum("new_recovered").alias("total_recovered")
        ).orderBy(col("total_cases").desc())
    )
    
    # Vẽ biểu đồ tròn cho truy vấn 6
    save_visualization(
        result_6, 6, 'pie',
        'Tỷ lệ ca mắc theo châu lục',
        '', '',
        'spark_continent_cases_pie.png'
    )
    
    # Vẽ biểu đồ cột cho truy vấn 6
    save_visualization(
        result_6, 6, 'bar',
        'Tổng số ca mắc theo châu lục',
        'Châu lục', 'Tổng số ca mắc',
        'spark_continent_cases_bar.png'
    )
    
    # 7. Số ca tử vong theo từng tháng
    result_7 = execute_query(
        spark, df,
        7, "Số ca tử vong theo từng tháng",
        lambda df: df.withColumn("month", substring(col("date"), 1, 7))
                     .groupBy("month")
                     .agg(sum("new_deaths").alias("monthly_deaths"))
                     .orderBy("month")
    )
    
    # Vẽ biểu đồ đường cho truy vấn 7
    save_visualization(
        result_7, 7, 'line',
        'Số ca tử vong theo tháng',
        'Tháng', 'Số ca tử vong',
        'spark_monthly_deaths.png'
    )
    
    # 8. Xu hướng ca mắc theo từng năm
    result_8 = execute_query(
        spark, df,
        8, "Xu hướng ca mắc theo từng năm",
        lambda df: df.withColumn("year", substring(col("date"), 1, 4))
                     .groupBy("year")
                     .agg(
                         sum("new_cases").alias("yearly_cases"),
                         sum("new_deaths").alias("yearly_deaths"),
                         sum("new_recovered").alias("yearly_recovered")
                     )
                     .orderBy("year")
    )
    
    # Vẽ biểu đồ kết hợp cho xu hướng theo năm
    if result_8 is not None and result_8.count() > 0:
        pdf = result_8.toPandas()
        
        plt.figure(figsize=(12, 6))
        
        plt.plot(pdf['year'], pdf['yearly_cases'], marker='o', label='Ca mắc')
        plt.plot(pdf['year'], pdf['yearly_deaths'], marker='s', label='Tử vong')
        plt.plot(pdf['year'], pdf['yearly_recovered'], marker='^', label='Hồi phục')
        
        plt.title("Xu hướng dịch bệnh theo năm")
        plt.xlabel("Năm")
        plt.ylabel("Số ca")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        
        output_path = os.path.join(VISUALIZATIONS_DIR, 'spark_yearly_trend.png')
        plt.savefig(output_path)
        plt.close()
        
        print_with_timestamp(f"Đã lưu biểu đồ: {output_path}")
    
    # 9. Top 5 quốc gia có tỷ lệ hồi phục cao nhất
    result_9 = execute_query(
        spark, df,
        9, "Top 5 quốc gia có tỷ lệ hồi phục cao nhất",
        lambda df: df.groupBy("country").agg(
            sum("new_cases").alias("total_cases"),
            sum("new_recovered").alias("total_recovered"),
            (round(sum("new_recovered") * 100.0 / sum("new_cases"), 2)).alias("recovery_rate")
        ).filter(col("total_cases") > 1000).orderBy(col("recovery_rate").desc()).limit(5)
    )

    # 10. Phân tích mức độ nghiêm trọng theo mùa (Quý)
    result_10 = execute_query(
        spark, df,
        10, "Phân tích mức độ nghiêm trọng theo mùa (Quý)",
        lambda df: spark.sql("""
            SELECT 
                CONCAT(SUBSTRING(date, 1, 4), '-Q', CAST((CAST(SUBSTRING(date, 6, 2) AS INT) + 2) / 3 AS STRING)) AS quarter,
                SUM(new_cases) AS quarterly_cases,
                SUM(new_deaths) AS quarterly_deaths
            FROM pandemic_data
            GROUP BY quarter
            ORDER BY quarter
        """)
    )

    print_with_timestamp("\n===== Phân tích Spark hoàn tất =====")
    
def main():
    """Hàm chính"""
    print_with_timestamp("===== BẮT ĐẦU PHÂN TÍCH DỮ LIỆU BẰNG APACHE SPARK =====")
    
    # Khởi tạo Spark Session
    spark = create_spark_session()
    
    try:
        # Đọc dữ liệu từ HDFS hoặc file local
        df = load_data(spark, use_hdfs=False)
        
        # Thực hiện phân tích
        run_analysis(spark, df)
        
    except Exception as e:
        print_with_timestamp(f"Lỗi: {str(e)}")
    finally:
        # Dừng Spark Session
        print_with_timestamp("Đang dừng Spark Session...")
        spark.stop()
        print_with_timestamp("Đã dừng Spark Session")
    
    print_with_timestamp("===== KẾT THÚC PHÂN TÍCH =====")

if __name__ == "__main__":
    main()
EOF
    
    chmod +x "$SPARK_SCRIPT"
    echo -e "${GREEN}Đã tạo script phân tích Spark: $SPARK_SCRIPT${NC}"
else
    echo -e "${GREEN}Script phân tích Spark đã tồn tại: $SPARK_SCRIPT${NC}"
fi

# Kiểm tra file dữ liệu tồn tại
echo -e "${BLUE}[3/4] Kiểm tra file dữ liệu...${NC}"
if [ ! -f "pandemic_data.csv" ]; then
    echo -e "${YELLOW}File pandemic_data.csv không tồn tại.${NC}"
    echo -e "${YELLOW}Bạn có muốn chạy script generate_pandemic_data.py để tạo dữ liệu không? (y/n)${NC}"
    read -r generate_data
    if [[ "$generate_data" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo -e "${GREEN}Đang tạo dữ liệu...${NC}"
        python3 scripts/generate_pandemic_data.py
        if [ ! -f "pandemic_data.csv" ]; then
            echo -e "${RED}Không thể tạo file dữ liệu. Thoát phân tích.${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Không có file dữ liệu. Thoát phân tích.${NC}"
        exit 1
    fi
fi

# Chạy phân tích
echo -e "${BLUE}[4/4] Chạy phân tích bằng Spark...${NC}"
if [ "$SIMULATE_MODE" != true ]; then
    # Chạy bằng Spark thực tế
    echo -e "${GREEN}Đang chạy phân tích bằng Spark...${NC}"
    spark-submit --master local[*] --driver-memory 2g "$SPARK_SCRIPT"
else
    # Chạy script Python thường (giả lập Spark)
    echo -e "${YELLOW}Chế độ mô phỏng: Chạy script phân tích không sử dụng Spark...${NC}"
    echo -e "${YELLOW}Đang cài đặt thư viện cần thiết...${NC}"
    pip install pandas numpy matplotlib findspark
    
    echo -e "${GREEN}Đang chạy phân tích (mô phỏng)...${NC}"
    python3 "$SPARK_SCRIPT"
fi

# Kiểm tra kết quả
if [ -d "visualizations" ] && [ "$(ls -A visualizations 2>/dev/null)" ]; then
    echo -e "\n${GREEN}==========================================================${NC}"
    echo -e "${GREEN}     PHÂN TÍCH BẰNG SPARK HOÀN TẤT THÀNH CÔNG             ${NC}"
    echo -e "${GREEN}==========================================================${NC}"
    
    echo -e "\n${YELLOW}Tóm tắt:${NC}"
    echo -e "- Số biểu đồ tạo ra: $(ls -1 visualizations/spark_*.png 2>/dev/null | wc -l)"
    echo -e "- Thư mục chứa kết quả: ${BLUE}$(pwd)/visualizations/${NC}"
    
    # Hiển thị danh sách biểu đồ Spark
    echo -e "\n${YELLOW}Các biểu đồ Spark đã tạo:${NC}"
    for chart in visualizations/spark_*.png; do
        if [ -f "$chart" ]; then
            echo -e "- ${BLUE}$chart${NC}"
        fi
    done
else
    echo -e "\n${RED}==========================================================${NC}"
    echo -e "${RED}     KHÔNG TÌM THẤY KẾT QUẢ PHÂN TÍCH SPARK                ${NC}"
    echo -e "${RED}==========================================================${NC}"
fi

# Đề xuất bước tiếp theo
echo -e "\n${YELLOW}Đề xuất bước tiếp theo:${NC}"
echo -e "1. Xem các biểu đồ trong thư mục: ${BLUE}visualizations/${NC}"
echo -e "2. Tạo báo cáo tổng hợp: ${BLUE}python3 scripts/generate_report.py${NC}"
echo -e "3. Đóng gói bài nộp: ${BLUE}./prepare_submission.sh${NC}"

echo -e "\n${GREEN}Hoàn tất!${NC}" 