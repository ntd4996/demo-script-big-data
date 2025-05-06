#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phân tích dữ liệu dịch bệnh sử dụng PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg, col, substring, desc, expr, date_format
import matplotlib.pyplot as plt
import pandas as pd
import os

# Tạo thư mục để lưu biểu đồ
os.makedirs("visualizations", exist_ok=True)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Pandemic Data Analysis") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Đọc dữ liệu từ file CSV trên HDFS (nếu bạn muốn đọc trực tiếp từ file)
# df = spark.read.csv("hdfs:///user/pandemic_data/pandemic_data.csv", header=True, inferSchema=True)

# HOẶC sử dụng bảng Hive đã tạo (khuyến nghị)
print("Đọc dữ liệu từ bảng Hive 'pandemic_data'...")
df = spark.sql("SELECT * FROM pandemic_data")

# Đăng ký DataFrame như một bảng tạm thời
df.createOrReplaceTempView("pandemic_temp")

print("Số dòng dữ liệu:", df.count())
print("Cấu trúc dữ liệu:")
df.printSchema()

print("\n----- PHÂN TÍCH DỮ LIỆU -----\n")

# 1. Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới
print("1. Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới:")
world_totals = df.agg(
    spark_sum("new_cases").alias("total_cases_worldwide"),
    spark_sum("new_deaths").alias("total_deaths_worldwide"),
    spark_sum("new_recovered").alias("total_recovered_worldwide")
)
world_totals.show()

# 2. Top 10 quốc gia có số ca mắc cao nhất
print("2. Top 10 quốc gia có số ca mắc cao nhất:")
top_countries = df.groupBy("country") \
    .agg(spark_sum("new_cases").alias("total_cases")) \
    .orderBy(desc("total_cases")) \
    .limit(10)
top_countries.show()

# Chuyển đổi thành Pandas DataFrame cho việc trực quan hóa
top_countries_pd = top_countries.toPandas()

# Vẽ biểu đồ
plt.figure(figsize=(12, 6))
plt.bar(top_countries_pd["country"], top_countries_pd["total_cases"])
plt.title("Top 10 quốc gia có số ca mắc cao nhất")
plt.xlabel("Quốc gia")
plt.ylabel("Tổng số ca mắc")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("visualizations/top10_countries_cases.png")
print("Đã lưu biểu đồ: visualizations/top10_countries_cases.png")

# 3. Tổng số ca mắc theo châu lục
print("3. Tổng số ca mắc theo châu lục:")
continent_totals = df.groupBy("continent") \
    .agg(
        spark_sum("new_cases").alias("total_cases"),
        spark_sum("new_deaths").alias("total_deaths"),
        spark_sum("new_recovered").alias("total_recovered")
    ) \
    .orderBy(desc("total_cases"))
continent_totals.show()

# Trực quan hóa dữ liệu theo châu lục
continent_totals_pd = continent_totals.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(continent_totals_pd["continent"], continent_totals_pd["total_cases"])
plt.title("Tổng số ca mắc theo châu lục")
plt.xlabel("Châu lục")
plt.ylabel("Tổng số ca mắc")
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig("visualizations/continent_cases.png")
print("Đã lưu biểu đồ: visualizations/continent_cases.png")

# 4. Tỷ lệ tử vong trung bình theo quốc gia
print("4. Tỷ lệ tử vong trung bình của các quốc gia:")
mortality_rates = spark.sql("""
    SELECT 
        country,
        SUM(new_deaths) / SUM(new_cases) * 100 AS mortality_rate
    FROM pandemic_temp
    GROUP BY country
    HAVING SUM(new_cases) > 0
    ORDER BY mortality_rate DESC
    LIMIT 10
""")
mortality_rates.show()

# 5. Phân tích xu hướng ca mắc theo từng năm
print("5. Xu hướng ca mắc theo từng năm:")
yearly_trend = df.withColumn("year", substring("date", 1, 4)) \
    .groupBy("year") \
    .agg(
        spark_sum("new_cases").alias("yearly_cases"),
        spark_sum("new_deaths").alias("yearly_deaths"),
        spark_sum("new_recovered").alias("yearly_recovered")
    ) \
    .orderBy("year")
yearly_trend.show()

# Trực quan hóa xu hướng theo năm
yearly_trend_pd = yearly_trend.toPandas()
plt.figure(figsize=(12, 6))
plt.plot(yearly_trend_pd["year"], yearly_trend_pd["yearly_cases"], marker='o', label='Ca mắc')
plt.plot(yearly_trend_pd["year"], yearly_trend_pd["yearly_deaths"], marker='s', label='Tử vong')
plt.plot(yearly_trend_pd["year"], yearly_trend_pd["yearly_recovered"], marker='^', label='Hồi phục')
plt.title("Xu hướng dịch bệnh theo năm")
plt.xlabel("Năm")
plt.ylabel("Số ca")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("visualizations/yearly_trend.png")
print("Đã lưu biểu đồ: visualizations/yearly_trend.png")

# 6. Truy vấn số ca tử vong theo từng tháng
print("6. Số ca tử vong theo từng tháng:")
monthly_deaths = df.withColumn("month", substring("date", 1, 7)) \
    .groupBy("month") \
    .agg(spark_sum("new_deaths").alias("monthly_deaths")) \
    .orderBy("month")
monthly_deaths.show(24)  # Hiển thị tất cả các tháng

# 7. Tìm ngày có số ca mắc mới cao nhất
print("7. Ngày có số ca mắc mới cao nhất:")
peak_day = df.groupBy("date") \
    .agg(spark_sum("new_cases").alias("total_new_cases")) \
    .orderBy(desc("total_new_cases")) \
    .limit(1)
peak_day.show()

# 8. Phân tích số ca mắc trung bình theo ngày trong tuần
print("8. Số ca mắc trung bình theo ngày trong tuần:")
weekday_avg = spark.sql("""
    SELECT 
        date_format(to_date(date), 'EEEE') AS day_of_week,
        AVG(new_cases) AS avg_cases
    FROM pandemic_temp
    GROUP BY date_format(to_date(date), 'EEEE')
    ORDER BY avg_cases DESC
""")
weekday_avg.show()

# Tổng kết
print("\n----- KẾT QUẢ PHÂN TÍCH -----")
world_summary = world_totals.first()
print(f"Tổng số ca mắc trên toàn cầu: {world_summary['total_cases_worldwide']:,}")
print(f"Tổng số ca tử vong trên toàn cầu: {world_summary['total_deaths_worldwide']:,}")
print(f"Tổng số ca hồi phục trên toàn cầu: {world_summary['total_recovered_worldwide']:,}")

print(f"\nTỷ lệ tử vong toàn cầu: {world_summary['total_deaths_worldwide'] / world_summary['total_cases_worldwide'] * 100:.2f}%")
print(f"Tỷ lệ hồi phục toàn cầu: {world_summary['total_recovered_worldwide'] / world_summary['total_cases_worldwide'] * 100:.2f}%")

top_country = top_countries.first()
print(f"\nQuốc gia có nhiều ca mắc nhất: {top_country['country']} với {top_country['total_cases']:,} ca")

# Đóng Spark Session
spark.stop()
print("\nPhân tích hoàn tất!") 