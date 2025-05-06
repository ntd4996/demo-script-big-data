-- Tạo cơ sở dữ liệu mới cho phân tích dịch bệnh
CREATE DATABASE IF NOT EXISTS pandemic_analysis;

-- Sử dụng cơ sở dữ liệu này
USE pandemic_analysis;

-- Tạo bảng external cho dữ liệu dịch bệnh
-- Bảng external cho phép truy cập dữ liệu mà không cần sao chép vào Hive
CREATE EXTERNAL TABLE IF NOT EXISTS pandemic_data (
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
TBLPROPERTIES ("skip.header.line.count"="1");

-- Tạo bảng đã được tối ưu hóa theo định dạng Parquet
-- Định dạng Parquet mang lại hiệu suất truy vấn tốt hơn
CREATE TABLE IF NOT EXISTS pandemic_data_optimized
STORED AS PARQUET
AS SELECT * FROM pandemic_data;

-- Tạo các chỉ mục để tăng tốc truy vấn
-- (Lưu ý: Hive không hỗ trợ đầy đủ chỉ mục như RDBMS)
CREATE INDEX idx_country ON TABLE pandemic_data_optimized(country) 
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';

CREATE INDEX idx_date ON TABLE pandemic_data_optimized(date) 
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';

CREATE INDEX idx_continent ON TABLE pandemic_data_optimized(continent) 
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';

-- Tạo view cho các truy vấn thường dùng
-- View này tổng hợp dữ liệu theo quốc gia
CREATE VIEW IF NOT EXISTS country_summary AS
SELECT 
    country,
    continent,
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    SUM(new_recovered) AS total_recovered,
    ROUND(SUM(new_deaths) * 100.0 / SUM(new_cases), 2) AS mortality_rate,
    ROUND(SUM(new_recovered) * 100.0 / SUM(new_cases), 2) AS recovery_rate
FROM pandemic_data_optimized
GROUP BY country, continent;

-- View này tổng hợp dữ liệu theo thời gian
CREATE VIEW IF NOT EXISTS time_summary AS
SELECT 
    SUBSTR(date, 1, 7) AS month,
    SUM(new_cases) AS monthly_cases,
    SUM(new_deaths) AS monthly_deaths,
    SUM(new_recovered) AS monthly_recovered
FROM pandemic_data_optimized
GROUP BY SUBSTR(date, 1, 7);

-- Phân vùng dữ liệu theo năm và tháng để tối ưu các truy vấn theo thời gian
CREATE TABLE IF NOT EXISTS pandemic_data_partitioned (
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
PARTITIONED BY (
    year STRING,
    month STRING
)
STORED AS PARQUET;

-- Cài đặt để cho phép Hive tự động xác định phân vùng từ dữ liệu
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Chèn dữ liệu vào bảng đã phân vùng
INSERT OVERWRITE TABLE pandemic_data_partitioned
PARTITION (year, month)
SELECT 
    date,
    country,
    continent,
    new_cases,
    new_deaths,
    new_recovered,
    total_cases,
    total_deaths,
    total_recovered,
    SUBSTR(date, 1, 4) AS year,
    SUBSTR(date, 6, 2) AS month
FROM pandemic_data_optimized;

-- Kiểm tra bảng đã tạo
DESCRIBE pandemic_data;

-- Hiển thị 10 dòng đầu tiên của bảng
SELECT * FROM pandemic_data LIMIT 10; 