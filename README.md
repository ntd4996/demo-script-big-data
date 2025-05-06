# Phân tích Dữ liệu Dịch bệnh COVID-19 với Big Data

## Tổng quan dự án

Dự án này thực hiện phân tích tình hình dịch bệnh COVID-19 toàn cầu sử dụng các công nghệ Big Data. Dự án mô phỏng toàn bộ quy trình phân tích Big Data từ việc sinh dữ liệu, lưu trữ phân tán, xử lý, phân tích đến trực quan hóa kết quả.

Mục tiêu chính của dự án là:
- Thiết kế quy trình xử lý dữ liệu lớn cho dữ liệu dịch tễ học
- Mô phỏng việc sử dụng các công nghệ Hadoop/HDFS/Hive trong phân tích dữ liệu lớn
- Thực hiện các phân tích có ý nghĩa về tình hình dịch bệnh toàn cầu
- Trực quan hóa kết quả phân tích và rút ra các kết luận

## Kiến trúc hệ thống

Dự án mô phỏng kiến trúc một hệ thống Big Data với các thành phần chính:

![Kiến trúc hệ thống](https://i.imgur.com/ABCDef.png)

- **Tạo dữ liệu**: Sinh dữ liệu mô phỏng dịch bệnh với khối lượng lớn
- **Lưu trữ**: Mô phỏng HDFS để lưu trữ dữ liệu phân tán
- **Xử lý**: Mô phỏng Hive để quản lý và truy vấn dữ liệu
- **Phân tích**: Thực hiện 10 truy vấn phân tích để rút ra thông tin hữu ích
- **Trực quan hóa**: Tạo các biểu đồ để minh họa kết quả phân tích
- **Báo cáo**: Tổng hợp kết quả vào một báo cáo hoàn chỉnh

## Cấu trúc dự án

```
.
├── pandemic_data.csv               # Dữ liệu mô phỏng dịch bệnh
├── scripts/
│   ├── generate_pandemic_data.py   # Script tạo dữ liệu
│   ├── simulate_hadoop.py          # Script mô phỏng HDFS và Hive
│   ├── simulate_analysis.py        # Script phân tích dữ liệu
│   └── generate_report.py          # Script tạo báo cáo
├── hdfs_simulation/                # Thư mục mô phỏng HDFS
├── visualizations/                 # Thư mục lưu các biểu đồ
├── hive_simulation.db              # SQLite DB mô phỏng Hive
├── big_data_covid_report.md        # Báo cáo tổng hợp
├── analysis_queries.sql            # Các truy vấn phân tích
├── create_hive_table.hql           # Lệnh tạo bảng Hive
├── setup_hdfs.sh                   # Script thiết lập HDFS
├── run_analysis.sh                 # Script chạy phân tích
├── run_spark.sh                    # Script chạy phân tích trên Spark
├── prepare_submission.sh           # Script đóng gói nộp bài
├── .gitignore                      # Cấu hình loại trừ file khi chia sẻ
└── README.md                       # File hướng dẫn
```

## Chi tiết các thành phần

### 1. Sinh dữ liệu (generate_pandemic_data.py)

Script này tạo dữ liệu mô phỏng dịch bệnh COVID-19 với các đặc điểm:

- **Cấu trúc dữ liệu**: date, country, continent, new_cases, new_deaths, new_recovered, total_cases, total_deaths, total_recovered
- **Phạm vi thời gian**: 01/01/2020 - 31/12/2023 (4 năm)
- **Phạm vi không gian**: 60 quốc gia thuộc 6 châu lục
- **Mô hình dữ liệu**: 
  - Mô phỏng các đợt sóng dịch bệnh (3-4 đợt)
  - Hiệu ứng theo mùa (mùa đông số ca cao hơn)
  - Sự khác biệt giữa các quốc gia và châu lục
  - Tương quan giữa số ca mắc, tử vong và hồi phục
- **Xử lý theo lô (batch)**: Xử lý dữ liệu theo từng lô 20,000 dòng để tiết kiệm bộ nhớ
- **Khả năng mở rộng**: Có thể điều chỉnh tham số để tạo 50 triệu dòng (~5-10GB dữ liệu)

Mẫu dữ liệu được tạo ra:

```
date,country,continent,new_cases,new_deaths,new_recovered,total_cases,total_deaths,total_recovered
2020-01-01,Vietnam,Asia,12,0,0,12,0,0
2020-01-01,United States,North America,83,2,0,83,2,0
2020-01-01,Brazil,South America,45,1,0,45,1,0
...
```

### 2. Mô phỏng HDFS và Hive (simulate_hadoop.py)

Script này mô phỏng quá trình làm việc với hệ sinh thái Hadoop:

- **Mô phỏng HDFS**: 
  - Tạo cấu trúc thư mục giống HDFS
  - Mô phỏng quá trình upload file lên HDFS
  - Theo dõi kích thước và vị trí của dữ liệu

- **Mô phỏng Hive**: 
  - Sử dụng SQLite làm backend để mô phỏng Hive
  - Tạo schema phù hợp cho dữ liệu dịch bệnh
  - Tạo bảng external và import dữ liệu từ CSV

Lệnh Hive tương đương được mô phỏng:

```sql
CREATE EXTERNAL TABLE pandemic_data (
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
```

### 3. Phân tích dữ liệu (simulate_analysis.py)

Thực hiện 10 truy vấn phân tích quan trọng:

1. **Tổng số ca mắc, tử vong, hồi phục toàn cầu**
2. **Tổng số ca theo từng quốc gia**
3. **Top 10 quốc gia có số ca mắc cao nhất**
4. **Top 10 quốc gia có số ca tử vong cao nhất**
5. **Tỷ lệ tử vong trung bình của từng quốc gia**
6. **Tổng số ca mắc theo châu lục**
7. **Số ca mắc trung bình mỗi ngày**
8. **Số ca tử vong theo từng tháng**
9. **Xu hướng ca mắc theo từng năm**
10. **Top 5 quốc gia có tỷ lệ hồi phục cao nhất**

Mỗi truy vấn bao gồm:
- Mã SQL được thực thi
- Thời gian thực thi
- Kết quả dạng bảng
- Biểu đồ trực quan (nếu phù hợp)

### 4. Trực quan hóa kết quả

Tạo các biểu đồ minh họa kết quả phân tích:

- **Biểu đồ cột**: 
  - Top 10 quốc gia có số ca mắc cao nhất
  - Top 10 quốc gia có số ca tử vong cao nhất
  - Tổng số ca mắc theo châu lục

- **Biểu đồ đường**: 
  - Số ca tử vong theo tháng
  - Xu hướng dịch bệnh theo năm

- **Biểu đồ tròn**: 
  - Tỷ lệ ca mắc theo châu lục

- **Biểu đồ kết hợp**: 
  - Xu hướng ca mắc, tử vong, hồi phục theo năm

### 5. Tạo báo cáo (generate_report.py)

Tổng hợp tất cả kết quả vào báo cáo Markdown:

- **Giới thiệu**: Mô tả tổng quan về dự án
- **Dữ liệu**: Thông tin về dữ liệu, mẫu và thống kê mô tả
- **Phân tích**: Kết quả chi tiết của 10 truy vấn phân tích
- **Trực quan hóa**: Đính kèm tất cả biểu đồ kết quả
- **Kết luận**: Rút ra các nhận xét và kết luận từ kết quả phân tích

## Hướng dẫn sử dụng

### Yêu cầu hệ thống

- Python 3.6 trở lên
- Ít nhất 2GB RAM (cho 100,000 dòng dữ liệu)
- Khoảng 100MB dung lượng ổ đĩa

### Cài đặt môi trường

```bash
# Nâng cấp pip
python -m pip install --upgrade pip

# Tạo và kích hoạt môi trường ảo (tùy chọn nhưng khuyến khích)
python -m venv venv
source venv/bin/activate  # Trên Linux/Mac
# hoặc
venv\Scripts\activate  # Trên Windows

# Cài đặt các thư viện cần thiết
pip install pandas numpy matplotlib pillow
```

### Chạy từng bước

1. **Sinh dữ liệu**:
```bash
python scripts/generate_pandemic_data.py
```
- Kết quả: File pandemic_data.csv 
- Mặc định: 100,000 dòng (~10MB)
- Để tạo dữ liệu lớn hơn, chỉnh tham số NUM_ROWS trong script

2. **Mô phỏng HDFS và Hive**:
```bash
python scripts/simulate_hadoop.py
```
- Kết quả: Thư mục hdfs_simulation/ và file hive_simulation.db

3. **Phân tích dữ liệu**:
```bash
python scripts/simulate_analysis.py
```
- Kết quả: Các truy vấn được thực thi và biểu đồ được lưu trong thư mục visualizations/

4. **Tạo báo cáo**:
```bash
python scripts/generate_report.py
```
- Kết quả: File big_data_covid_report.md

### Chạy toàn bộ quy trình

```bash
# Cấp quyền thực thi cho script
chmod +x run_analysis.sh

# Chạy toàn bộ quy trình
./run_analysis.sh
```

### Đóng gói nộp bài

```bash
# Cấp quyền thực thi
chmod +x prepare_submission.sh

# Chạy script đóng gói
./prepare_submission.sh
```
- Kết quả: File pandemic_analysis_submission.zip và thư mục pandemic_analysis_submission/

### Chia sẻ dự án

Khi chia sẻ dự án với người khác (hoặc sử dụng Git), file `.gitignore` đã được cấu hình để loại bỏ các file tạm thời và dữ liệu sinh ra trong quá trình chạy:

- Dữ liệu mô phỏng sinh ra (pandemic_data.csv)
- Thư mục mô phỏng HDFS (hdfs_simulation/)
- Database mô phỏng Hive (hive_simulation.db)
- Biểu đồ kết quả (visualizations/)
- Báo cáo sinh ra (big_data_covid_report.md)
- File nén và thư mục nộp bài (pandemic_analysis_submission.zip, pandemic_analysis_submission/)
- Các file môi trường ảo Python (venv/)

Tuy nhiên, một file mẫu dữ liệu với 10 dòng (pandemic_data_minimal.csv) được bao gồm trong dự án để người dùng có thể hiểu cấu trúc dữ liệu mà không cần chạy script tạo dữ liệu đầy đủ.

Để chia sẻ dự án với đầy đủ mã nguồn nhưng không có dữ liệu lớn:

```bash
# Nén dự án để chia sẻ (không bao gồm các file trong .gitignore)
git archive --format zip --output pandemic_project.zip HEAD
```

Nếu sử dụng Git:
```bash
# Sao chép dự án từ Git
git clone <repository-url>

# Xem cấu trúc dữ liệu mẫu
head pandemic_data_minimal.csv

# Hoặc chạy từ đầu để tạo dữ liệu đầy đủ
./run_analysis.sh
```

## Kết quả và phân tích

Sau khi chạy các bước phân tích, chúng ta thu được nhiều kết quả có giá trị về tình hình dịch bệnh:

1. **Phân bố địa lý**: Châu Á và Bắc Mỹ là hai khu vực chịu ảnh hưởng nặng nề nhất
2. **Xu hướng thời gian**: 
   - Đỉnh dịch vào năm 2020-2021
   - Giảm dần vào các năm 2022-2023
   - Có tính mùa vụ rõ rệt (mùa đông số ca cao hơn)
3. **Tỷ lệ tử vong**: 
   - Trung bình toàn cầu khoảng 1.3-1.4%
   - Một số quốc gia có tỷ lệ tử vong cao hơn đáng kể
4. **Khả năng hồi phục**: Hầu hết quốc gia có tỷ lệ hồi phục từ 85-95%

## Ý tưởng mở rộng

Dự án có thể được mở rộng theo nhiều hướng:

1. **Nguồn dữ liệu thực**: Thay thế dữ liệu mô phỏng bằng dữ liệu thực từ:
   - Johns Hopkins CSSE COVID-19 dataset
   - WHO Coronavirus Dashboard
   - Our World in Data COVID-19 dataset

2. **Triển khai thực tế**: Triển khai trên cụm Hadoop thực tế thay vì mô phỏng:
   - Hadoop Distributed File System (HDFS)
   - Apache Hive cho kho dữ liệu
   - Apache Spark cho xử lý dữ liệu nhanh

3. **Phân tích nâng cao**:
   - Dự đoán xu hướng dịch bệnh sử dụng Machine Learning
   - Phân tích tương quan với các chỉ số kinh tế, xã hội, y tế
   - Phân tích mạng lưới xã hội về sự lây lan
   - Phân tích dữ liệu địa lý (GIS) về mẫu hình lây lan

4. **Giao diện người dùng**:
   - Tạo dashboard tương tác thay vì báo cáo tĩnh
   - Ứng dụng web để khám phá dữ liệu
   - Cập nhật dữ liệu theo thời gian thực

## Kết luận

Dự án này đã mô phỏng thành công quy trình phân tích Big Data cho dữ liệu dịch bệnh COVID-19. Từ việc tạo dữ liệu, lưu trữ, phân tích đến trực quan hóa, dự án đã cung cấp một quy trình hoàn chỉnh để đối phó với thách thức của việc xử lý dữ liệu lớn trong lĩnh vực dịch tễ học.

Mặc dù đây là một mô phỏng, các nguyên tắc và kỹ thuật được sử dụng trong dự án này có thể áp dụng cho các hệ thống Big Data thực tế, giúp các nhà nghiên cứu và hoạch định chính sách theo dõi và phân tích dữ liệu dịch bệnh hiệu quả. 