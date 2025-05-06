#!/bin/bash

# Script chuẩn bị file nộp bài

echo "===== CHUẨN BỊ FILE NỘP BÀI ====="

# Tạo thư mục nộp bài
SUBMISSION_DIR="pandemic_analysis_submission"
mkdir -p "$SUBMISSION_DIR"

# Sao chép mã nguồn Python
echo "Sao chép mã nguồn Python..."
mkdir -p "$SUBMISSION_DIR/scripts"
cp generate_pandemic_data.py "$SUBMISSION_DIR/scripts/"
cp simulate_hadoop.py "$SUBMISSION_DIR/scripts/"
cp simulate_analysis.py "$SUBMISSION_DIR/scripts/"
cp generate_report.py "$SUBMISSION_DIR/scripts/"

# Sao chép file CSV mẫu
echo "Sao chép mẫu dữ liệu CSV..."
if [ -f "pandemic_data.csv" ]; then
    head -n 1000 pandemic_data.csv > "$SUBMISSION_DIR/pandemic_data_sample.csv"
fi

# Sao chép các biểu đồ
echo "Sao chép các biểu đồ..."
mkdir -p "$SUBMISSION_DIR/visualizations"
if [ -d "visualizations" ]; then
    cp visualizations/*.png "$SUBMISSION_DIR/visualizations/"
fi

# Sao chép báo cáo
echo "Sao chép báo cáo..."
cp big_data_covid_report.md "$SUBMISSION_DIR/"

# Sao chép HDFS và Hive mô phỏng
echo "Sao chép kết quả mô phỏng HDFS và Hive..."
if [ -d "hdfs_simulation" ]; then
    mkdir -p "$SUBMISSION_DIR/hdfs_simulation"
    cp -r hdfs_simulation/* "$SUBMISSION_DIR/hdfs_simulation/"
fi

if [ -f "hive_simulation.db" ]; then
    cp hive_simulation.db "$SUBMISSION_DIR/"
fi

# Sao chép các file SQL và tập lệnh
echo "Sao chép các file SQL và tập lệnh..."
cp analysis_queries.sql "$SUBMISSION_DIR/"
cp create_hive_table.hql "$SUBMISSION_DIR/"
cp setup_hdfs.sh "$SUBMISSION_DIR/"
cp run_analysis.sh "$SUBMISSION_DIR/"
cp run_spark.sh "$SUBMISSION_DIR/"

# Tạo file README cho nộp bài
echo "Tạo file README cho nộp bài..."
cat > "$SUBMISSION_DIR/README.md" << EOL
# Phân tích Dữ liệu Dịch bệnh COVID-19

## Tổng quan dự án

Dự án này thực hiện phân tích tình hình dịch bệnh COVID-19 toàn cầu sử dụng các công nghệ Big Data. Dự án bao gồm toàn bộ quy trình từ việc sinh dữ liệu, lưu trữ, xử lý, phân tích đến trực quan hóa kết quả.

## Kiến trúc hệ thống

Dự án mô phỏng kiến trúc một hệ thống Big Data với các thành phần:

- **Sinh dữ liệu**: Tạo dữ liệu mô phỏng dịch bệnh với khối lượng lớn
- **HDFS**: Mô phỏng lưu trữ dữ liệu phân tán Hadoop
- **Hive**: Mô phỏng kho dữ liệu và xử lý truy vấn SQL
- **Phân tích**: Thực hiện các truy vấn phân tích trên dữ liệu
- **Trực quan hóa**: Tạo biểu đồ từ kết quả phân tích
- **Báo cáo**: Tổng hợp kết quả thành báo cáo hoàn chỉnh

## Cấu trúc thư mục

\`\`\`
pandemic_analysis_submission/
├── big_data_covid_report.md     # Báo cáo tổng hợp
├── pandemic_data_sample.csv     # Mẫu dữ liệu (1000 dòng)
├── hive_simulation.db           # Database SQLite mô phỏng Hive
├── analysis_queries.sql         # Tập hợp các truy vấn phân tích
├── create_hive_table.hql        # Lệnh tạo bảng Hive
├── setup_hdfs.sh                # Script thiết lập HDFS
├── run_analysis.sh              # Script chạy phân tích
├── run_spark.sh                 # Script chạy phân tích trên Spark
├── README.md                    # File hướng dẫn này
├── scripts/                     # Thư mục chứa mã nguồn Python
│   ├── generate_pandemic_data.py  # Script sinh dữ liệu
│   ├── simulate_hadoop.py         # Script mô phỏng HDFS và Hive
│   ├── simulate_analysis.py       # Script phân tích dữ liệu
│   └── generate_report.py         # Script tạo báo cáo
├── visualizations/              # Thư mục chứa biểu đồ
│   ├── top10_countries_cases.png
│   ├── top10_countries_deaths.png
│   ├── continent_cases_pie.png
│   ├── continent_cases_bar.png
│   ├── monthly_deaths.png
│   └── yearly_trend.png
└── hdfs_simulation/             # Dữ liệu mô phỏng HDFS
    └── user/
        └── pandemic_data/
            └── pandemic_data.csv
\`\`\`

## Chi tiết các thành phần

### 1. Sinh dữ liệu (generate_pandemic_data.py)

Script này tạo dữ liệu mô phỏng dịch bệnh với các đặc điểm:

- **Cấu trúc dữ liệu**: date, country, continent, new_cases, new_deaths, new_recovered, total_cases, total_deaths, total_recovered
- **Phạm vi thời gian**: 01/01/2020 - 31/12/2023 (4 năm)
- **Phạm vi không gian**: 60 quốc gia thuộc 6 châu lục
- **Mô hình dữ liệu**: Tạo mô hình các đợt sóng dịch bệnh theo thời gian với hiệu ứng theo mùa
- **Xử lý theo lô (batch)**: Xử lý dữ liệu theo từng lô 20,000 dòng để tiết kiệm bộ nhớ
- **Khả năng mở rộng**: Ban đầu thiết kế cho 50 triệu dòng (~5-10GB), có thể điều chỉnh thông số

### 2. Mô phỏng HDFS và Hive (simulate_hadoop.py)

Script này mô phỏng quá trình làm việc với hệ sinh thái Hadoop:

- **Mô phỏng HDFS**: Tạo cấu trúc thư mục giống HDFS, thực hiện việc "upload" dữ liệu
- **Mô phỏng Hive**: Sử dụng SQLite làm backend để mô phỏng Hive
- **Schema**: Tạo schema phù hợp cho dữ liệu dịch bệnh
- **Import dữ liệu**: Đưa dữ liệu từ file CSV vào bảng Hive (SQLite)

### 3. Phân tích dữ liệu (simulate_analysis.py)

Thực hiện các truy vấn phân tích trên dữ liệu:

- **10 truy vấn phân tích**: Tổng số ca mắc/tử vong toàn cầu, top 10 quốc gia, tỷ lệ tử vong, phân tích theo châu lục, theo thời gian...
- **Hiệu năng**: Ghi lại thời gian thực thi từng truy vấn
- **Kết quả**: Hiển thị kết quả dạng bảng

### 4. Trực quan hóa kết quả

Tạo các biểu đồ minh họa kết quả phân tích:

- **Biểu đồ cột**: Top 10 quốc gia có số ca mắc/tử vong cao nhất, phân bố theo châu lục
- **Biểu đồ đường**: Xu hướng theo thời gian (tháng, năm)
- **Biểu đồ tròn**: Tỷ lệ ca mắc theo châu lục
- **Biểu đồ kết hợp**: Xu hướng dịch bệnh theo năm (ca mắc, tử vong, hồi phục)

### 5. Tạo báo cáo (generate_report.py)

Tổng hợp tất cả kết quả vào báo cáo Markdown:

- **Giới thiệu**: Mô tả tổng quan về dự án
- **Dữ liệu**: Thông tin về dữ liệu, các mẫu và thống kê
- **Phân tích**: Kết quả của 10 truy vấn phân tích
- **Trực quan hóa**: Đính kèm các biểu đồ kết quả
- **Kết luận**: Rút ra các nhận xét từ kết quả phân tích

## Hướng dẫn sử dụng

### Cài đặt môi trường

1. Yêu cầu Python 3.6+ và các thư viện:

\`\`\`bash
# Nâng cấp pip
python -m pip install --upgrade pip

# Tạo và kích hoạt môi trường ảo (tùy chọn nhưng khuyến khích)
python -m venv venv
source venv/bin/activate  # Trên Linux/Mac
# hoặc
venv\\Scripts\\activate  # Trên Windows

# Cài đặt các thư viện cần thiết
pip install pandas numpy matplotlib pillow
\`\`\`

### Chạy từng bước

1. **Sinh dữ liệu**:
\`\`\`bash
python scripts/generate_pandemic_data.py
\`\`\`
- Kết quả: File pandemic_data.csv (có thể điều chỉnh số lượng dòng trong script)

2. **Mô phỏng HDFS và Hive**:
\`\`\`bash
python scripts/simulate_hadoop.py
\`\`\`
- Kết quả: Thư mục hdfs_simulation và file hive_simulation.db

3. **Phân tích dữ liệu**:
\`\`\`bash
python scripts/simulate_analysis.py
\`\`\`
- Kết quả: Các truy vấn được thực thi và biểu đồ được lưu trong thư mục visualizations/

4. **Tạo báo cáo**:
\`\`\`bash
python scripts/generate_report.py
\`\`\`
- Kết quả: File big_data_covid_report.md

### Hoặc chạy toàn bộ quy trình

\`\`\`bash
# Cấp quyền thực thi cho script nếu cần
chmod +x run_analysis.sh

# Chạy toàn bộ quy trình
./run_analysis.sh
\`\`\`

## Mở rộng và cải tiến

Dự án có thể được mở rộng theo các hướng:

1. **Sử dụng dữ liệu thực**: Thay thế dữ liệu mô phỏng bằng dữ liệu thực từ các nguồn như JHU CSSE, WHO, Our World in Data...
2. **Triển khai Hadoop thực tế**: Triển khai trên cụm Hadoop thực tế thay vì mô phỏng
3. **Phân tích nâng cao**: Thêm các phân tích như:
   - Dự đoán xu hướng dịch bệnh sử dụng ML
   - Phân tích tương quan với các yếu tố kinh tế, xã hội
   - Phân tích dữ liệu địa lý (GIS) về sự lây lan
4. **Dashboard trực quan**: Tạo dashboard tương tác thay vì báo cáo tĩnh

## Tác giả

[Tên của bạn]

## Giấy phép

Dự án này được phân phối dưới giấy phép MIT.
EOL

# Tạo file ZIP cho nộp bài
echo "Tạo file ZIP cho nộp bài..."
zip -r pandemic_analysis_submission.zip "$SUBMISSION_DIR"

echo "===== HOÀN TẤT ====="
echo "File nộp bài đã được tạo: pandemic_analysis_submission.zip"
echo "Thư mục nộp bài: $SUBMISSION_DIR" 