#!/bin/bash

# Script để chạy toàn bộ quy trình phân tích dữ liệu dịch bệnh

echo "===== BẮT ĐẦU QUY TRÌNH PHÂN TÍCH DỮ LIỆU DỊCH BỆNH ====="

# Đường dẫn đến thư mục hiện tại
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Tạo thư mục logs để lưu nhật ký
mkdir -p logs

# 1. Sinh dữ liệu
echo "===== 1. SINH DỮ LIỆU DỊCH BỆNH ====="
echo "Bắt đầu sinh dữ liệu dịch bệnh với kích thước 5-10GB..."
python3 generate_pandemic_data.py | tee logs/generate_data.log

# Kiểm tra nếu file dữ liệu đã được tạo
if [ ! -f "pandemic_data.csv" ]; then
    echo "Không thể tạo file dữ liệu pandemic_data.csv. Vui lòng kiểm tra lỗi."
    exit 1
fi

# Hiển thị kích thước file
DATA_SIZE=$(du -h pandemic_data.csv | cut -f1)
echo "Kích thước file dữ liệu: $DATA_SIZE"

# 2. Thiết lập và tải dữ liệu lên HDFS
echo "===== 2. CÀI ĐẶT HDFS & TẢI DỮ LIỆU ====="
echo "Thiết lập HDFS và tải dữ liệu lên..."
bash setup_hdfs.sh | tee logs/setup_hdfs.log

# 3. Tạo bảng Hive
echo "===== 3. TẠO BẢNG HIVE ====="
echo "Tạo bảng Hive để ánh xạ với dữ liệu CSV..."
# Kết nối Hive và thực thi script
beeline -u jdbc:hive2://localhost:10000 -f create_hive_table.hql | tee logs/create_hive_table.log

# 4. Thực hiện các truy vấn phân tích
echo "===== 4. PHÂN TÍCH DỮ LIỆU BẰNG HIVE ====="
echo "Thực hiện các truy vấn phân tích dữ liệu..."
bash run_analysis.sh | tee logs/hive_analysis.log

# 5. Phân tích dữ liệu bằng PySpark
echo "===== 5. PHÂN TÍCH DỮ LIỆU BẰNG PYSPARK ====="
echo "Thực hiện phân tích bằng PySpark và tạo biểu đồ..."
bash run_spark.sh | tee logs/spark_analysis.log

# Tổng kết
echo "===== TỔNG KẾT QUÁ TRÌNH ====="
echo "Quy trình phân tích dữ liệu dịch bệnh đã hoàn tất!"
echo "Dữ liệu đã được tạo với kích thước: $DATA_SIZE"
echo "Các file kết quả được lưu trong thư mục hiện tại và thư mục 'visualizations'"
echo "Logs được lưu trong thư mục 'logs'"

# Liệt kê các file kết quả
echo "Các file dữ liệu và kết quả:"
ls -lh pandemic_data.csv visualizations/ 2>/dev/null

echo "===== HOÀN TẤT =====" 