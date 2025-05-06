#!/bin/bash

#############################################
# Script chạy toàn bộ quy trình phân tích   #
# dữ liệu dịch bệnh COVID-19                #
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
echo "       PHÂN TÍCH DỮ LIỆU DỊCH BỆNH COVID-19 VỚI BIG DATA"
echo "================================================================="
echo -e "${NC}"

# Kiểm tra Python và các thư viện cần thiết
echo -e "${BLUE}[1/6] Kiểm tra môi trường Python...${NC}"
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}Python 3 không được cài đặt. Vui lòng cài đặt Python 3.${NC}"; exit 1; }

# Kiểm tra thư viện cần thiết
echo -e "${BLUE}[2/6] Kiểm tra các thư viện Python cần thiết...${NC}"
python3 -c "import pandas, numpy, matplotlib, sqlite3" 2>/dev/null || {
    echo -e "${YELLOW}Một số thư viện Python cần thiết chưa được cài đặt.${NC}"
    echo -e "${YELLOW}Đang cài đặt thư viện...${NC}"
    pip install pandas numpy matplotlib pillow
}

# Bước 1: Tạo dữ liệu mô phỏng
echo -e "\n${BLUE}[3/6] Bước 1: Tạo dữ liệu mô phỏng dịch bệnh...${NC}"
if [ -f "pandemic_data.csv" ]; then
    echo -e "${YELLOW}File pandemic_data.csv đã tồn tại. Bạn có muốn tạo lại không? (y/n)${NC}"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        python3 scripts/generate_pandemic_data.py
    else
        echo -e "${GREEN}Bỏ qua bước tạo dữ liệu.${NC}"
    fi
else
    python3 scripts/generate_pandemic_data.py
fi

# Kiểm tra file tạo thành công
if [ ! -f "pandemic_data.csv" ]; then
    echo -e "${RED}Không thể tạo file dữ liệu. Vui lòng kiểm tra lại.${NC}"
    exit 1
fi

# Bước 2: Mô phỏng HDFS và Hive
echo -e "\n${BLUE}[4/6] Bước 2: Mô phỏng HDFS và Hive...${NC}"
python3 scripts/simulate_hadoop.py

# Kiểm tra file database tạo thành công
if [ ! -f "hive_simulation.db" ]; then
    echo -e "${RED}Không thể tạo file database. Vui lòng kiểm tra lại.${NC}"
    exit 1
fi

# Bước 3: Phân tích dữ liệu
echo -e "\n${BLUE}[5/6] Bước 3: Phân tích dữ liệu...${NC}"
# Tạo thư mục visualizations nếu chưa tồn tại
mkdir -p visualizations
python3 scripts/simulate_analysis.py

# Kiểm tra các biểu đồ đã được tạo
if [ ! "$(ls -A visualizations 2>/dev/null)" ]; then
    echo -e "${RED}Không thể tạo biểu đồ. Vui lòng kiểm tra lại.${NC}"
    exit 1
fi

# Bước 4: Tạo báo cáo
echo -e "\n${BLUE}[6/6] Bước 4: Tạo báo cáo tổng hợp...${NC}"
python3 scripts/generate_report.py

# Kiểm tra báo cáo đã được tạo
if [ ! -f "big_data_covid_report.md" ]; then
    echo -e "${RED}Không thể tạo báo cáo. Vui lòng kiểm tra lại.${NC}"
    exit 1
fi

# Hiển thị thông tin tóm tắt
echo -e "\n${GREEN}==========================================================${NC}"
echo -e "${GREEN}             QUÁ TRÌNH PHÂN TÍCH HOÀN TẤT                ${NC}"
echo -e "${GREEN}==========================================================${NC}"

echo -e "\n${YELLOW}Tóm tắt:${NC}"
echo -e "- File dữ liệu: $(du -h pandemic_data.csv | cut -f1) ($(wc -l < pandemic_data.csv) dòng)"
echo -e "- Database Hive: $(du -h hive_simulation.db | cut -f1)"
echo -e "- Số biểu đồ: $(ls -1 visualizations | wc -l)"
echo -e "- Báo cáo: big_data_covid_report.md ($(du -h big_data_covid_report.md | cut -f1))"

echo -e "\n${YELLOW}Các file tạo ra:${NC}"
echo -e "- Dữ liệu: ${BLUE}pandemic_data.csv${NC}"
echo -e "- Database: ${BLUE}hive_simulation.db${NC}"
echo -e "- Biểu đồ: ${BLUE}visualizations/*.png${NC}"
echo -e "- Báo cáo: ${BLUE}big_data_covid_report.md${NC}"

echo -e "\n${YELLOW}Để xem báo cáo, bạn có thể dùng các lệnh:${NC}"
echo -e "- Xem trong terminal: ${BLUE}less big_data_covid_report.md${NC}"
echo -e "- Chuyển sang HTML (nếu có pandoc): ${BLUE}pandoc -s big_data_covid_report.md -o report.html${NC}"

echo -e "\n${YELLOW}Bạn có muốn đóng gói bài nộp không? (y/n)${NC}"
read -r package_response
if [[ "$package_response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    # Cấp quyền thực thi cho file prepare_submission.sh
    chmod +x prepare_submission.sh
    # Chạy script đóng gói
    ./prepare_submission.sh
fi

echo -e "\n${GREEN}Hoàn tất!${NC}" 