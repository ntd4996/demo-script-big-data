#!/bin/bash

#############################################
# Script thiết lập HDFS cho dự án phân tích #
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
echo "          THIẾT LẬP HDFS CHO PHÂN TÍCH DỮ LIỆU COVID-19"
echo "================================================================="
echo -e "${NC}"

# Kiểm tra Hadoop đã được cài đặt
echo -e "${BLUE}[1/6] Kiểm tra môi trường Hadoop...${NC}"
command -v hadoop >/dev/null 2>&1 || { 
    echo -e "${RED}Hadoop không được cài đặt hoặc không nằm trong PATH.${NC}"
    echo -e "${YELLOW}Script này yêu cầu Hadoop được cài đặt và cấu hình đúng.${NC}"
    echo -e "${YELLOW}Bạn có muốn tiếp tục với mô phỏng Hadoop thay vì Hadoop thực tế không? (y/n)${NC}"
    read -r simulate
    if [[ "$simulate" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo -e "${GREEN}Chuyển sang chế độ mô phỏng Hadoop.${NC}"
        SIMULATE_MODE=true
    else
        echo -e "${RED}Thoát cài đặt.${NC}"
        exit 1
    fi
}

# Kiểm tra HDFS đã khởi động chưa
if [ "$SIMULATE_MODE" != true ]; then
    echo -e "${BLUE}[2/6] Kiểm tra trạng thái HDFS...${NC}"
    hdfs dfsadmin -report >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}HDFS chưa được khởi động.${NC}"
        echo -e "${YELLOW}Bạn có muốn khởi động HDFS không? (y/n)${NC}"
        read -r start_hdfs
        if [[ "$start_hdfs" =~ ^([yY][eE][sS]|[yY])$ ]]; then
            echo -e "${GREEN}Đang khởi động HDFS...${NC}"
            start-dfs.sh
            sleep 5
            hdfs dfsadmin -report >/dev/null 2>&1
            if [ $? -ne 0 ]; then
                echo -e "${RED}Không thể khởi động HDFS. Vui lòng kiểm tra cấu hình Hadoop.${NC}"
                echo -e "${YELLOW}Chuyển sang chế độ mô phỏng Hadoop.${NC}"
                SIMULATE_MODE=true
            else
                echo -e "${GREEN}HDFS đã được khởi động thành công.${NC}"
            fi
        else
            echo -e "${YELLOW}Chuyển sang chế độ mô phỏng Hadoop.${NC}"
            SIMULATE_MODE=true
        fi
    else
        echo -e "${GREEN}HDFS đã được khởi động.${NC}"
    fi
else
    echo -e "${BLUE}[2/6] Chế độ mô phỏng: Chuẩn bị môi trường...${NC}"
    # Tạo thư mục mô phỏng HDFS
    mkdir -p hdfs_simulation/user/pandemic_data
    echo -e "${GREEN}Đã tạo thư mục mô phỏng HDFS.${NC}"
fi

# Tạo thư mục trên HDFS
echo -e "${BLUE}[3/6] Tạo thư mục trên HDFS...${NC}"
if [ "$SIMULATE_MODE" != true ]; then
    # Tạo thư mục trên HDFS thực tế
    hadoop fs -test -d /user/pandemic_data
    if [ $? -ne 0 ]; then
        hadoop fs -mkdir -p /user/pandemic_data
        echo -e "${GREEN}Đã tạo thư mục /user/pandemic_data trên HDFS.${NC}"
    else
        echo -e "${YELLOW}Thư mục /user/pandemic_data đã tồn tại trên HDFS.${NC}"
    fi
else
    echo -e "${GREEN}(Mô phỏng) Đã tạo thư mục /user/pandemic_data trên HDFS.${NC}"
fi

# Kiểm tra file dữ liệu tồn tại
echo -e "${BLUE}[4/6] Kiểm tra file dữ liệu...${NC}"
if [ ! -f "pandemic_data.csv" ]; then
    echo -e "${YELLOW}File pandemic_data.csv không tồn tại.${NC}"
    echo -e "${YELLOW}Bạn có muốn chạy script generate_pandemic_data.py để tạo dữ liệu không? (y/n)${NC}"
    read -r generate_data
    if [[ "$generate_data" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo -e "${GREEN}Đang tạo dữ liệu...${NC}"
        python3 scripts/generate_pandemic_data.py
        if [ ! -f "pandemic_data.csv" ]; then
            echo -e "${RED}Không thể tạo file dữ liệu. Thoát cài đặt.${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Không có file dữ liệu. Thoát cài đặt.${NC}"
        exit 1
    fi
fi

# Tải dữ liệu lên HDFS
echo -e "${BLUE}[5/6] Tải dữ liệu lên HDFS...${NC}"
if [ "$SIMULATE_MODE" != true ]; then
    # Tải lên HDFS thực tế
    hadoop fs -test -f /user/pandemic_data/pandemic_data.csv
    if [ $? -ne 0 ]; then
        echo -e "${GREEN}Đang tải file lên HDFS...${NC}"
        hadoop fs -put -f pandemic_data.csv /user/pandemic_data/
        echo -e "${GREEN}Đã tải file lên HDFS thành công.${NC}"
    else
        echo -e "${YELLOW}File đã tồn tại trên HDFS. Bạn có muốn ghi đè không? (y/n)${NC}"
        read -r overwrite
        if [[ "$overwrite" =~ ^([yY][eE][sS]|[yY])$ ]]; then
            echo -e "${GREEN}Đang tải file lên HDFS (ghi đè)...${NC}"
            hadoop fs -put -f pandemic_data.csv /user/pandemic_data/
            echo -e "${GREEN}Đã tải file lên HDFS thành công.${NC}"
        else
            echo -e "${GREEN}Giữ nguyên file hiện có trên HDFS.${NC}"
        fi
    fi
    
    # Hiển thị thông tin file trên HDFS
    echo -e "${BLUE}Thông tin file trên HDFS:${NC}"
    hadoop fs -ls -h /user/pandemic_data/
    hadoop fs -du -s -h /user/pandemic_data/
else
    # Mô phỏng tải lên HDFS
    cp pandemic_data.csv hdfs_simulation/user/pandemic_data/
    echo -e "${GREEN}(Mô phỏng) Đã tải file lên HDFS thành công.${NC}"
    
    # Hiển thị thông tin file
    echo -e "${BLUE}Thông tin file (mô phỏng):${NC}"
    ls -la hdfs_simulation/user/pandemic_data/
    du -h hdfs_simulation/user/pandemic_data/pandemic_data.csv
fi

# Tạo bảng Hive
echo -e "${BLUE}[6/6] Kiểm tra Hive và tạo bảng...${NC}"
if [ "$SIMULATE_MODE" != true ]; then
    # Kiểm tra Hive
    command -v hive >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}Hive không được cài đặt hoặc không nằm trong PATH.${NC}"
        echo -e "${YELLOW}Chuyển sang chế độ mô phỏng Hive.${NC}"
        SIMULATE_HIVE=true
    else
        echo -e "${GREEN}Hive đã được cài đặt.${NC}"
        # Tạo bảng Hive thực tế
        echo -e "${GREEN}Đang tạo bảng Hive...${NC}"
        hive -f create_hive_table.hql
        echo -e "${GREEN}Đã tạo bảng Hive thành công.${NC}"
        
        # Kiểm tra bảng đã tạo
        echo -e "${BLUE}Danh sách bảng trong Hive:${NC}"
        hive -e "USE pandemic_analysis; SHOW TABLES;"
    fi
else
    echo -e "${GREEN}(Mô phỏng) Đang tạo bảng Hive...${NC}"
    python3 scripts/simulate_hadoop.py
    echo -e "${GREEN}(Mô phỏng) Đã tạo bảng Hive thành công.${NC}"
fi

# Hiển thị tóm tắt
echo -e "\n${GREEN}==========================================================${NC}"
echo -e "${GREEN}                THIẾT LẬP HDFS HOÀN TẤT                  ${NC}"
echo -e "${GREEN}==========================================================${NC}"

if [ "$SIMULATE_MODE" == true ]; then
    echo -e "\n${YELLOW}Tóm tắt (CHẾ ĐỘ MÔ PHỎNG):${NC}"
    echo -e "- Thư mục mô phỏng HDFS: ${BLUE}hdfs_simulation/user/pandemic_data/${NC}"
    echo -e "- File dữ liệu: ${BLUE}hdfs_simulation/user/pandemic_data/pandemic_data.csv${NC}"
    echo -e "- Database mô phỏng Hive: ${BLUE}hive_simulation.db${NC}"
    
    echo -e "\n${YELLOW}Bước tiếp theo:${NC}"
    echo -e "Chạy script phân tích để xử lý dữ liệu mô phỏng:"
    echo -e "${BLUE}./run_analysis.sh${NC}"
else
    echo -e "\n${YELLOW}Tóm tắt:${NC}"
    echo -e "- Thư mục HDFS: ${BLUE}/user/pandemic_data/${NC}"
    echo -e "- File dữ liệu trên HDFS: ${BLUE}/user/pandemic_data/pandemic_data.csv${NC}"
    echo -e "- Cơ sở dữ liệu Hive: ${BLUE}pandemic_analysis${NC}"
    echo -e "- Bảng Hive: ${BLUE}pandemic_data, pandemic_data_optimized, pandemic_data_partitioned${NC}"
    
    echo -e "\n${YELLOW}Bước tiếp theo:${NC}"
    echo -e "1. Chạy các truy vấn phân tích trên Hive:"
    echo -e "${BLUE}hive -f analysis_queries.sql${NC}"
    echo -e "hoặc"
    echo -e "2. Chạy script phân tích tự động:"
    echo -e "${BLUE}./run_analysis.sh${NC}"
fi

echo -e "\n${GREEN}Hoàn tất!${NC}" 