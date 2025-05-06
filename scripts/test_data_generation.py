#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kiểm tra việc sinh dữ liệu dịch bệnh với số lượng dòng nhỏ
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import time

# Đặt seed cho tính nhất quán
np.random.seed(42)
random.seed(42)

# Thông số cấu hình
NUM_ROWS = 1000  # Chỉ tạo 1000 dòng để kiểm tra
OUTPUT_FILE = "pandemic_data_sample.csv"

# Danh sách các quốc gia và châu lục
countries_by_continent = {
    "Asia": ["China", "India", "Japan", "South Korea", "Vietnam"],
    "Europe": ["Germany", "France", "UK", "Italy", "Spain"],
    "North America": ["USA", "Canada", "Mexico", "Cuba", "Panama"],
    "South America": ["Brazil", "Argentina", "Chile", "Colombia", "Peru"],
    "Africa": ["South Africa", "Egypt", "Nigeria", "Kenya", "Morocco"],
    "Oceania": ["Australia", "New Zealand", "Fiji", "Papua New Guinea", "Solomon Islands"]
}

# Tạo danh sách phẳng các quốc gia và ánh xạ quốc gia->châu lục
all_countries = []
country_to_continent = {}
for continent, countries in countries_by_continent.items():
    all_countries.extend(countries)
    for country in countries:
        country_to_continent[country] = continent

# Tạo danh sách các ngày từ 01/01/2020 đến 31/12/2023
start_date = datetime(2020, 1, 1)
days = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(365*4 + 1)]  # 4 năm

# Hàm tạo dữ liệu cho một quốc gia trong một thời gian cụ thể
def generate_country_data(country, continent, date_str, seed):
    np.random.seed(seed)
    
    # Mô phỏng sự biến đổi theo thời gian và các đợt lây nhiễm
    date = datetime.strptime(date_str, "%Y-%m-%d")
    days_since_start = (date - start_date).days
    
    # Tạo mô hình sóng dịch bệnh
    wave1 = np.exp(-((days_since_start - 100) ** 2) / 5000) * 10000  # Đợt dịch đầu tiên
    wave2 = np.exp(-((days_since_start - 300) ** 2) / 8000) * 20000  # Đợt dịch thứ hai
    wave3 = np.exp(-((days_since_start - 600) ** 2) / 10000) * 15000  # Đợt dịch thứ ba
    wave4 = np.exp(-((days_since_start - 900) ** 2) / 12000) * 8000   # Đợt dịch thứ tư
    
    # Thêm hiệu ứng theo mùa và ngẫu nhiên
    seasonal = 2000 * np.sin(days_since_start / 365 * 2 * np.pi)
    random_factor = np.random.normal(1, 0.2)
    
    # Yếu tố quy mô dân số dựa trên châu lục
    population_factor = {
        "Asia": 1.5,
        "Europe": 1.2,
        "North America": 1.3,
        "South America": 1.1,
        "Africa": 0.8,
        "Oceania": 0.6
    }[continent]
    
    # Tính số ca mới
    base_new_cases = max(0, (wave1 + wave2 + wave3 + wave4 + seasonal) * random_factor * population_factor)
    new_cases = int(base_new_cases) if base_new_cases < 1000000 else int(1000000)
    
    # Tính tỷ lệ tử vong và hồi phục (thay đổi theo thời gian khi y học phát triển)
    death_rate = max(0.01, 0.05 - (days_since_start / 5000))  # Giảm dần theo thời gian
    recovery_rate = min(0.98, 0.7 + (days_since_start / 4000))  # Tăng dần theo thời gian
    
    new_deaths = int(new_cases * death_rate * np.random.normal(1, 0.1))
    new_recovered = int(new_cases * recovery_rate * np.random.normal(1, 0.1))
    
    return {
        "date": date_str,
        "country": country,
        "continent": continent,
        "new_cases": new_cases,
        "new_deaths": new_deaths,
        "new_recovered": new_recovered
    }

# Hàm tính tổng tích lũy
def calculate_totals(df):
    temp_df = df.copy()
    # Sắp xếp theo quốc gia và ngày
    temp_df = temp_df.sort_values(by=['country', 'date'])
    
    # Tính tổng tích lũy cho mỗi quốc gia
    temp_df['total_cases'] = temp_df.groupby('country')['new_cases'].cumsum()
    temp_df['total_deaths'] = temp_df.groupby('country')['new_deaths'].cumsum()
    temp_df['total_recovered'] = temp_df.groupby('country')['new_recovered'].cumsum()
    
    return temp_df

def main():
    print(f"Bắt đầu tạo {NUM_ROWS:,} dòng dữ liệu dịch bệnh để kiểm tra")
    start_time = time.time()
    
    data = []
    for i in range(NUM_ROWS):
        # Chọn quốc gia và ngày ngẫu nhiên
        country = random.choice(all_countries)
        continent = country_to_continent[country]
        date_str = random.choice(days)
        
        # Tạo seed độc đáo cho mỗi cặp quốc gia/ngày để dữ liệu nhất quán
        unique_seed = hash(f"{country}_{date_str}") % (2**32)
        
        # Tạo dữ liệu
        row = generate_country_data(country, continent, date_str, unique_seed)
        data.append(row)
    
    # Tạo DataFrame
    df = pd.DataFrame(data)
    
    # Tính tổng tích lũy
    df = calculate_totals(df)
    
    # Ghi vào file CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\nQuá trình hoàn tất!")
    print(f"Tổng thời gian: {total_time:.2f} giây")
    print(f"Kích thước file: {os.path.getsize(OUTPUT_FILE) / (1024):.2f} KB")
    print(f"Số dòng tạo ra: {NUM_ROWS:,}")
    
    # Hiển thị 10 dòng đầu tiên của dữ liệu
    print("\nMẫu dữ liệu (10 dòng đầu tiên):")
    print(df.head(10))

if __name__ == "__main__":
    main() 