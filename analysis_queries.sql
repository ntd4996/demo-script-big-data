-- Tập hợp các truy vấn phân tích dữ liệu dịch bệnh COVID-19

-- 1. Tổng số ca mắc, tử vong, hồi phục trên toàn thế giới
SELECT 
    SUM(new_cases) AS total_cases_worldwide,
    SUM(new_deaths) AS total_deaths_worldwide,
    SUM(new_recovered) AS total_recovered_worldwide
FROM pandemic_data;

-- 2. Tổng số ca theo từng quốc gia
SELECT 
    country,
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    SUM(new_recovered) AS total_recovered
FROM pandemic_data
GROUP BY country
ORDER BY total_cases DESC;

-- 3. Top 10 quốc gia có số ca mắc cao nhất
SELECT 
    country,
    SUM(new_cases) AS total_cases
FROM pandemic_data
GROUP BY country
ORDER BY total_cases DESC
LIMIT 10;

-- 4. Top 10 quốc gia có số ca tử vong cao nhất
SELECT 
    country,
    SUM(new_deaths) AS total_deaths
FROM pandemic_data
GROUP BY country
ORDER BY total_deaths DESC
LIMIT 10;

-- 5. Tính tỷ lệ tử vong trung bình của từng quốc gia
SELECT 
    country,
    ROUND(SUM(new_deaths) * 100.0 / SUM(new_cases), 2) AS mortality_rate
FROM pandemic_data
GROUP BY country
HAVING SUM(new_cases) > 0
ORDER BY mortality_rate DESC;

-- 6. Tổng số ca mắc theo châu lục
SELECT 
    continent,
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    SUM(new_recovered) AS total_recovered
FROM pandemic_data
GROUP BY continent
ORDER BY total_cases DESC;

-- 7. Phân tích số ca mắc trung bình mỗi ngày
SELECT 
    date,
    AVG(new_cases) AS avg_daily_cases,
    AVG(new_deaths) AS avg_daily_deaths,
    AVG(new_recovered) AS avg_daily_recovered
FROM pandemic_data
GROUP BY date
ORDER BY date;

-- 8. Truy vấn số ca tử vong theo từng tháng
SELECT 
    SUBSTR(date, 1, 7) AS month,
    SUM(new_deaths) AS monthly_deaths
FROM pandemic_data
GROUP BY SUBSTR(date, 1, 7)
ORDER BY month;

-- 9. Phân tích xu hướng ca mắc theo từng năm
SELECT 
    SUBSTR(date, 1, 4) AS year,
    SUM(new_cases) AS yearly_cases,
    SUM(new_deaths) AS yearly_deaths,
    SUM(new_recovered) AS yearly_recovered
FROM pandemic_data
GROUP BY SUBSTR(date, 1, 4)
ORDER BY year;

-- 10. Top 5 quốc gia có tỷ lệ hồi phục cao nhất
SELECT 
    country,
    ROUND(SUM(new_recovered) * 100.0 / SUM(new_cases), 2) AS recovery_rate
FROM pandemic_data
GROUP BY country
HAVING SUM(new_cases) > 1000
ORDER BY recovery_rate DESC
LIMIT 5;

-- 11. Phân tích mức độ nghiêm trọng theo mùa (Quý)
SELECT 
    SUBSTR(date, 1, 4) || '-Q' || ((CAST(SUBSTR(date, 6, 2) AS INTEGER) + 2) / 3) AS quarter,
    SUM(new_cases) AS quarterly_cases,
    SUM(new_deaths) AS quarterly_deaths
FROM pandemic_data
GROUP BY quarter
ORDER BY quarter;

-- 12. So sánh dịch bệnh giữa các quốc gia lân cận
-- Ví dụ: các quốc gia Đông Nam Á
SELECT 
    country,
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    ROUND(SUM(new_deaths) * 100.0 / SUM(new_cases), 2) AS mortality_rate
FROM pandemic_data
WHERE country IN ('Vietnam', 'Thailand', 'Singapore', 'Malaysia', 'Indonesia', 'Philippines')
GROUP BY country
ORDER BY total_cases DESC;

-- 13. Phân tích xu hướng hàng tuần
SELECT 
    SUBSTR(date, 1, 4) || '-W' || (JULIANDAY(date) - JULIANDAY(SUBSTR(date, 1, 4) || '-01-01')) / 7 + 1 AS week,
    SUM(new_cases) AS weekly_cases,
    SUM(new_deaths) AS weekly_deaths
FROM pandemic_data
GROUP BY week
ORDER BY week
LIMIT 52;

-- 14. Tìm ngày có số ca mắc mới cao nhất
SELECT 
    date,
    SUM(new_cases) AS daily_cases
FROM pandemic_data
GROUP BY date
ORDER BY daily_cases DESC
LIMIT 10;

-- 15. Tính tỷ lệ tăng trưởng ca mắc mới theo tháng
WITH monthly_cases AS (
    SELECT 
        SUBSTR(date, 1, 7) AS month,
        SUM(new_cases) AS monthly_cases
    FROM pandemic_data
    GROUP BY month
    ORDER BY month
)
SELECT 
    m1.month,
    m1.monthly_cases,
    m1.monthly_cases - m2.monthly_cases AS growth,
    ROUND((m1.monthly_cases - m2.monthly_cases) * 100.0 / m2.monthly_cases, 2) AS growth_rate
FROM monthly_cases m1
JOIN monthly_cases m2 ON m1.month = (
    SELECT month FROM monthly_cases 
    WHERE month > m2.month 
    ORDER BY month ASC 
    LIMIT 1
)
ORDER BY m1.month; 