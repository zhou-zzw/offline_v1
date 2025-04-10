

create database lx_3_30_04;
use lx_3_30_04;



-- 创建品类销售分析表
CREATE TABLE category_sales_analysis (
     category_id VARCHAR(50) PRIMARY KEY,
     category_name VARCHAR(100),
     analysis_date DATE,
     total_sales DECIMAL(10, 2),
     total_quantity INT,
     monthly_payment_progress DECIMAL(10, 2),
     monthly_payment_contribution DECIMAL(5, 2),
     last_month_payment_rank INT,
     time_dimension VARCHAR(10)
);

-- 创建品类属性分析表
CREATE TABLE category_attribute_analysis (
     category_id VARCHAR(50),
     attribute_name VARCHAR(100),
     analysis_date DATE,
     traffic INT,
     payment_conversion_rate DECIMAL(5, 2),
     active_product_count INT,
     time_dimension VARCHAR(10),
     FOREIGN KEY (category_id) REFERENCES category_sales_analysis(category_id)
);

-- 创建品类流量分析表
CREATE TABLE category_traffic_analysis (
   category_id VARCHAR(50),
   traffic_source VARCHAR(50),
   analysis_date DATE,
   visitor_count INT,
   conversion_count INT,
   search_keyword VARCHAR(100),
   keyword_rank INT,
   keyword_visitor_count INT,
   FOREIGN KEY (category_id) REFERENCES category_sales_analysis(category_id)
);

-- 创建品类客群洞察表
CREATE TABLE category_customer_insight (
   category_id VARCHAR(50),
   user_type VARCHAR(50),
   age_group VARCHAR(50),
   gender VARCHAR(10),
   purchase_frequency VARCHAR(50),
   FOREIGN KEY (category_id) REFERENCES category_sales_analysis(category_id)
);


-- 向品类销售分析表插入 20 条数据
INSERT INTO category_sales_analysis (category_id, category_name, analysis_date, total_sales, total_quantity, monthly_payment_progress, monthly_payment_contribution, last_month_payment_rank, time_dimension)
VALUES
    ('C001', '女装', '2025-01-01', 5000.00, 200, 3000.00, 0.4, 3, '月'),
    ('C002', '男装', '2025-01-02', 4500.00, 180, 2800.00, 0.35, 4, '月'),
    ('C003', '童装', '2025-01-03', 3000.00, 120, 1800.00, 0.25, 6, '月'),
    ('C004', '家电', '2025-01-04', 8000.00, 50, 5000.00, 0.6, 1, '月'),
    ('C005', '数码', '2025-01-05', 6000.00, 80, 3800.00, 0.45, 2, '月'),
    ('C006', '食品', '2025-01-06', 2500.00, 300, 1500.00, 0.2, 7, '月'),
    ('C007', '美妆', '2025-01-07', 4000.00, 150, 2500.00, 0.3, 5, '月'),
    ('C008', '家居', '2025-01-08', 3500.00, 160, 2200.00, 0.28, 5, '月'),
    ('C009', '母婴', '2025-01-09', 2800.00, 220, 1700.00, 0.23, 6, '月'),
    ('C010', '运动', '2025-01-10', 5500.00, 190, 3300.00, 0.42, 3, '月'),
    ('C011', '箱包', '2025-01-11', 3200.00, 130, 2000.00, 0.26, 5, '月'),
    ('C012', '鞋履', '2025-01-12', 4200.00, 170, 2600.00, 0.32, 4, '月'),
    ('C013', '宠物', '2025-01-13', 1800.00, 250, 1100.00, 0.18, 8, '月'),
    ('C014', '珠宝', '2025-01-14', 7000.00, 40, 4200.00, 0.55, 2, '月'),
    ('C015', '文具', '2025-01-15', 1500.00, 400, 900.00, 0.15, 9, '月'),
    ('C016', '玩具', '2025-01-16', 2200.00, 280, 1300.00, 0.21, 7, '月'),
    ('C017', '乐器', '2025-01-17', 3800.00, 90, 2300.00, 0.31, 5, '月'),
    ('C018', '户外', '2025-01-18', 4800.00, 140, 2900.00, 0.37, 3, '月'),
    ('C019', '汽车用品', '2025-01-19', 6500.00, 60, 4000.00, 0.52, 2, '月'),
    ('C020', '家纺', '2025-01-20', 3600.00, 170, 2300.00, 0.3, 5, '月');

-- 向品类属性分析表插入 20 条数据
INSERT INTO category_attribute_analysis (category_id, attribute_name, analysis_date, traffic, payment_conversion_rate, active_product_count, time_dimension)
VALUES
    ('C001', '颜色:黑色', '2025-01-01', 1000, 0.08, 50, '月'),
    ('C002', '款式:休闲', '2025-01-02', 800, 0.07, 40, '月'),
    ('C003', '尺码:3-6岁', '2025-01-03', 600, 0.06, 30, '月'),
    ('C004', '功能:智能', '2025-01-04', 1200, 0.1, 60, '月'),
    ('C005', '品牌:知名', '2025-01-05', 900, 0.09, 45, '月'),
    ('C006', '口味:甜', '2025-01-06', 700, 0.05, 35, '月'),
    ('C007', '类型:口红', '2025-01-07', 1100, 0.085, 55, '月'),
    ('C008', '材质:木质', '2025-01-08', 850, 0.075, 42, '月'),
    ('C009', '适用年龄:0-1岁', '2025-01-09', 750, 0.065, 32, '月'),
    ('C010', '用途:跑步', '2025-01-10', 1050, 0.095, 52, '月'),
    ('C011', '风格:简约', '2025-01-11', 820, 0.072, 41, '月'),
    ('C012', '季节:春秋', '2025-01-12', 920, 0.082, 46, '月'),
    ('C013', '用品类型:猫粮', '2025-01-13', 650, 0.055, 33, '月'),
    ('C014', '材质:黄金', '2025-01-14', 1300, 0.11, 65, '月'),
    ('C015', '用途:书写', '2025-01-15', 720, 0.052, 36, '月'),
    ('C016', '类型:益智', '2025-01-16', 880, 0.078, 44, '月'),
    ('C017', '类型:吉他', '2025-01-17', 980, 0.088, 49, '月'),
    ('C018', '用途:露营', '2025-01-18', 1150, 0.098, 57, '月'),
    ('C019', '类型:座垫', '2025-01-19', 950, 0.083, 47, '月'),
    ('C020', '材质:纯棉', '2025-01-20', 870, 0.077, 43, '月');

-- 向品类流量分析表插入 20 条数据
INSERT INTO category_traffic_analysis (category_id, traffic_source, analysis_date, visitor_count, conversion_count, search_keyword, keyword_rank, keyword_visitor_count)
VALUES
    ('C001', '直通车', '2025-01-01', 800, 60, '时尚女装', 5, 1200),
    ('C002', '自然搜索', '2025-01-02', 700, 50, '男士休闲装', 6, 1100),
    ('C003', '直通车', '2025-01-03', 600, 40, '儿童服装', 7, 1000),
    ('C004', '自然搜索', '2025-01-04', 900, 70, '智能家电', 4, 1300),
    ('C005', '直通车', '2025-01-05', 850, 65, '知名数码产品', 5, 1250),
    ('C006', '自然搜索', '2025-01-06', 750, 55, '甜食', 6, 1150),
    ('C007', '直通车', '2025-01-07', 950, 75, '口红美妆', 4, 1350),
    ('C008', '自然搜索', '2025-01-08', 820, 62, '木质家居用品', 5, 1220),
    ('C009', '直通车', '2025-01-09', 720, 52, '婴儿用品', 6, 1120),
    ('C010', '自然搜索', '2025-01-10', 920, 72, '跑步运动装备', 4, 1320),
    ('C011', '直通车', '2025-01-11', 800, 60, '简约箱包', 5, 1200),
    ('C012', '自然搜索', '2025-01-12', 900, 70, '春秋鞋履', 4, 1300),
    ('C013', '直通车', '2025-01-13', 650, 45, '猫粮', 7, 1050),
    ('C014', '自然搜索', '2025-01-14', 1000, 80, '黄金珠宝', 3, 1400),
    ('C015', '直通车', '2025-01-15', 720, 52, '书写文具', 6, 1120),
    ('C016', '自然搜索', '2025-01-16', 880, 68, '益智玩具', 5, 1280),
    ('C017', '直通车', '2025-01-17', 980, 78, '吉他乐器', 4, 1380),
    ('C018', '自然搜索', '2025-01-18', 1050, 85, '露营户外用品', 3, 1450),
    ('C019', '直通车', '2025-01-19', 950, 75, '汽车座垫', 4, 1350),
    ('C020', '自然搜索', '2025-01-20', 870, 67, '纯棉家纺', 5, 1270);

-- 向品类客群洞察表插入 20 条数据
INSERT INTO category_customer_insight
(category_id, user_type, age_group, gender, purchase_frequency)
VALUES
    ('C001', '搜索人群', '20-30岁', '女', '偶尔购买'),
    ('C002', '浏览人群', '30-40岁', '男', '经常购买'),
    ('C003', '搜索人群', '有小孩家庭', '不限', '偶尔购买'),
    ('C004', '浏览人群', '25-35岁', '不限', '经常购买'),
    ('C005', '搜索人群', '20-25岁', '不限', '偶尔购买'),
    ('C006', '浏览人群', '所有年龄段', '不限', '经常购买'),
    ('C007', '搜索人群', '18-25岁', '女', '偶尔购买'),
    ('C008', '浏览人群', '35-45岁', '不限', '经常购买'),
    ('C009', '搜索人群', '有婴儿家庭', '不限', '偶尔购买'),
    ('C010', '浏览人群', '25-35岁', '不限', '经常购买'),
    ('C011', '搜索人群', '22-32岁', '不限', '偶尔购买'),
    ('C012', '浏览人群', '30-40岁', '不限', '经常购买'),
    ('C013', '搜索人群', '养宠物人群', '不限', '偶尔购买'),
    ('C014', '浏览人群', '35-50岁', '不限', '经常购买'),
    ('C015', '搜索人群', '学生', '不限', '偶尔购买'),
    ('C016', '浏览人群', '儿童及家长', '不限', '经常购买'),
    ('C017', '搜索人群', '音乐爱好者', '不限', '偶尔购买'),
    ('C018', '浏览人群', '户外运动爱好者', '不限', '经常购买'),
    ('C019', '搜索人群', '有车一族', '不限', '偶尔购买'),
    ('C020', '浏览人群', '家庭主妇', '女', '经常购买');







-- 计算品类销售转化率（销售分析指标）
SELECT
    category_id,
    conversion_count/visitor_count AS sales_conversion_rate
FROM
    category_traffic_analysis;

-- 计算属性流量占比（属性分析指标）
SELECT
    category_id,
    attribute_name,
    traffic / (SELECT SUM(traffic) FROM category_attribute_analysis WHERE category_id = caa.category_id) AS traffic_percentage
FROM
    category_attribute_analysis caa;

















