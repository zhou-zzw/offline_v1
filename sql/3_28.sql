create database lx3_28;
use lx3_28;



-- 1.商品表（tb_product）
CREATE TABLE tb_product (
                            product_id INT PRIMARY KEY,
                            product_name VARCHAR(255),
                            price DECIMAL(10, 2),
                            category VARCHAR(255),
                            brand VARCHAR(255)
);






-- 2.销售表（tb_sales）

CREATE TABLE tb_sales (
                          sales_id INT PRIMARY KEY,
                          product_id INT,
                          sales_date DATE,
                          sales_quantity INT,
                          sales_amount DECIMAL(10, 2),
                          buyer_id INT,
                          FOREIGN KEY (product_id) REFERENCES tb_product(product_id)
);




-- 3.流量表（tb_traffic）

CREATE TABLE tb_traffic (
                            traffic_id INT PRIMARY KEY,
                            product_id INT,
                            traffic_channel VARCHAR(255),
                            access_time TIMESTAMP,
                            visitor_id INT,
                            is_converted BOOLEAN,
                            FOREIGN KEY (product_id) REFERENCES tb_product(product_id)
);



-- 4.评价表（tb_evaluation）

CREATE TABLE tb_evaluation (
                               evaluation_id INT PRIMARY KEY,
                               product_id INT,
                               buyer_id INT,
                               evaluation_date DATE,
                               score INT,
                               evaluation_content TEXT,
                               is_active_evaluation BOOLEAN,
                               FOREIGN KEY (product_id) REFERENCES tb_product(product_id)
);


-- 5.搜索词表（tb_search_term）
CREATE TABLE tb_search_term (
                                search_term_id INT PRIMARY KEY,
                                product_id INT,
                                search_term VARCHAR(255),
                                search_count INT,
                                traffic_count INT,
                                conversion_rate DECIMAL(5, 2),
                                FOREIGN KEY (product_id) REFERENCES tb_product(product_id)
);

-- 6.内容表（tb_content）
CREATE TABLE tb_content (
                            content_id INT PRIMARY KEY,
                            product_id INT,
                            content_type VARCHAR(255),
                            content_title VARCHAR(255),
                            play_count INT,
                            interaction_count INT,
                            FOREIGN KEY (product_id) REFERENCES tb_product(product_id)
);



-- 向商品表（tb_product）插入数据
INSERT INTO tb_product (product_id, product_name, price, category, brand)
VALUES
    (1, 'iPhone 15', 7999.00, '智能手机', 'Apple'),
    (2, 'Galaxy S24', 6999.00, '智能手机', 'Samsung'),
    (3, 'MacBook Pro', 19999.00, '笔记本电脑', 'Apple'),
    (4, 'Surface Pro 9', 9999.00, '笔记本电脑', 'Microsoft'),
    (5, 'AirPods Pro 2', 1999.00, '无线耳机', 'Apple');

-- 向销售表（tb_sales）插入数据
INSERT INTO tb_sales (sales_id, product_id, sales_date, sales_quantity, sales_amount, buyer_id)
VALUES
    (1, 1, '2025-03-01', 10, 79990.00, 101),
    (2, 2, '2025-03-02', 5, 34995.00, 102),
    (3, 3, '2025-03-03', 3, 59997.00, 103),
    (4, 4, '2025-03-04', 7, 69993.00, 104),
    (5, 5, '2025-03-05', 15, 29985.00, 105);

-- 向流量表（tb_traffic）插入数据
INSERT INTO tb_traffic (traffic_id, product_id, traffic_channel, access_time, visitor_id, is_converted)
VALUES
    (1, 1, '搜索引擎', '2025-03-01 10:00:00', 201, true),
    (2, 2, '社交媒体', '2025-03-02 14:30:00', 202, false),
    (3, 3, '电商平台广告', '2025-03-03 16:15:00', 203, true),
    (4, 4, '搜索引擎', '2025-03-04 09:45:00', 204, false),
    (5, 5, '社交媒体', '2025-03-05 11:30:00', 205, true);

-- 向评价表（tb_evaluation）插入数据
INSERT INTO tb_evaluation (evaluation_id, product_id, buyer_id, evaluation_date, score, evaluation_content, is_active_evaluation)
VALUES
    (1, 1, 101, '2025-03-06', 5, '非常好用，性能强劲！', true),
    (2, 2, 102, '2025-03-07', 4, '屏幕显示效果不错。', true),
    (3, 3, 103, '2025-03-08', 5, '工作利器，很满意！', false),
    (4, 4, 104, '2025-03-09', 3, '续航能力有待提高。', true),
    (5, 5, 105, '2025-03-10', 5, '音质出色，佩戴舒适。', false);

-- 向搜索词表（tb_search_term）插入数据
INSERT INTO tb_search_term (search_term_id, product_id, search_term, search_count, traffic_count, conversion_rate)
VALUES
    (1, 1, 'iPhone 15', 100, 30, 0.3),
    (2, 2, 'Galaxy S24', 80, 20, 0.25),
    (3, 3, 'MacBook Pro', 60, 15, 0.25),
    (4, 4, 'Surface Pro 9', 70, 20, 0.28),
    (5, 5, 'AirPods Pro 2', 90, 25, 0.28);

-- 向内容表（tb_content）插入数据
INSERT INTO tb_content (content_id, product_id, content_type, content_title, play_count, interaction_count)
VALUES
    (1, 1, '视频评测', 'iPhone 15 全面评测', 5000, 200),
    (2, 2, '图文介绍', 'Galaxy S24 深度解析', 3000, 100),
    (3, 3, '视频开箱', 'MacBook Pro 开箱体验', 4000, 150),
    (4, 4, '图文评测', 'Surface Pro 9 详细评测', 2500, 80),
    (5, 5, '视频推荐', 'AirPods Pro 2 使用感受', 3500, 120);










# 指标开发
# 商品销售核心指标
# 近 7 天销售额

SELECT

    product_id,
    SUM(sales_amount) AS sales_amount_7d
FROM
    tb_sales
WHERE
        sales_date >= DATE_SUB('2025-03-05', INTERVAL 7 DAY) # - INTERVAL 7 DAY
GROUP BY
    product_id;




# 近 30 天销售数量

SELECT
    product_id,
    SUM(sales_quantity) AS sales_quantity_30d
FROM
    tb_sales
WHERE
        sales_date >= CURDATE() - INTERVAL 30 DAY
GROUP BY
    product_id;




# 流量指标
# 各流量渠道访客占比

WITH total_visitors AS (
    SELECT
        product_id,
        COUNT(DISTINCT visitor_id) AS total_count
    FROM
        tb_traffic
    GROUP BY
        product_id
)
SELECT
    t.product_id,
    t.traffic_channel,
    COUNT(DISTINCT t.visitor_id) / tv.total_count AS visitor_ratio
FROM
    tb_traffic t
        JOIN
    total_visitors tv ON t.product_id = tv.product_id
GROUP BY
    t.product_id, t.traffic_channel;





# 各流量渠道转化率

SELECT
    product_id,
    traffic_channel,
    SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) / COUNT(*) AS conversion_rate
FROM
    tb_traffic
GROUP BY
    product_id, traffic_channel;




# 评价指标
# 商品整体评分

SELECT
    product_id,
    AVG(score) AS average_score
FROM
    tb_evaluation
GROUP BY
    product_id;




# 近 30 天负面评价数

SELECT
    product_id,
    COUNT(*) AS negative_evaluation_count_30d
FROM
    tb_evaluation
WHERE
        evaluation_date >= CURDATE() - INTERVAL 30 DAY AND score <= 3
GROUP BY
    product_id;




# 搜索词指标
# 搜索词引流转化率前 5

SELECT
    product_id,
    search_term,
    conversion_rate
FROM
    tb_search_term
ORDER BY
    conversion_rate DESC
    LIMIT 5;





# 内容指标
# 内容互动量 TOP3

SELECT
    product_id,
    content_type,
    content_title,
    interaction_count
FROM
    tb_content
ORDER BY
    interaction_count DESC
    LIMIT 3;

















