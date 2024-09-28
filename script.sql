-- Create the "customers" table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- track when the customer was added
) DISTRIBUTED BY (customer_id);

-- Create the "products" table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- track when the product was added
) DISTRIBUTED BY (product_id);

-- Create the "sales_transactions" table
CREATE TABLE sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
    product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
    purchase_date DATE NOT NULL,
    quantity INT NOT NULL
) DISTRIBUTED BY (transaction_id);

-- Create the "shipping_details" table
CREATE TABLE shipping_details (
    transaction_id INT PRIMARY KEY REFERENCES sales_transactions(transaction_id) ON DELETE CASCADE, -- foreign key and primary key
    shipping_date DATE NOT NULL,
    shipping_address VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100)
) DISTRIBUTED BY (transaction_id);

-- 2. SQL Query for Analysis

WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', st.purchase_date) AS sales_month,
        SUM(p.price * st.quantity) AS total_amount,
        SUM(st.quantity) as total_qty
    FROM
        sales_transactions st
    INNER JOIN
        products p ON p.product_id = st.product_id
    GROUP BY
        DATE_TRUNC('month', st.purchase_date)
    ORDER BY
        sales_month DESC
)
SELECT
    sales_month,
    total_amount,
    total_qty,
    AVG(total_amount) OVER (
        ORDER BY sales_month DESC
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_month_sales
FROM
    monthly_sales
ORDER BY
    sales_month DESC;

