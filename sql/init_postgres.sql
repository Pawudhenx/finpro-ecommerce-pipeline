CREATE TABLE IF NOT EXISTS users (
    user_id        VARCHAR(50) PRIMARY KEY,
    name           VARCHAR(255),
    email          VARCHAR(255),
    created_date   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id     VARCHAR(50) PRIMARY KEY,
    product_name   VARCHAR(255),
    category       VARCHAR(100),
    price          NUMERIC(15,2),
    created_date   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       VARCHAR(50) PRIMARY KEY,
    user_id        VARCHAR(50) REFERENCES users(user_id),
    product_id     VARCHAR(50) REFERENCES products(product_id),
    quantity       INT,
    amount         NUMERIC(15,2),
    country        VARCHAR(10),
    created_date   TIMESTAMP,
    status         VARCHAR(20)
);
