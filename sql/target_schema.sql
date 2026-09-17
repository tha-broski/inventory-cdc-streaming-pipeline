CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(100),
    price NUMERIC(10,2),
    is_active BOOLEAN,
    updated_at TIMESTAMPTZ
);

CREATE TABLE products_staging (
    product_id INTEGER,
    name VARCHAR(200),
    category VARCHAR(100),
    price NUMERIC(10,2),
    is_active BOOLEAN,
    updated_at TIMESTAMPTZ
);

CREATE TABLE warehouses (
    warehouse_id INTEGER PRIMARY KEY,
    name VARCHAR(150),
    location VARCHAR(200),
    is_active BOOLEAN,
    updated_at TIMESTAMPTZ
);

CREATE TABLE warehouses_staging (
    warehouse_id INTEGER,
    name VARCHAR(150),
    location VARCHAR(200),
    is_active BOOLEAN,
    updated_at TIMESTAMPTZ
);

CREATE TABLE inventory (
    inventory_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    warehouse_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL,

    UNIQUE (product_id, warehouse_id)
);

CREATE TABLE inventory_staging (
    inventory_id INTEGER,
    product_id INTEGER,
    warehouse_id INTEGER,
    quantity INTEGER,
    updated_at TIMESTAMPTZ
);