INSERT INTO sales.customers (
    customer_id,
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code,
    gender,
    phone_number,
    membership_status,
    join_date,
    customer_type,
    customer_created_date,
    processing_time
)
VALUES (
    '1',
    'Louie',
    'Danick',
    22,
    'ldanick0@cargocollective.com',
    'Vietnam',
    '70000',                -- ví dụ postal_code
    'Male',
    '301-670-4557',
    TRUE,
    '2012-02-01',           -- YYYY-MM-DD format
    'Business',
    '2012-03-08',
    CURRENT_TIMESTAMP       -- hoặc '2012-03-08' nếu muốn cố định
);
