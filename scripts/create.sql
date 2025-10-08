CREATE TABLE IF NOT EXISTS sales.customers
(
    customer_id character varying NOT NULL,
    first_name character varying(50) NOT NULL,
    last_name character varying(50) NOT NULL,
    age integer NOT NULL,
    email character varying(50) NOT NULL,
    country character varying(50) NOT NULL,
    postal_code character varying(20),
    gender character varying(10) NOT NULL,
    phone_number character varying(20),
    membership_status boolean NOT NULL,
    join_date date NOT NULL,
    customer_type character varying(30) NOT NULL,
    customer_created_date date NOT NULL,
    processing_time date,
    CONSTRAINT customers_pkey PRIMARY KEY (customer_id)
)