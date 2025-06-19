CREATE TABLE transactions_stream (
    msno VARCHAR PRIMARY KEY,
    payment_method_id INTEGER,
    payment_plan_days INTEGER,
    plan_list_price INTEGER,
    actual_amount_paid INTEGER,
    is_auto_renew BOOLEAN,
    transaction_date VARCHAR,
    membership_expire_date VARCHAR,
    is_cancel BOOLEAN
); 