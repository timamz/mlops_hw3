-- clickhouse objects for ingesting the transaction stream produced from Kafka

DROP TABLE IF EXISTS transactions_kafka_mv;
DROP TABLE IF EXISTS transactions_kafka;
DROP TABLE IF EXISTS transactions;

CREATE TABLE IF NOT EXISTS transactions_kafka
(
    transaction_time String,
    merch String,
    cat_id String,
    amount String,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat String,
    lon String,
    population_city String,
    jobs String,
    merchant_lat String,
    merchant_lon String,
    target String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions_topic',
    kafka_group_name = 'transactions_consumer_hw3_v2',
    kafka_num_consumers = '1',
    kafka_format = 'JSONEachRow',
    kafka_row_delimiter = '\n';

CREATE TABLE IF NOT EXISTS transactions
(
    transaction_time DateTime,
    merch String,
    cat_id LowCardinality(String) CODEC(ZSTD(3)),
    amount Float64,
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city String,
    us_state LowCardinality(String) CODEC(ZSTD(3)),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(transaction_time)
PRIMARY KEY (us_state, cat_id, transaction_time)
ORDER BY (us_state, cat_id, transaction_time)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_kafka_mv
TO transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    toFloat64OrZero(amount) AS amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    toFloat64OrZero(lat) AS lat,
    toFloat64OrZero(lon) AS lon,
    toUInt32OrZero(population_city) AS population_city,
    jobs,
    toFloat64OrZero(merchant_lat) AS merchant_lat,
    toFloat64OrZero(merchant_lon) AS merchant_lon,
    toUInt8OrZero(target) AS target
FROM transactions_kafka;
