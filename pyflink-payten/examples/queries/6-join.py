import os
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, Csv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(6)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

# "asin","category","brand","rank","main_cat"
t_env.connect(
    FileSystem().path("/opt/examples/data/input/products_no_duplicates.csv")
).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("asin", DataTypes.STRING())
    .field("category", DataTypes.STRING())
    .field("brand", DataTypes.STRING())
    .field("rank", DataTypes.STRING())
    .field("main_cat", DataTypes.STRING())
).with_schema(
    Schema()
    .field("asin", DataTypes.STRING())
    .field("category", DataTypes.STRING())
    .field("brand", DataTypes.STRING())
    .field("rank", DataTypes.STRING())
    .field("main_cat", DataTypes.STRING())
).create_temporary_table(
    "products"
)
# "customer_id","first_name","last_name","gender","ssn","credit_card","credit_card_provider","birth_date",
# "start_date","title","office","organization","salary","bonus","accured_holidays" "547874","George","Clayton","M",
# "003-77-1939","3585853856999379","VISA 16 digit","1985-01-20","2018-01-12","VP","Seattle","Product",152000,24000,1
t_env.connect(FileSystem().path("/opt/examples/data/input/customers.csv")).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("customer_id", DataTypes.STRING())
    .field("last_name", DataTypes.STRING())
    .field("gender", DataTypes.STRING())
    .field("ssn", DataTypes.STRING())
    .field("credit_card", DataTypes.STRING())
    .field("credit_card_provider", DataTypes.STRING())
    .field("birth_date", DataTypes.STRING())
    .field("start_date", DataTypes.STRING())
    .field("title", DataTypes.STRING())
    .field("office", DataTypes.STRING())
    .field("organization", DataTypes.STRING())
    .field("salary", DataTypes.INT())
    .field("bonus", DataTypes.INT())
    .field("accured_holidays", DataTypes.INT())
).with_schema(
    Schema()
    .field("customer_id", DataTypes.STRING())
    .field("first_name", DataTypes.STRING())
    .field("last_name", DataTypes.STRING())
    .field("gender", DataTypes.STRING())
    .field("ssn", DataTypes.STRING())
    .field("credit_card", DataTypes.STRING())
    .field("credit_card_provider", DataTypes.STRING())
    .field("birth_date", DataTypes.STRING())
    .field("start_date", DataTypes.STRING())
    .field("title", DataTypes.STRING())
    .field("office", DataTypes.STRING())
    .field("organization", DataTypes.STRING())
    .field("salary", DataTypes.INT())
    .field("bonus", DataTypes.INT())
    .field("accured_holidays", DataTypes.INT())
).create_temporary_table(
    "customers"
)

# "transaction_id","product_id","transaction_amount","transaction_date","customer_id"
# "172161","9742356831",49.01,"2020-02-08 08:09:39","754658"
t_env.connect(
    FileSystem().path("/opt/examples/data/input/transactions.csv")
).with_format(
    OldCsv()
    .ignore_first_line()
    .field_delimiter(",")
    .quote_character('"')
    .field("transaction_id", DataTypes.STRING())
    .field("product_id", DataTypes.STRING())
    .field("transaction_amount", DataTypes.DOUBLE())
    .field("transaction_date", DataTypes.STRING())
    .field("customer_id", DataTypes.STRING())
).with_schema(
    Schema()
    .field("transaction_id", DataTypes.STRING())
    .field("product_id", DataTypes.STRING())
    .field("transaction_amount", DataTypes.DOUBLE())
    .field("transaction_date", DataTypes.STRING())
    .field("customer_id", DataTypes.STRING())
).create_temporary_table(
    "transactions"
)

t_env.connect(
    FileSystem().path("/opt/examples/data/output/6_join_output.csv")
).with_format(Csv().derive_schema()).with_schema(
    Schema()
    .field("customers_no", DataTypes.BIGINT())
    .field("brand", DataTypes.STRING())
    .field("product_id", DataTypes.STRING())
).create_temporary_table(
    "mySink"
)

final_table = t_env.sql_query(
    """select
        count(customer_id) as customers_no,
        MIN(brand) as brand,
        product_id
        from transactions t left join products p on t.product_id = p.asin
        group by product_id
    """
)
final_table.insert_into("mySink")
t_env.execute("6-join")
