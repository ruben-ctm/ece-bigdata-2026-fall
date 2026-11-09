from run_labs import *
import os
AWS = os.environ.get("AWS_CMD", "aws")
B = os.environ["LAB_BUCKET_NAME"]
S="2.0 Connexion"
cell(S, '''
import os
import pandas as pd
import trino

CATALOG = "lakehouse"

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"],
    port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"],
    http_scheme="http",
    catalog=CATALOG,
)

def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)
''')
cell(S, 'sql("SELECT version()")')
S="2.1 Catalogues"
for q in ['sql("SHOW CATALOGS")','sql("SHOW SCHEMAS FROM lakehouse")','sql("SHOW TABLES FROM lakehouse.silver")','sql("DESCRIBE lakehouse.silver.orders")','sql("DESCRIBE lakehouse.silver.users")']:
    cell(S,q)
S="2.2 Silver"
cell(S,'sql("SELECT count(*) AS orders FROM lakehouse.silver.orders")')
cell(S,'sql("SELECT count(*) AS users FROM lakehouse.silver.users")')
cell(S,'''sql("""
    SELECT state,
           count(*) AS users,
           count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
""")''')
cell(S,'''sql("""
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")''')
S="2.3 Metadonnees Iceberg"
cell(S,'''sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
""")''')
cell(S,'''sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files"
""")''')
cell(S,'sql(\'SELECT * FROM lakehouse.silver."orders$history"\')')
bash(S, AWS+" s3 ls s3://"+B+"/warehouse/silver/ --recursive | grep -v metadata.json")
S="2.4 Plan"
cell(S,'''plan = sql("""
    EXPLAIN (TYPE DISTRIBUTED)
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
""")
print(plan.iloc[0, 0])''')
cell(S,'''stats = sql("""
    EXPLAIN ANALYZE
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
""")
print(stats.iloc[0, 0])''')
cell(S,'''sql("""
    SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms, query
    FROM system.runtime.queries
    ORDER BY created DESC
    LIMIT 5
""")''')
S="2.5 Gold"
cell(S,'sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")')
cell(S,'sql("SHOW SCHEMAS FROM lakehouse")')
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")''')
cell(S,'sql("SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")')
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.top_products AS
    SELECT
        product,
        count(*)      AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")''')
cell(S,'sql("SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")')
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)        AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)        AS first_order_at,
        max(o.ordered_at)        AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")''')
cell(S,'sql("SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")')
cell(S,'sql("SHOW TABLES FROM lakehouse.gold")')
cell(S,'''sql("""
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
""")''')
S="2.6 Validation"
cell(S,'''silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
assert silver_units == gold_units
assert silver_units == product_units
print(silver_units, gold_units, product_units)''')
cell(S,'''silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
assert silver_users == gold_users
assert silver_orders == gold_orders
print(silver_users, gold_users, silver_orders, gold_orders)''')
S="2.7 Refresh"
cell(S,'sql("DELETE FROM lakehouse.gold.top_products")')
cell(S,'sql("SELECT count(*) AS products FROM lakehouse.gold.top_products")')
cell(S,'''sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity),
           rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
""")''')
cell(S,'''sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")''')
cell(S,'''first = sql("""
    SELECT snapshot_id
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
    LIMIT 1
""").iloc[0, 0]
sql(f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")''')
cell(S,'sql("SELECT count(*) AS products FROM lakehouse.gold.top_products")')
save("lab2.json")
