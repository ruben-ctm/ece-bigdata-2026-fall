from run_labs import *
import os
AWS = os.environ.get("AWS_CMD", "aws")
B = os.environ["LAB_BUCKET_NAME"]
cell("3.0", '''
import os
import pandas as pd
import trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"], http_scheme="http", catalog="lakehouse")
def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)
''')
S="3.1 Dimensions"
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")''')
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")''')
cell(S,'''first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]
print(first_day, last_day)''')
cell(S,'''sql(f"""
    CREATE TABLE lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")''')
cell(S,'sql("SELECT * FROM lakehouse.gold.dim_user ORDER BY user_key LIMIT 5")')
cell(S,'sql("SELECT * FROM lakehouse.gold.dim_product ORDER BY product_key")')
cell(S,'sql("SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key LIMIT 8")')
S="3.2 Fait"
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")''')
cell(S,'''sql("""
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
""")''')
cell(S,'sql("DESCRIBE lakehouse.gold.fact_orders")')
cell(S,'sql("SELECT * FROM lakehouse.gold.fact_orders ORDER BY ordered_at LIMIT 5")')
S="3.3 Requetes etoile"
cell(S,'''sql("""
    SELECT d.year, u.state, p.product,
           count(*)        AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")''')
cell(S,'''star = sql("""
    SELECT u.state, d.year, d.month,
           count(*)                   AS orders,
           sum(f.quantity)            AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
    ORDER BY d.year, d.month, u.state
    LIMIT 10
""")
star''')
cell(S,'''ref = sql("SELECT state, orders, units, buyers FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")
print(ref.to_string())
print("Identique a l'etoile :", (ref[["orders","units","buyers"]].values == star[["orders","units","buyers"]].values).all() and (ref.state.values == star.state.values).all())''')
S="3.4 Validation"
cell(S,'''silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"
unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"
print(silver_orders, fact_orders, silver_units, fact_units, unmatched)''')
cell(S,'''# Controle supplementaire (hors enonce) : la cle pointe-t-elle vers le BON utilisateur / produit ?
sql("""
    SELECT
      count_if(u.user_id <> o.user_id)  AS wrong_user,
      count_if(p.product <> o.product)  AS wrong_product,
      count_if(f.ordered_at <> o.ordered_at OR f.quantity <> o.quantity) AS wrong_measure
    FROM lakehouse.gold.fact_orders f
    JOIN lakehouse.silver.orders o ON o.order_id = f.order_id
    JOIN lakehouse.gold.dim_user u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product p ON p.product_key = f.product_key
""")''')
S="3.5 Exercice 1"
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)'])
    AS SELECT * FROM lakehouse.gold.orders_by_state_month
""")''')
cell(S,'''sql("""SELECT partition, record_count, file_count FROM lakehouse.gold."orders_by_state_month_part$partitions" ORDER BY partition LIMIT 5""")''')
cell(S,'''sql("""SELECT regexp_extract(file_path, '[^/]+/[^/]+/[^/]+$') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."orders_by_state_month_part$files" ORDER BY file LIMIT 6""")''')
cell(S,'''sql("""SELECT count(*) AS files, sum(record_count) AS rows FROM lakehouse.gold."orders_by_state_month$files" """)''')
cell(S,'''sql("""SELECT count(*) AS files, sum(record_count) AS rows FROM lakehouse.gold."orders_by_state_month_part$files" """)''')
cell(S,'''print(sql("""EXPLAIN ANALYZE SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC' """).iloc[0,0])''')
cell(S,'''print(sql("""EXPLAIN ANALYZE SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month_part WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC' """).iloc[0,0])''')
S="3.6 Exercice 2"
cell(S,'''sql("""
    CREATE TABLE lakehouse.gold.cohorts AS
    SELECT
        (year(u.birthdate) / 10) * 10 AS decade,
        count(DISTINCT u.user_id)     AS users,
        count(o.order_id)             AS orders,
        round(CAST(sum(o.quantity) AS double) / count(o.order_id), 2) AS avg_units_per_order
    FROM lakehouse.silver.users u
    LEFT JOIN lakehouse.silver.orders o ON o.user_id = u.user_id
    GROUP BY 1
""")''')
cell(S,'sql("SELECT * FROM lakehouse.gold.cohorts ORDER BY decade")')
cell(S,'sql("SELECT sum(users) AS users, sum(orders) AS orders FROM lakehouse.gold.cohorts")')
S="3.7 Exercice 3"
cell(S,'''sql("""
    INSERT INTO lakehouse.silver.orders
    SELECT '00000000-0000-0000-0000-000000000001', user_id,
           TIMESTAMP '2020-02-10 12:00:00 UTC', 3, 'croissant'
    FROM lakehouse.silver.users ORDER BY user_id LIMIT 1
""")''')
cell(S,'sql("SELECT count(*) AS orders FROM lakehouse.silver.orders")')
cell(S,'''sql("""
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state, date_trunc('month', o.ordered_at) AS month,
               count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders o JOIN lakehouse.silver.users u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)
""")''')
cell(S,'''sql("""SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at""")''')
cell(S,'''snap = sql("""SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at LIMIT 1""").iloc[0,0]
print(sql(f"SELECT sum(orders) AS orders_before FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {snap}").to_string())
print(sql("SELECT sum(orders) AS orders_after FROM lakehouse.gold.orders_by_state_month").to_string())''')
S="3.8 Exercice 4"
cell(S,'''sql("""SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")''')
cell(S,'''sql("""SELECT regexp_extract(file_path, '[^/]+$') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files" """)''')
cell(S,'sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")')
cell(S,'''sql("""SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")''')
cell(S,'''sql("""SELECT regexp_extract(file_path, '[^/]+$') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files" """)''')
cell(S,'sql("SELECT count(*) AS products FROM lakehouse.gold.top_products")')
cell("3.9","conn.close()")
save("lab3.json")
