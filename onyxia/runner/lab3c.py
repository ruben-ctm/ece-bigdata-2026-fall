import os
from run_labs import *
import run_labs, json
run_labs.log.extend(json.load(open("lab3.json")))
cell("3.4b", '''
import os, pandas as pd, trino
pd.set_option("display.width", 200)
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]), user=os.environ["TRINO_USER"], http_scheme="http", catalog="lakehouse")
def sql(statement):
    cur = conn.cursor(); cur.execute(statement); rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)
''')
cell("3.4b", '''# Defaut 1 : un user_id present deux fois dans dim_user (simule, sans ecriture)
sql("""
    WITH dim_user_dup AS (
        SELECT user_key, user_id FROM lakehouse.gold.dim_user
        UNION ALL
        SELECT 999, user_id FROM lakehouse.gold.dim_user WHERE user_id = '9793b9b4-1374-4146-b2c5-bd89b70b3420'
    )
    SELECT (SELECT count(*) FROM lakehouse.silver.orders) AS silver_orders,
           count(*) AS fact_orders_simule
    FROM lakehouse.silver.orders o LEFT JOIN dim_user_dup u ON u.user_id = o.user_id
""")''')
cell("3.4b", '''# Defaut 2 : une commande sans produit (NULL), simulee sans ecriture
sql("""
    WITH orders_null AS (
        SELECT order_id, product FROM lakehouse.silver.orders
        UNION ALL SELECT 'x-null-product', CAST(NULL AS varchar)
    )
    SELECT count(*) AS orders, count_if(p.product_key IS NULL) AS product_key_null
    FROM orders_null o LEFT JOIN lakehouse.gold.dim_product p ON p.product = o.product
""")''')
cell("3.4b", '''# Defaut 3 : ajout d'un utilisateur dans dim_user reconstruite avec row_number() (simule)
sql("""
    WITH new_dim AS (
        SELECT row_number() OVER (ORDER BY user_id) AS user_key, user_id
        FROM (SELECT user_id FROM lakehouse.silver.users
              UNION ALL SELECT '00000000-0000-0000-0000-00000000000a')
    )
    SELECT o.user_id, o.user_key AS cle_actuelle, n.user_key AS cle_apres_reconstruction
    FROM lakehouse.gold.dim_user o JOIN new_dim n ON n.user_id = o.user_id
    WHERE o.user_key <> n.user_key ORDER BY o.user_key LIMIT 5
""")''')
cell("3.4b", '''sql("""
    WITH new_dim AS (
        SELECT row_number() OVER (ORDER BY user_id) AS user_key, user_id
        FROM (SELECT user_id FROM lakehouse.silver.users
              UNION ALL SELECT '00000000-0000-0000-0000-00000000000a')
    )
    SELECT count_if(o.user_key <> n.user_key) AS users_dont_la_cle_change, count(*) AS users
    FROM lakehouse.gold.dim_user o JOIN new_dim n ON n.user_id = o.user_id
""")''')
save("lab3.json")
