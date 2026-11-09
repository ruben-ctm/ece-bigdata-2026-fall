"""Comparaison Trino / Spark (lab 2, section 'Compare with Spark' et lab 3, exercice 5).
Spark est connecte a Polaris (principal dev). Journalise les cellules dans spark.json."""
import json, os, time
import pandas as pd
from spark_common import session

pd.set_option("display.width", 220); pd.set_option("display.max_columns", 30)
log = []
def cell(section, code, fn):
    t0 = time.time()
    try:
        out = fn(); err = False
    except Exception as e:
        out = "ERREUR: " + str(e)[:600]; err = True
    if hasattr(out, "toPandas"):
        out = out.toPandas().to_string()
    elif isinstance(out, pd.DataFrame):
        out = out.to_string()
    log.append({"section": section, "kind": "python", "code": code.strip("\n"), "output": str(out).rstrip("\n"), "error": err, "seconds": round(time.time() - t0, 2)})
    print(f"[{section}] {'ERR' if err else 'ok'} {round(time.time()-t0,2)}s: {code.strip().splitlines()[0][:70]}")

t_session = time.time()
spark = session("lab-trino-vs-spark", polaris_credential=os.environ["POLARIS_DEV_CREDENTIAL"])
t_session = round(time.time() - t_session, 1)

S = "H Comparaison Spark (lab 2)"
cell(S, 'spark.sql("SHOW NAMESPACES IN polaris")', lambda: spark.sql("SHOW NAMESPACES IN polaris"))
cell(S, 'spark.sql("SHOW TABLES IN polaris.gold")', lambda: spark.sql("SHOW TABLES IN polaris.gold"))
FIRST = {}
cell(S, 'first = spark.sql("SELECT snapshot_id FROM polaris.silver.orders.snapshots ORDER BY committed_at LIMIT 1").first()[0]\n'
        'spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots ORDER BY committed_at")',
     lambda: (FIRST.__setitem__("id", spark.sql("SELECT snapshot_id FROM polaris.silver.orders.snapshots ORDER BY committed_at LIMIT 1").first()[0]),
              spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots ORDER BY committed_at"))[1])
TOP = """
    SELECT product,
           count(*)      AS orders,
           sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM polaris.silver.orders {ver}
    GROUP BY product"""
cell(S, 'top = spark.sql("""' + TOP.format(ver="VERSION AS OF <premier snapshot>") + '""")\ntop.writeTo("polaris.gold.top_products_spark").createOrReplace()',
     lambda: (spark.sql(TOP.format(ver=f"VERSION AS OF {FIRST['id']}")).writeTo("polaris.gold.top_products_spark").createOrReplace(), "table polaris.gold.top_products_spark creee")[1])
cell(S, 'spark.sql("SELECT * FROM polaris.gold.top_products_spark ORDER BY units_rank")', lambda: spark.sql("SELECT * FROM polaris.gold.top_products_spark ORDER BY units_rank"))
DIFF = """
    SELECT count(*) AS lignes_differentes FROM (
      (SELECT product, orders, units, units_rank FROM polaris.gold.top_products
       EXCEPT SELECT product, orders, units, units_rank FROM {other})
      UNION ALL
      (SELECT product, orders, units, units_rank FROM {other}
       EXCEPT SELECT product, orders, units, units_rank FROM polaris.gold.top_products))"""
cell(S, 'spark.sql("""' + DIFF.format(other="polaris.gold.top_products_spark") + '""")  # Trino contre Spark, memes donnees', lambda: spark.sql(DIFF.format(other="polaris.gold.top_products_spark")))
cell(S, 'spark.sql("""' + DIFF.format(other="(agregation de silver.orders actuel)") + '""")  # silver a recu un INSERT depuis', lambda: spark.sql(DIFF.format(other="(" + TOP.format(ver="") + ")")))
cell(S, 'spark.sql("""SELECT * FROM (agregation de silver.orders actuel) WHERE product = \'croissant\'""")', lambda: spark.sql("SELECT * FROM (" + TOP.format(ver="") + ") WHERE product = 'croissant'"))
cell(S, 'spark.sql("DESCRIBE polaris.gold.top_products")  # table ecrite par Trino, lue par Spark', lambda: spark.sql("DESCRIBE polaris.gold.top_products"))
cell(S, 'spark.sql("DESCRIBE polaris.gold.top_products_spark")', lambda: spark.sql("DESCRIBE polaris.gold.top_products_spark"))
cell(S, 'spark.sql("SELECT snapshot_id, operation FROM polaris.gold.top_products_spark.snapshots")', lambda: spark.sql("SELECT snapshot_id, operation FROM polaris.gold.top_products_spark.snapshots"))
cell(S, 'print("duree de creation de la session Spark :", t_session, "s")', lambda: f"duree de creation de la session Spark : {t_session} s")

S = "3.9 Exercice 5 (lab 3)"
AGG = """
    SELECT u.state, date_trunc('month', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders o JOIN polaris.silver.users u ON u.user_id = o.user_id
    GROUP BY 1, 2"""
cell(S, 'spark.sql("""' + AGG + '""").count()', lambda: spark.sql(AGG).count())
cell(S, 'spark.sql("SELECT count(*) AS lignes, sum(orders) AS commandes FROM polaris.gold.orders_by_state_month")  # table Trino', lambda: spark.sql("SELECT count(*) AS lignes, sum(orders) AS commandes FROM polaris.gold.orders_by_state_month"))
cell(S, 'spark.sql("""SELECT count(*) AS lignes, sum(orders) AS commandes FROM (' + AGG.strip().replace("\n", " ")[:0] + 'agregation Spark)""")', lambda: spark.sql("SELECT count(*) AS lignes, sum(orders) AS commandes FROM (" + AGG + ")"))
cell(S, 'spark.sql("""  -- lignes qui different entre Spark et la table gold de Trino\n  (agregation Spark EXCEPT orders_by_state_month) UNION ALL (orders_by_state_month EXCEPT agregation Spark)""")', lambda: spark.sql(f"""
    SELECT count(*) AS lignes_differentes FROM (
      (SELECT state, month, orders, units, buyers FROM ({AGG}) EXCEPT SELECT state, month, orders, units, buyers FROM polaris.gold.orders_by_state_month)
      UNION ALL
      (SELECT state, month, orders, units, buyers FROM polaris.gold.orders_by_state_month EXCEPT SELECT state, month, orders, units, buyers FROM ({AGG})))"""))
json.dump(log, open("spark.json", "w"), indent=1)
spark.stop()
