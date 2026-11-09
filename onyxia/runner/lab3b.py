import os
from run_labs import *
import run_labs, json, os
run_labs.log.extend(json.load(open("lab3.json")))
AWS = os.environ.get("AWS_CMD", "aws")
B = os.environ["LAB_BUCKET_NAME"]
cell("3.0b", '''
import os, pandas as pd, trino
pd.set_option("display.width", 220)
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]), user=os.environ["TRINO_USER"], http_scheme="http", catalog="lakehouse")
def sql(statement):
    cur = conn.cursor(); cur.execute(statement); rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)
''')
S="3.8b Exercice 4 (table reecrite par MERGE)"
q_files='''sql("""SELECT regexp_extract(file_path, '[^/]+$') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."orders_by_state_month$files" """)'''
q_snap='''sql("""SELECT snapshot_id, committed_at, operation, summary['total-data-files'] AS total_files FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at""")'''
cell(S,q_snap); cell(S,q_files)
cell(S,'sql("ALTER TABLE lakehouse.gold.orders_by_state_month EXECUTE optimize")')
cell(S,q_snap); cell(S,q_files)
cell(S,'sql("SELECT count(*) AS n, sum(orders) AS orders FROM lakehouse.gold.orders_by_state_month")')
bash("3.10 Stockage gold", AWS+" s3 ls s3://"+B+"/warehouse/gold/ --recursive | grep -E 'parquet' | sed -E 's#warehouse/gold/##' | cut -c1-150")
bash("2.3b Stockage silver", AWS+" s3 ls s3://"+B+"/warehouse/ ")
save("lab3.json")
