"""Lab 09 : enregistre silver.orders dans Polaris sans copier les donnees, cree silver.users via Polaris."""
import os, re, subprocess
from spark_common import session, BUCKET

AWS = os.environ.get("AWS_CMD", "aws --profile default")
meta = f"s3://{BUCKET}/warehouse/silver/orders/metadata/"
version = subprocess.run(f"{AWS} s3 cp {meta}version-hint.text -", shell=True, capture_output=True, text=True, check=True).stdout.strip()
print("version-hint:", version)
spark = session("lab-polaris", polaris_credential=os.environ["POLARIS_DEV_CREDENTIAL"])
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
if spark.catalog.tableExists("polaris.silver.orders"):
    print("silver.orders deja enregistre dans Polaris")
else:
    spark.sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json')
    """).show(truncate=False)
spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.stop()
