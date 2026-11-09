"""Captures d'environnement (equivalent des captures 'env' du rapport)."""
import os
from run_labs import *
B = os.environ["LAB_BUCKET_NAME"]; AWS = os.environ.get("AWS_CMD", "aws")
bash("env1", "(kubectl get pods -o wide 2>/dev/null | sed -E 's/ +/  /g' | cut -c1-170) || true; echo; (helm list 2>/dev/null) || echo '(kubectl/helm indisponibles)'")
bash("env2", f"cd ${{HOME}}/work/tp-adaltas && wc -c users.csv orders.csv && head -c 330 users.csv && echo && head -3 orders.csv && echo && python3 -c \"import csv;print('lignes orders.csv :', sum(1 for _ in csv.reader(open('orders.csv')))-1, '| lignes users.csv :', sum(1 for _ in csv.reader(open('users.csv')))-1)\" && {AWS} s3 ls s3://{B}/bronze/")
bash("env3", f"""TOKEN=$(curl -s $POLARIS_URI/api/catalog/v1/oauth/tokens -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token); curl -s $POLARIS_URI/api/management/v1/catalogs -H "Authorization: Bearer $TOKEN" | jq -c '.catalogs[] | {{name, type, storage: .storageConfigInfo.storageType, endpoint: .storageConfigInfo.endpoint, allowedLocations: .storageConfigInfo.allowedLocations}}'; echo; curl -s http://$TRINO_HOST:$TRINO_PORT/v1/info""")
save("env.json")
