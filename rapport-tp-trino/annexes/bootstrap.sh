set -e
cd /tmp/claude-0/lab; . .venv/bin/activate
docker compose down -v >/dev/null 2>&1
docker compose up -d s3 polaris >/dev/null 2>&1
for i in $(seq 40); do curl -s localhost:8182/q/health | grep -q '"UP"' && break; sleep 2; done
sleep 5
python -c '
import boto3
boto3.client("s3",endpoint_url="http://localhost:8333",aws_access_key_id="labkey",aws_secret_access_key="labsecret",region_name="us-east-1").create_bucket(Bucket="lab")'
TOKEN=$(curl -s localhost:8181/api/catalog/v1/oauth/tokens -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
A="Authorization: Bearer $TOKEN"; J="Content-Type: application/json"
curl -s -o /dev/null -X POST localhost:8181/api/management/v1/catalogs -H "$A" -H "$J" -d '{"catalog":{"name":"lab","type":"INTERNAL","readOnly":false,"properties":{"default-base-location":"s3://lab/warehouse"},"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://lab/warehouse"],"endpoint":"http://s3:8333","endpointInternal":"http://s3:8333","pathStyleAccess":true,"stsUnavailable":true,"region":"us-east-1"}}}'
curl -s -o /dev/null -X PUT localhost:8181/api/management/v1/principal-roles/service_admin/catalog-roles/lab -H "$A" -H "$J" -d '{"catalogRole":{"name":"catalog_admin"}}'
curl -s -o /dev/null -X PUT localhost:8181/api/management/v1/catalogs/lab/catalog-roles/catalog_admin/grants -H "$A" -H "$J" -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
docker compose up -d trino >/dev/null 2>&1
for i in $(seq 60); do curl -s localhost:8080/v1/info | grep -q '"starting":false' && break; sleep 3; done
sleep 3
python build_silver.py
