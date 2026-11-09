#!/usr/bin/env bash
# TP Big Data : labs 03 (donnees), 07 (silver Spark), 09 (Polaris), 10 (Trino) puis labs Trino 2 et 3.
# Journalise chaque etape et pousse les resultats sur la branche onyxia-results, meme en cas d'echec.
set -u
REPO="ruben-ctm/ece-bigdata-2026-fall"
BRANCH="${TP_BRANCH:-claude/epic-wright-po7bx7}"
HOME="${HOME:-/home/onyxia}"
W="$HOME/work/tp-adaltas"; SRC="$W/src"; RES="$W/results"
mkdir -p "$W" "$RES"
exec > >(tee -a "$RES/run.txt") 2>&1
LOCAL_TEST="${LOCAL_TEST:-0}"      # 1 = test hors cluster (pas de kubectl/helm)
STATUS="$RES/steps.tsv"; : > "$STATUS"
START="${TP_START:-00}"
RAW="https://raw.githubusercontent.com/$REPO/$BRANCH/onyxia"

now() { date '+%H:%M:%S'; }
push_results() {
  [ -n "${GIT_PERSONAL_ACCESS_TOKEN:-}" ] || { echo "[push] pas de jeton GitHub (Compte > Git dans Onyxia) : resultats dans $RES"; return 0; }
  (
    set -e; cd "$W"
    [ -d push ] || git clone -q --depth 1 "https://x-access-token:${GIT_PERSONAL_ACCESS_TOKEN}@github.com/$REPO.git" push
    cd push
    git config user.name "${GIT_USER_NAME:-tp-bot}"; git config user.email "${GIT_USER_MAIL:-tp-bot@users.noreply.github.com}"
    git checkout -q -B onyxia-results
    rm -rf onyxia-results && mkdir -p onyxia-results && cp -r "$RES"/. onyxia-results/
    git add -f onyxia-results && git commit -q -m "onyxia results $(date -u +%FT%TZ)" || true
    git push -q -f origin onyxia-results
  ) >>"$RES/push.txt" 2>&1 && echo "[push] ok" || echo "[push] ECHEC (voir push.txt)"
}
step() { # step NOM commande...
  local name="$1"; shift
  if [[ "${name%% *}" < "$START" ]]; then echo "--- $name ignoree (reprise a $START)"; return 0; fi
  echo "=== [$(now)] $name"
  if "$@"; then echo -e "$name\tOK" >>"$STATUS"; else echo -e "$name\tECHEC" >>"$STATUS"; echo "!!! $name a echoue"; tail -n 120 "$RES/run.txt" > "$RES/fail_${name%% *}.txt"; diag; push_results; return 1; fi
  push_results
}
with_progress() { # lance une commande longue en poussant l'etat des pods toutes les 150 s
  ( while sleep 150; do { echo "--- progress $(now)"; kubectl get pods 2>&1; kubectl get events --sort-by=.lastTimestamp 2>&1 | tail -6; } >>"$RES/progress.txt"; push_results >/dev/null 2>&1; done ) &
  local bg=$!; "$@"; local rc=$?; kill "$bg" 2>/dev/null; return $rc
}
diag() {
  [ "$LOCAL_TEST" = 1 ] && return 0
  { echo "--- diag $(now)"; kubectl get pods -o wide 2>&1 | tail -20; kubectl get events --sort-by=.lastTimestamp 2>&1 | tail -15; } >>"$RES/diag.txt" 2>&1
}
trap 'push_results' EXIT

cur_control() { curl -fsS -m 20 "$RAW/control.txt?$(date +%s)" 2>/dev/null | head -1; }
: "${TP_RERUN_ID:=$(cur_control | cut -d' ' -f1)}"
wait_for_rerun() {
  [ "$LOCAL_TEST" = 1 ] && return 0
  echo "=== [$(now)] en attente de consignes ($RAW/control.txt)"
  while true; do
    sleep 60
    c="$(cur_control)"; id="${c%% *}"; stepn="${c#* }"; [ "$stepn" = "$c" ] && stepn="00"
    if [ -n "$id" ] && [ "$id" != "${TP_RERUN_ID:-}" ]; then
      curl -fsSL "$RAW/run.sh?$(date +%s)" -o "$W/run.sh.new" && mv "$W/run.sh.new" "$W/run.sh"
      TP_RERUN_ID="$id" TP_START="$stepn" exec bash "$W/run.sh"
    fi
  done
}
main() {

# --- sources
get_sources() {
  rm -rf "$SRC"
  if [ -n "${TP_SRC_LOCAL:-}" ]; then cp -r "$TP_SRC_LOCAL" "$SRC"; return; fi
  rm -rf "$W/repo"; git clone -q --depth 1 -b "$BRANCH" "https://github.com/$REPO.git" "$W/repo" && cp -r "$W/repo/onyxia" "$SRC"
}
echo "=== [$(now)] 00 sources"
get_sources || { echo "!!! 00 sources a echoue"; echo -e "00 sources\tECHEC" >>"$STATUS"; push_results; return 1; }
echo -e "00 sources\tOK" >>"$STATUS"

# --- environnement
export LAB_BUCKET_NAME="${LAB_BUCKET_NAME:-${KUBERNETES_NAMESPACE:?KUBERNETES_NAMESPACE manquant}}"
export POLARIS_URI="${POLARIS_URI:-http://polaris:8181}" POLARIS_MGMT_URI="${POLARIS_MGMT_URI:-http://polaris-mgmt:8182}"
export POLARIS_CATALOG="${POLARIS_CATALOG:-$LAB_BUCKET_NAME}" AWS_REGION="${AWS_REGION:-us-east-1}"
export TRINO_HOST="${TRINO_HOST:-trino}" TRINO_PORT="${TRINO_PORT:-8080}" TRINO_USER="${TRINO_USER:-$LAB_BUCKET_NAME}"
export PATH="$HOME/.local/bin:$PATH" PYTHONPATH="$SRC"
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then eval "$(aws configure export-credentials --profile default --format env)"; fi
if [ -z "${S3_ENDPOINT_URL:-}" ]; then
  export S3_ENDPOINT_URL="$(aws configure get endpoint_url --profile default 2>/dev/null || true)"
  [ -n "$S3_ENDPOINT_URL" ] || export S3_ENDPOINT_URL="https://${AWS_S3_ENDPOINT}"
fi
if [ -z "${AWS_CMD:-}" ]; then export AWS_CMD="aws --endpoint-url $S3_ENDPOINT_URL"; fi
echo "bucket=$LAB_BUCKET_NAME s3=$S3_ENDPOINT_URL aws=$AWS_CMD"
command -v uv >/dev/null || pip install --user -q uv
PY="${PYTHON:-python3}"

# --- lab 03 : donnees (generateur du lab) -> bronze
gen_bronze() {
  cd "$SRC/gen" &&
  uv run --quiet --no-project --with faker env PYTHONPATH=src python -m work.dataset_users -o csv > "$W/users.csv" &&
  uv run --quiet --no-project --with faker env PYTHONPATH=src python -m work.dataset_orders -o csv > "$W/orders.csv" &&
  wc -c "$W/users.csv" "$W/orders.csv" &&
  $AWS_CMD s3 cp "$W/users.csv" "s3://$LAB_BUCKET_NAME/bronze/users.csv" &&
  $AWS_CMD s3 cp "$W/orders.csv" "s3://$LAB_BUCKET_NAME/bronze/orders.csv" &&
  $AWS_CMD s3 ls "s3://$LAB_BUCKET_NAME/bronze/"
}
step "01 donnees bronze" gen_bronze || return 1

# --- lab 07 : silver Spark
detect_spark() {
  v=$("$PY" -c 'import pyspark;print(".".join(pyspark.__version__.split(".")[:2]))' 2>/dev/null || true)
  [ -n "$v" ] || v=$("${SPARK_HOME:-/opt/spark}/bin/spark-submit" --version 2>&1 | grep -o 'version [0-9]*\.[0-9]*' | head -1 | cut -d' ' -f2)
  [ -n "$v" ] && export ICEBERG_PACKAGE="${ICEBERG_PACKAGE:-org.apache.iceberg:iceberg-spark-runtime-${v}_2.13:1.11.0}"
  echo "spark=${v:-inconnu} iceberg=${ICEBERG_PACKAGE:-defaut}"
}
detect_spark
spark_run() {
  if "$PY" -c 'import pyspark' 2>/dev/null; then "$PY" "$@"
  else "${SPARK_HOME:-/opt/spark}/bin/spark-submit" --master 'local[*]' --packages "${ICEBERG_PACKAGE:-org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0}" "$@"; fi
}
silver() { cd "$SRC" && { echo "python=$(command -v $PY) spark_home=${SPARK_HOME:-?}"; $PY -c 'import pyspark;print("pyspark",pyspark.__version__)' 2>&1 | tail -1; java -version 2>&1 | head -1; } && spark_run silver_spark.py; }
step "02 silver spark" silver || return 1

# --- lab 09 : Polaris
deploy_polaris() (
  set -e; cd "$SRC"
  kubectl create configmap polaris-config \
    --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" --from-literal=POLARIS_URI="$POLARIS_URI" \
    --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" --from-literal=POLARIS_REALM="POLARIS" \
    --from-literal=POLARIS_CLIENT_ID="root" --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
    --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic polaris-credential --from-literal=POLARIS_ROOT_SECRET="s3cr3t" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic polaris-s3 --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" --from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    --dry-run=client -o yaml | kubectl apply -f -
  helm repo add polaris https://downloads.apache.org/polaris/helm-chart
  helm repo update
  helm dependency update polaris/deployment
  st=$(helm status polaris -o json 2>/dev/null | jq -r '.info.status // empty')
  if [ -n "$st" ] && [ "$st" != deployed ]; then echo "release polaris en etat $st : reinstallation"; helm uninstall polaris --wait || true; kubectl delete pvc data-polaris-postgresql-0 --ignore-not-found; fi
  with_progress helm upgrade --install polaris polaris/deployment --wait --timeout 40m \
    --set postgresql.primary.resources.requests.cpu=10m --set polaris.resources.requests.cpu=20m
  curl -s http://polaris-mgmt:8182/q/health | head -5
  kubectl wait --for=condition=complete job/polaris-bootstrap --timeout=300s
)
get_polaris_credentials() {
  kubectl logs job/polaris-bootstrap | grep '"credentials"' | jq -r 'select(.principal == "dev" or .principal == "analyst")
    | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' > "$HOME/.polaris-credentials.env"
  [ -s "$HOME/.polaris-credentials.env" ]
}
reset_polaris_credentials() { # si les identifiants ont ete perdus (bootstrap deja joue)
  TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
  : > "$HOME/.polaris-credentials.env"
  for p in dev analyst; do
    curl -s -X POST -H "Authorization: Bearer $TOKEN" "$POLARIS_URI/api/management/v1/principals/$p/reset" \
    | jq -r --arg p "$p" '"POLARIS_\($p | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' >> "$HOME/.polaris-credentials.env"
  done
}
if [ "$LOCAL_TEST" != 1 ]; then
  step "03 deploiement polaris" deploy_polaris || return 1
  get_polaris_credentials || reset_polaris_credentials
  [ -s "$HOME/.polaris-credentials.env" ] || { echo "identifiants Polaris introuvables"; return 1; }
  set -a; . "$HOME/.polaris-credentials.env"; set +a
else
  export POLARIS_DEV_CREDENTIAL="${POLARIS_DEV_CREDENTIAL:-root:s3cr3t}"
fi
register() { cd "$SRC" && spark_run register_polaris.py; }
step "04 silver dans polaris" register || return 1

# --- lab 10 : Trino
deploy_trino() (
  set -e; cd "$SRC"
  kubectl create secret generic trino-polaris-oauth --from-literal=POLARIS_DEV_CREDENTIAL="$POLARIS_DEV_CREDENTIAL" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create configmap trino-polaris-config --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" --dry-run=client -o yaml | kubectl apply -f -
  helm repo add trino https://trinodb.github.io/charts
  helm repo update
  helm dependency update ./trino/deployment
  st=$(helm status trino -o json 2>/dev/null | jq -r '.info.status // empty')
  if [ -n "$st" ] && [ "$st" != deployed ]; then echo "release trino en etat $st : reinstallation"; helm uninstall trino --wait || true; fi
  with_progress helm upgrade --install trino ./trino/deployment --wait --timeout 40m \
    --set trino.coordinator.resources.requests.cpu=50m
  curl -sf http://trino:8080/v1/info
)
if [ "$LOCAL_TEST" != 1 ]; then step "05 deploiement trino" deploy_trino || return 1; fi

# --- labs Trino 2 et 3
run_labs() (
  set -e; cd "$SRC/runner"
  uv venv -q "$W/venv" && uv pip install -q -p "$W/venv" trino pandas
  P="$W/venv/bin/python"
  "$P" lab2.py && "$P" lab3.py && "$P" lab3b.py && "$P" lab3c.py
  "$P" envcap.py
  cp lab2.json lab3.json env.json "$RES/"
)
step "06 labs trino 2 et 3" run_labs
echo "=== [$(now)] TERMINE"; cat "$STATUS"
}
main
wait_for_rerun
