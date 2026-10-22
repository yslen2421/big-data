#!/usr/bin/env bash
# Crea la base, las dos tablas y corre MSCK REPAIR en Athena (workgroup primary).
# Uso: bash setup_athena.sh
set -euo pipefail

BUCKET="parcial2-aerolinea-838910"
WG="primary"
OUT="s3://${BUCKET}/athena-results/"

echo "==> Configurando result location del workgroup ${WG}..."
aws athena update-work-group --work-group "$WG" \
  --configuration-updates "ResultConfigurationUpdates={OutputLocation=${OUT}}" >/dev/null

run_query() {
  local label="$1"
  local sql="$2"
  echo "==> ${label}"
  local qid
  qid=$(aws athena start-query-execution \
    --query-string "$sql" \
    --work-group "$WG" \
    --query-execution-context Database=airline_analytics \
    --result-configuration OutputLocation="$OUT" \
    --query 'QueryExecutionId' --output text 2>/dev/null) || {
      # la primera consulta (CREATE DATABASE) corre sin contexto de base de datos
      qid=$(aws athena start-query-execution \
        --query-string "$sql" \
        --work-group "$WG" \
        --result-configuration OutputLocation="$OUT" \
        --query 'QueryExecutionId' --output text)
    }

  while true; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' --output text)
    if [[ "$state" == "SUCCEEDED" ]]; then
      echo "    OK ($qid)"
      break
    elif [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]]; then
      reason=$(aws athena get-query-execution --query-execution-id "$qid" \
        --query 'QueryExecution.Status.StateChangeReason' --output text)
      echo "    FALLÓ: $reason"
      exit 1
    fi
    sleep 2
  done
  echo "$qid"
}

run_query "1/4 CREATE DATABASE" \
  "CREATE DATABASE IF NOT EXISTS airline_analytics;"

run_query "2/4 CREATE TABLE reservations_curated" \
"CREATE EXTERNAL TABLE IF NOT EXISTS airline_analytics.reservations_curated (
  reservation_id string,
  flight_id      string,
  route          string,
  fare_class     string,
  price_cents    bigint,
  status         string
)
PARTITIONED BY (month string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/curated/reservations/'
TBLPROPERTIES ('classification' = 'parquet');"

run_query "3/4 MSCK REPAIR TABLE" \
  "MSCK REPAIR TABLE airline_analytics.reservations_curated;"

run_query "4/4 CREATE TABLE route_month_metrics" \
"CREATE EXTERNAL TABLE IF NOT EXISTS airline_analytics.route_month_metrics (
  route          string,
  month          string,
  seats_capacity bigint,
  seats_sold     bigint,
  occupancy      double,
  revenue_cents  bigint,
  bookings       bigint,
  cancellations  bigint
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/curated/metrics/'
TBLPROPERTIES ('classification' = 'parquet');"

echo "==> Listo. Verificando particiones y conteo..."

qid=$(aws athena start-query-execution \
  --query-string "SHOW PARTITIONS airline_analytics.reservations_curated;" \
  --work-group "$WG" --result-configuration OutputLocation="$OUT" \
  --query 'QueryExecutionId' --output text)
sleep 3
echo "--- particiones ---"
aws athena get-query-results --query-execution-id "$qid" \
  --query 'ResultSet.Rows[].Data[].VarCharValue' --output text | tr '\t' '\n'

qid=$(aws athena start-query-execution \
  --query-string "SELECT count(*) FROM airline_analytics.reservations_curated;" \
  --work-group "$WG" --result-configuration OutputLocation="$OUT" \
  --query 'QueryExecutionId' --output text)
sleep 3
echo "--- count(*) (esperado 1997430) ---"
aws athena get-query-results --query-execution-id "$qid" \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text

echo "==> TODO LISTO"
