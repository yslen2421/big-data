"""
Job PySpark del Parcial 2 (Persona B).

Lee los CSV crudos (flights.csv, reservations.csv), aplica las reglas C1-C5 y escribe:
  1) <out>/reservations/  -> reservations_curated, Parquet particionado por month (24 particiones)
  2) <out>/metrics/       -> route_month_metrics, Parquet sin particionar

Uso:
  Local:  spark-submit etl_job.py --raw data/raw/ --out data/curated/
  EMR:    spark-submit s3://BUCKET/scripts/etl_job.py --raw s3://BUCKET/raw/ --out s3://BUCKET/curated/
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Schemas explícitos: así Spark no adivina tipos (flight_date quedaría como fecha y
# price_cents podría salir mal tipado por las filas vacías).
FLIGHTS_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("route", StringType()),
    StructField("flight_date", StringType()),
    StructField("capacity", LongType()),
])

RESERVATIONS_SCHEMA = StructType([
    StructField("reservation_id", StringType()),
    StructField("flight_id", StringType()),
    StructField("fare_class", StringType()),
    StructField("price_cents", LongType()),   # vacío en el CSV -> null
    StructField("status", StringType()),
    StructField("days_before_departure", LongType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="carpeta con flights.csv y reservations.csv")
    ap.add_argument("--out", required=True, help="carpeta base de salida (curated/)")
    a = ap.parse_args()
    raw = a.raw.rstrip("/") + "/"
    out = a.out.rstrip("/") + "/"

    spark = SparkSession.builder.appName("parcial2-etl").getOrCreate()

    # ---------------------------------------------------------------- vuelos
    flights = (spark.read.csv(raw + "flights.csv", header=True, schema=FLIGHTS_SCHEMA)
               # C1: quitar TODO espacio y pasar a mayúsculas (' bog - mde ' -> 'BOG-MDE')
               .withColumn("route", F.upper(F.regexp_replace("route", r"\s+", "")))
               # mes del VUELO (no de la reserva): 'YYYY-MM'
               .withColumn("month", F.substring("flight_date", 1, 7)))

    # ------------------------------------------------------------- reservas
    res = (spark.read.csv(raw + "reservations.csv", header=True, schema=RESERVATIONS_SCHEMA)
           # C2: status sin espacios a los lados y en mayúsculas ('confirmed ' -> 'CONFIRMED')
           .withColumn("status", F.upper(F.trim("status")))
           # C3: solo CONFIRMED o CANCELLED (descarta PENDING, etc.)
           .filter(F.col("status").isin("CONFIRMED", "CANCELLED"))
           # C4: precio presente y > 0
           .filter(F.col("price_cents").isNotNull() & (F.col("price_cents") > 0))
           # C5: un solo registro por reservation_id. Los duplicados del generador son
           # copias exactas, así que da igual cuál se conserve.
           .dropDuplicates(["reservation_id"]))

    # ------------------------------------------- tabla 1: reservations_curated
    # La ruta y el mes salen del vuelo: las reservas crudas no tienen columna route.
    detail = (res.join(flights.select("flight_id", "route", "month"), on="flight_id", how="inner")
              .select("reservation_id", "flight_id", "route", "fare_class",
                      F.col("price_cents").cast("bigint").alias("price_cents"),
                      "status", "month"))

    (detail.repartition("month")            # un archivo por mes
           .write.mode("overwrite")
           .partitionBy("month")             # carpetas month=2023-01/ ... month=2024-12/
           .parquet(out + "reservations/"))

    # -------------------------------------------- tabla 2: route_month_metrics
    # Capacidad = suma de capacidad de TODOS los vuelos de la ruta en el mes,
    # tengan o no reservas.
    capacity = flights.groupBy("route", "month").agg(F.sum("capacity").alias("seats_capacity"))

    conf = F.col("status") == "CONFIRMED"
    sales = detail.groupBy("route", "month").agg(
        F.sum(F.when(conf, 1).otherwise(0)).alias("seats_sold"),
        F.sum(F.when(conf, F.col("price_cents")).otherwise(0)).alias("revenue_cents"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancellations"),
    )

    metrics = (capacity.join(sales, on=["route", "month"], how="left")
               .na.fill(0, ["seats_sold", "revenue_cents", "cancellations"])
               .select(
                   "route", "month",
                   F.col("seats_capacity").cast("bigint").alias("seats_capacity"),
                   F.col("seats_sold").cast("bigint").alias("seats_sold"),
                   (F.col("seats_sold") / F.col("seats_capacity")).cast("double").alias("occupancy"),
                   F.col("revenue_cents").cast("bigint").alias("revenue_cents"),
                   # contrato A.2: bookings cuenta solo CONFIRMED (igual a seats_sold)
                   F.col("seats_sold").cast("bigint").alias("bookings"),
                   F.col("cancellations").cast("bigint").alias("cancellations"),
               ))

    metrics.coalesce(1).write.mode("overwrite").parquet(out + "metrics/")

    spark.stop()


if __name__ == "__main__":
    main()
