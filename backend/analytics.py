import os
import time

import boto3
from fastapi import APIRouter, HTTPException, Query

REGION = os.getenv("AWS_REGION", "us-east-1")
DATABASE = os.getenv("ATHENA_DATABASE", "airline_analytics")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
OUTPUT = os.getenv("ATHENA_OUTPUT")
TIMEOUT_S = 9.0

router = APIRouter()
_athena = boto3.client("athena", region_name=REGION)

SQL = ("SELECT seats_sold, seats_capacity, occupancy "
       "FROM route_month_metrics WHERE route = ? AND month = ?")


def _consultar(route: str, month: str):
    kw = dict(
        QueryString=SQL,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ExecutionParameters=[f"'{route}'", f"'{month}'"],
    )
    if OUTPUT:
        kw["ResultConfiguration"] = {"OutputLocation": OUTPUT}
    qid = _athena.start_query_execution(**kw)["QueryExecutionId"]

    inicio = time.monotonic()
    while True:
        estado = _athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if estado["State"] == "SUCCEEDED":
            break
        if estado["State"] in ("FAILED", "CANCELLED"):
            raise HTTPException(502, f"Athena {estado['State']}: {estado.get('StateChangeReason', '')}")
        if time.monotonic() - inicio > TIMEOUT_S:
            _athena.stop_query_execution(QueryExecutionId=qid)
            raise HTTPException(504, "Athena tardó demasiado")
        time.sleep(0.2)

    filas = _athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return filas[1:]


@router.get("/analytics/occupancy")
def occupancy(route: str = Query(..., pattern=r"^[A-Z]{3}-[A-Z]{3}$"),
              month: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    filas = _consultar(route, month)
    if not filas:
        raise HTTPException(404, "sin datos para esa ruta y mes")
    sold, cap, occ = (c.get("VarCharValue") for c in filas[0]["Data"])
    return {
        "route": route,
        "month": month,
        "occupancy": float(occ),
        "seats_sold": int(sold),
        "seats_capacity": int(cap),
    }
