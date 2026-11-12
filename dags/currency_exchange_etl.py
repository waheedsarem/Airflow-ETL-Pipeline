"""Load daily EUR reference rates from Frankfurter into MongoDB."""
import json
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

import pendulum
from airflow.sdk import dag, get_current_context, task
from pymongo import ASCENDING, MongoClient

BASE = "EUR"
QUOTES = ("USD", "GBP", "JPY")
API = "https://api.frankfurter.dev/v2/rates"
RAW_XCOM = "raw_exchange_rates"
TRANSFORMED_XCOM = "transformed_exchange_rates"
LOG = logging.getLogger(__name__)


@dag(
    dag_id="currency_exchange_etl",
    schedule="15 17 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["mlops", "frankfurter", "mongodb"],
)
def currency_exchange_etl():
    @task(do_xcom_push=False)
    def extract_currency_exchange_rates():
        params = urlencode({"base": BASE, "quotes": ",".join(QUOTES)})
        with urlopen(f"{API}?{params}", timeout=30) as response:
            rows = json.load(response)
        retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if not isinstance(rows, list) or not rows:
            raise ValueError("Frankfurter returned no rate records")
        get_current_context()["ti"].xcom_push(
            key=RAW_XCOM, value={"rows": rows, "retrieved_at": retrieved_at}
        )
        LOG.info("Extracted %d Frankfurter records", len(rows))

    @task(do_xcom_push=False)
    def transform_exchange_rate_data():
        ti = get_current_context()["ti"]
        payload = ti.xcom_pull(task_ids="extract_currency_exchange_rates", key=RAW_XCOM)
        records = []
        for row in payload["rows"]:
            base, quote = row["base"], row["quote"]
            rate_date = date.fromisoformat(row["date"]).isoformat()
            rate = float(row["rate"])
            if base != BASE or quote not in QUOTES or not math.isfinite(rate) or rate <= 0:
                raise ValueError(f"Unexpected exchange rate for {base}/{quote} on {rate_date}")
            records.append({
                "base_currency": base,
                "target_currency": quote,
                "exchange_rate": rate,
                "rate_date": rate_date,
                "source": "Frankfurter API",
                "retrieved_at": payload["retrieved_at"],
            })
        if len(records) != len(QUOTES) or {r["target_currency"] for r in records} != set(QUOTES):
            raise ValueError("Expected one exchange rate for each requested quote currency")
        ti.xcom_push(key=TRANSFORMED_XCOM, value=records)
        LOG.info("Transformed %d records for %s", len(records), ", ".join(QUOTES))

    @task(do_xcom_push=False)
    def load_data_into_mongodb():
        records = get_current_context()["ti"].xcom_pull(
            task_ids="transform_exchange_rate_data", key=TRANSFORMED_XCOM
        )
        key_fields = ("base_currency", "target_currency", "rate_date")
        uri = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
        with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
            collection = client["currency_etl"]["exchange_rates"]
            collection.create_index([(field, ASCENDING) for field in key_fields], unique=True)
            inserted = 0
            for record in records:
                key = {field: record[field] for field in key_fields}
                result = collection.update_one(key, {"$set": record}, upsert=True)
                inserted += result.upserted_id is not None
            LOG.info("Loaded %d rates into currency_etl.exchange_rates (%d inserted, %d matched)",
                     len(records), inserted, len(records) - inserted)

    extract_currency_exchange_rates() >> transform_exchange_rate_data() >> load_data_into_mongodb()


currency_exchange_etl()
