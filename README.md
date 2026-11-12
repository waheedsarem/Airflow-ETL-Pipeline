# Daily currency exchange ETL

This Astro CLI project loads EUR reference rates for USD, GBP, and JPY into the `currency_etl.exchange_rates` MongoDB collection. Airflow schedules it daily at 17:15 UTC (22:15 PKT). Frankfurter may return the most recent published business-day rates on weekends and holidays, so the stored rate date comes from the API.

## Run locally

1. Start Docker Desktop with Linux containers and install the Astro CLI.
2. From this directory run `astro dev start`.
3. Open [Airflow](http://localhost:8080), unpause `currency_exchange_etl`, and trigger a manual run.
4. Open [Mongo Express](http://127.0.0.1:8081), then select `currency_etl` and `exchange_rates`.

The three tasks exchange data with explicit XCom keys `raw_exchange_rates` and `transformed_exchange_rates`. The loader uses a unique index and upserts by base currency, target currency, and rate date. A rerun for the same published date updates the existing documents.

MongoDB and Mongo Express join the Astro project network. Airflow and Mongo Express use the `mongodb` service name to reach the database. MongoDB has no published host port. Mongo Express is bound to the host loopback interface for local use. MongoDB stores data in the `exchange_rates_data` named volume. Use `astro dev stop` and `astro dev start` to stop and resume the stack with its data.

[Frankfurter API documentation](https://frankfurter.dev/)
