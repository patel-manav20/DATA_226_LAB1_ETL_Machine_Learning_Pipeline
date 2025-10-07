**Stock Price Forecast Lab (Airflow + Snowflake + yfinance)**

Short-horizon stock forecasting for Apple (AAPL) and NVIDIA (NVDA) using a warehouse-native workflow:

Ingest daily OHLCV from yfinance

Transactional stage-and-merge into Snowflake

Train and infer 7-day forecasts with SNOWFLAKE.ML.FORECAST

Publish a single final table that unions ACTUAL + FORECAST rows

Orchestrate everything with Apache Airflow

**What’s inside**

Two Airflow DAGs:

stock_ingest_yf — daily ETL at 02:15 (cron 15 2 * * *)

stock_forecast_ml — triggered by ingest DAG

Snowflake schemas/tables:

RAW: STOCK_PRICES, STOCK_PRICES_STG

ANALYTICS: V_STOCK_CLOSE_TS, STOCK_FORECAST_7D, STOCK_PRICES_FINAL

Parametrized via Airflow Variables & secure Connection

**Conclusion**

This lab implements a clean, scalable pipeline for short-horizon equity forecasting. Daily prices for Apple (AAPL) and NVIDIA (NVDA) are ingested from yfinance, written to Snowflake via a transactional stage-and-merge pattern, and orchestrated end-to-end with Apache Airflow. Forecasts are generated in-database using SNOWFLAKE.ML.FORECAST (7-day horizon) and published alongside historicals in a single analytics table, making downstream analysis and visualization straightforward and reliable. The design is reproducible, cloud-native, and easy to extend.


This work was completed by me and my lab partner, Parth Patel.
