# ShopSense AI | Retail Intelligence and Real-Time Data Platform

An end-to-end retail data platform that turns orders, customer activity, inventory and product reviews into reliable business insights.

**Business question:** Which products are likely to run out of stock, what is driving their demand, and what should the business do about it?

## Roadmap

| Stage | Scope | Status |
|---|---|---|
| 1. Core platform | Retail data → S3-style lake → PySpark transformations → star-schema warehouse → dashboard | In progress |
| 2. Production reliability | Airflow orchestration, incremental loads, data-quality checks, monitoring, backfills, SCD Type 2 | Planned |
| 3. Streaming | Simulated live orders and clicks, deduplication, late-event handling | Planned |
| 4. AI | Review summaries and a business assistant that cites the metrics behind its answers | Planned |
