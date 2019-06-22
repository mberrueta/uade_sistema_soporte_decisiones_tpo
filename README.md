# Sistema soporte decisiones

## About

TPO sistema de soprte de decisiones

## Setup DB

- Proceed to clone the repo.
- Make sure that you have java, docker & pentaho installed in your machine.
- Use the docker compose database or any PG11
  - `docker-compose up -d`

### Create local db

set the env vars:

- PG_UADE_BI_HOST
- PG_UADE_BI_USER
- PG_UADE_BI_PORT

`db/staging/create_db.sh`

## Using it one by one

In order fire the transformations we need python3 installed (use conda for instance) and install the dependencies

```sh
conda activate py37 # if use conda
pip install -r src/etl/luigi/requirements.txt --no-index  --find-links file:/tmp/packages
PYTHONPATH='.' luigi --module src.etl.luigi.fetch_categoria Insert --local-scheduler
```

Run all

```sh
./src/etl/luigi/run.sh
```


## DB stats

```sql
SELECT schemaname,relname,n_live_tup
  FROM pg_stat_user_tables 
  ORDER BY n_live_tup DESC;
```

## Db export

```sh
pg_dump   --host "localhost" --port "5732" --username "postgres" --no-password --verbose --data-only --column-inserts   --format=p "support_system_decisions_staging"  > ./db/export/dbexport_support_system_decisions_staging.pgsql
```
