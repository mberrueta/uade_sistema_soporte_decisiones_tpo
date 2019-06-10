#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"
HOST=${PG_UADE_BI_HOST:=localhost}
USER=${PG_UADE_BI_USER:=postgres}
PORT=${PG_UADE_BI_PORT:=5432}

echo $DIR/schema.sql

psql -h $HOST -U $USER -p $PORT  -a -q -f $DIR/schema.sql
psql -h $HOST -U $USER -p $PORT  -a -q -f $DIR/seed.sql
