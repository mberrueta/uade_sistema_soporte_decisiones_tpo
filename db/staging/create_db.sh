#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"
HOST=${PG_HOST:=localhost}
USER=${PG_USER:=postgres}
PORT=${PG_PORT:=5532}

echo $DIR/schema.sql

psql -h $HOST -U $USER -p $PORT  -a -q -f $DIR/schema.sql
psql -h $HOST -U $USER -p $PORT  -a -q -f $DIR/seed.sql
