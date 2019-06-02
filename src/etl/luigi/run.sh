#!/bin/bash

PYTHONPATH=${PYTHONPATH:=.}

echo $PYTHONPATH

find  ./output/luigi  -name 'out_*' -delete
PYTHONPATH='.' luigi --module src.etl.luigi.etl_category Insert --local-scheduler
PYTHONPATH='.' luigi --module src.etl.luigi.etl_address Insert --local-scheduler
PYTHONPATH='.' luigi --module src.etl.luigi.etl_providers Insert --local-scheduler
PYTHONPATH='.' luigi --module src.etl.luigi.etl_products Insert --local-scheduler
PYTHONPATH='.' luigi --module src.etl.luigi.etl_orders Insert --local-scheduler