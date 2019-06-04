#!/bin/bash

PYTHONPATH=${PYTHONPATH:=.}

echo $PYTHONPATH

echo 'Removing previous'
echo '================='
find  ./output/luigi  -name 'out_*' -delete

echo 'Dimensions'
echo '=========='
echo '>>>>>>>>>>>> etl_address'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_address Insert --local-scheduler
echo '>>>>>>>>>>>> etl_category'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_category Insert --local-scheduler
echo '>>>>>>>>>>>> etl_providers'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_providers Insert --local-scheduler
echo '>>>>>>>>>>>> etl_products'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_products Insert --local-scheduler
echo '>>>>>>>>>>>> etl_clients'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_clients Insert --local-scheduler


echo 'Facts'
echo '====='
echo '>>>>>>>>>>>> etl_shippings'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_shippings Insert --local-scheduler
echo '>>>>>>>>>>>> etl_deliveries'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_deliveries Insert --local-scheduler
echo '>>>>>>>>>>>> etl_orders'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_orders Insert --local-scheduler
echo '>>>>>>>>>>>> etl_order_details'
PYTHONPATH='.' luigi --module src.etl.luigi.etl_order_details Insert --local-scheduler