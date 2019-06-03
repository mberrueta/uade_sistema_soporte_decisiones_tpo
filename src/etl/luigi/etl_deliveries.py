import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Pedidos
    #     - Id. de pedido
    #     - Cliente
    #     - Fecha de pedido
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        deliveries_df = pd.read_csv(
            'input/pedidos.csv')[['Id. de pedido', 'Cliente', 'Fecha de envío']]

        self.logger.info('==> Renaming columns')
        deliveries_df = deliveries_df.rename(index=str, columns={
                                         'Id. de pedido': 'id', 'Cliente': 'client', 'Fecha de envío': 'date'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            deliveries_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_deliveries.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_deliveries_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        deliveries_df = pd.read_csv(self.input().path)

        self.logger.info('==> Build date column')
        deliveries_df['date'] = deliveries_df['date'].apply(
            lambda orig_date: lib.TransforHelper.date_to_id(orig_date) )

        # TODO: Seek client_id
        self.logger.info('==> Build clients dictionary')
        clients_dic = {
            'others': 'xxxx'
        }
        # clients = lib.DBRead.get('dim_clients')
        # for row in clients:
        #     clients_dic[v] = k

        self.logger.info('==> Replacing clients name with id')
        deliveries_df['client'] = deliveries_df['client'].apply(
            lambda client_name: (
                clients_dic[client_name] if client_name in clients_dic else clients_dic['others']
            ))
        deliveries_df = deliveries_df.rename(index=str, columns={'client': 'id_client'})


        with self.output().open('w') as out_file:
            deliveries_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_deliveries_cleaned.csv')


class Insert(luigi.Task):
    table = 'fact_deliveries'
    columns = ['id', 'id_client', 'id_date']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_deliveries_done_{}.txt'.format(datetime.datetime.now().isoformat()))
