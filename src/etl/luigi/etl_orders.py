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
    #     - Forma de envío
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        orders_df = pd.read_csv(
            'input/pedidos.csv')[['Id. de pedido', 'Cliente', 'Fecha de pedido', 'Forma de envío']]

        self.logger.info('==> Renaming columns')
        orders_df = orders_df.rename(index=str, columns={
                                         'Id. de pedido': 'id', 'Cliente': 'client', 'Fecha de pedido': 'date', 'Forma de envío': 'shipping_method'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            orders_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_orders.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_orders_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        orders_df = pd.read_csv(self.input().path)

        self.logger.info('==> Build address dictionary')
        orders_df['date'] = orders_df['date'].apply(
            lambda orig_date: lib.TransforHelper.date_to_id(orig_date) )

        self.logger.info('==> Build clients dictionary')
        clients_dic = {}
        clients = lib.DBRead.get('dim_clients')
        for id, company_name, name, last_name, id_address in clients:
            clients_dic[company_name] = id

        self.logger.info('==> Replacing clients name with id')
        orders_df['client'] = orders_df['client'].apply(
            lambda client_name: (
                clients_dic[client_name] if client_name in clients_dic else clients_dic['others']
            ))
        orders_df = orders_df.rename(index=str, columns={'client': 'id_client'})

        self.logger.info('==> Build shipping_methods dictionary')
        shipping_methods_dic = {}
        shipping_methods = lib.DBRead.get('dim_shipping_methods')
        for id, name in shipping_methods:
            shipping_methods_dic[name] = id

        self.logger.info('==> Replacing shipping_methods name with id')
        orders_df['shipping_method'] = orders_df['shipping_method'].apply(
            lambda shipping_method_name: (
                shipping_methods_dic[shipping_method_name] if shipping_method_name in shipping_methods_dic else shipping_methods_dic['others']
            ))
        orders_df = orders_df.rename(index=str, columns={'shipping_method': 'id_shipping_method'})


        with self.output().open('w') as out_file:
            orders_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_orders_cleaned.csv')


class Insert(luigi.Task):
    table = 'fact_orders'
    columns = ['id', 'id_client', 'id_date', 'id_shipping_method']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_orders_done_{}.txt'.format(datetime.datetime.now().isoformat()))
