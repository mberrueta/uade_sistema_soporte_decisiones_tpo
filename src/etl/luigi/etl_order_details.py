import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Detalle Pedidos
    #   - Id. de pedido
    #   - Producto
    #   - Precio por unidad
    #   - Cantidad
    #   - Descuento
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        order_details_df = pd.read_csv(
            'input/detalle_pedidos.csv')[[
                'Id. de pedido', 'Producto', 'Precio por unidad', 'Cantidad', 'Descuento']]

        self.logger.info('==> Renaming columns')
        order_details_df = order_details_df.rename(index=str, columns={
                                         'Id. de pedido': 'id_order',
                                         'Producto': 'product',
                                         'Precio por unidad': 'price',
                                         'Cantidad': 'quantity',
                                         'Descuento': 'discount'
                                         })

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            order_details_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_order_details.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_order_details_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        order_details_df = pd.read_csv(self.input().path)

        self.logger.info('==> Split price symbols')
        order_details_df['unit_price'] = order_details_df['price'].apply(
            lambda orig_price: lib.TransforHelper.remove_currency(orig_price) )
        order_details_df['currency'] = order_details_df['price'].apply(
            lambda orig_price: lib.TransforHelper.get_currency(orig_price) )


        self.logger.info('==> Build products dictionary (to get id)')
        products = lib.DBRead.get('dim_products')
        for row in products:
            print(row)
            # products_dic[v] = k

        # self.logger.info('==> Replacing clients name with id')
        # order_details_df['client'] = order_details_df['client'].apply(
        #     lambda client_name: (
        #         clients_dic[client_name] if client_name in clients_dic else clients_dic['others']
        #     ))
        # order_details_df = order_details_df.rename(index=str, columns={'client': 'id_client'})


        # with self.output().open('w') as out_file:
        #     order_details_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_order_details_cleaned.csv')


class Insert(luigi.Task):
    table = 'fact_orders'
    columns = ['id', 'id_client', 'id_date']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_order_details_done_{}.txt'.format(datetime.datetime.now().isoformat()))
