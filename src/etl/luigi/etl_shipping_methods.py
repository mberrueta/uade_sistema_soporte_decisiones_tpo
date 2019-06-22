import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # Pedidos
    # - Forma de envío
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        df = pd.read_csv('input/pedidos.csv')[['Forma de envío']]

        self.logger.info('==> Renaming columns')
        df = df.rename(index=str, columns={'Forma de envío': 'name'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_shipping_methods.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_shipping_method_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        df = pd.read_csv(self.input().path)

        self.logger.info('==> Droping duplicates')
        df = df.drop_duplicates()
        # df = df.reset_index()
        # df.columns[0] = 'id'
        # df['id'] = df.index
        df['id'] = range(1, len(df) + 1)

        self.logger.info('==> Adding others')
        df2 = pd.DataFrame([[999, 'others']], columns=['id', 'name'])
        df = df.append(df2)

        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_shipping_method_cleaned.csv')


class Insert(luigi.Task):
    table = 'dim_shipping_methods'
    columns = ['id', 'name']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_shipping_method_done.txt')
