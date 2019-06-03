import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Clientes
    #   - Id. de cliente
    #   - Nombre de compañía
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        clients_df = pd.read_csv(
            'input/clientes.csv')[['Id. de cliente', 'Nombre del contacto']]

        self.logger.info('==> Renaming columns')
        clients_df = clients_df.rename(index=str, columns={
                                         'Id. de cliente': 'id'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            clients_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_clients.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_clients_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        clients_df = pd.read_csv(self.input().path)

        self.logger.info('==> Build address dictionary')
        clients_df['id_address'] = clients_df['id'].apply(
            lambda orig_id: 'client|{}'.format(orig_id))
        
        self.logger.info('==> Build name column')
        clients_df['name'] = clients_df['Nombre del contacto'].apply(lambda name: name.split(' ')[0])
        
        self.logger.info('==> Build lastname column')
        clients_df['lastname'] = clients_df['Nombre del contacto'].apply(lambda name: name.split(' ')[1])

        with self.output().open('w') as out_file:
            clients_df.to_csv(out_file, index=False)


class Insert(luigi.Task):
    table = 'dim_clients'
    columns = ['id', 'name', 'lastname', 'id_address']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_clients_done_{}.txt'.format(datetime.datetime.now().isoformat()))
