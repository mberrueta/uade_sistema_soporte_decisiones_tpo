import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Proveedores
    #   - Id. de proveedor
    #   - Nombre de compañía
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        providers_df = pd.read_csv(
            'input/proveedores.csv')[['Id. de proveedor', 'Nombre de compañía']]

        self.logger.info('==> Renaming columns')
        providers_df = providers_df.rename(index=str, columns={
                                         'Id. de proveedor': 'id', 'Nombre de compañía': 'name'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            providers_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_providers.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_providers_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        providers_df = pd.read_csv(self.input().path)

        self.logger.info('==> Build address dictionary')
        providers_df['id_address'] = providers_df['id'].apply(
            lambda orig_id: 'prov|{}'.format(orig_id))

        self.logger.info('==> Name cleaning')
        providers_df['name'] = providers_df['name'].apply(lambda name: lib.TransforHelper.text_clean(name))

        with self.output().open('w') as out_file:
            providers_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_providers_cleaned.csv')


class Insert(luigi.Task):
    table = 'dim_providers'
    columns = ['id', 'name', 'id_address']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_providers_done_{}.txt'.format(datetime.datetime.now().isoformat()))
