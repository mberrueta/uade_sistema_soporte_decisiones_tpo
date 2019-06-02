import csv
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Productos
    #   - Id. de producto
    #   - Nombre de producto
    #   - Categoría
    #   - Suspendido
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        products_df = pd.read_csv(
            'input/productos.csv')[['Id. de producto', 'Nombre de producto', 'Categoría', 'Suspendido']]

        self.logger.info('==> Renaming columns')
        products_df = products_df.rename(index=str, columns={
                                         'Id. de producto': 'id', 'Nombre de producto': 'name', 'Categoría': 'category', 'Suspendido': 'suspended'})

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            products_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_products.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_products_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        products_df = pd.read_csv(self.input().path)

        self.logger.info('==> Build categories dictionary')
        categories = lib.DBRead.get('dim_categories')
        categories_dic = {}
        for k, v in categories:
            categories_dic[v] = k

        self.logger.info('==> Replacing categories name with id')
        products_df['category'] = products_df['category'].apply(
            lambda category_name: (
                categories_dic[category_name] if category_name in categories_dic else categories_dic['others']
            ))
        products_df = products_df.rename(index=str, columns={'category': 'id_category'})


        self.logger.info('==> Name cleaning')
        products_df['name'] = products_df['name'].apply(lambda name: lib.TransforHelper.text_clean(name))

        with self.output().open('w') as out_file:
            products_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_products_cleaned.csv')


class Insert(luigi.Task):
    table = 'dim_products'
    columns = ['id', 'name', 'id_category', 'suspended']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
