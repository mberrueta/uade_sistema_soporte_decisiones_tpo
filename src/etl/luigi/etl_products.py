import csv
import datetime
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
    #   - Proveedor
    #   - Categoría
    #   - Suspendido
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        products_df = pd.read_csv(
            'input/productos.csv')[
                [
                    'Id. de producto', 'Nombre de producto', 'Proveedor',
                    'Categoría', 'Suspendido'
                ]
            ]

        self.logger.info('==> Renaming columns')
        products_df = products_df.rename(index=str, columns={
                                         'Id. de producto': 'id',
                                         'Nombre de producto': 'name',
                                         'Proveedor': 'provider',
                                         'Categoría': 'category',
                                         'Suspendido': 'suspended'})

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

        self.logger.info('==> Build providers dictionary')
        providers = lib.DBRead.get('dim_providers')
        providers_dic = {}
        for k, v, a in providers:
            print(k,v,a)
            providers_dic[v] = k

        self.logger.info('==> Replacing providers name with id')
        products_df['provider'] = products_df['provider'].apply(
            lambda provider_name: (
                providers_dic[provider_name] if provider_name in providers_dic else providers_dic['others']
            ))
        products_df = products_df.rename(index=str, columns={'provider': 'id_provider'})

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

        # Add dummy product
        df2 = pd.DataFrame([[9999, 'others', 9999, False]], columns=['id', 'name', 'id_category', 'suspended'])
        products_df = products_df.append(df2)

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
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_products_done_{}.txt'.format(datetime.datetime.now().isoformat()))
