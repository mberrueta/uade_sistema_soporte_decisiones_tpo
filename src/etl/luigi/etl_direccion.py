import csv
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Clientes
    #     - Id. de cliente
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    # - Empleados
    #     - Id. de empleado
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    # - Pedidos
    #     - Id. de pedido
    #     - Ciudad de destinatario
    #     - Región de destinatario
    #     - Código postal de destinatario
    #     - País de destinatario
    # - Proveedores
    #     - Id. de proveedor
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        self.logger.info('==> Reading original csv''s')
        clientes_df = pd.read_csv(
            'input/clientes.csv')[['Id. de cliente', 'Ciudad', 'Región', 'Código postal', 'País']]
        empleados_df = pd.read_csv(
            'input/empleados.csv')[['Id. de empleado', 'Ciudad', 'Región', 'Código postal', 'País']]
        pedidos_df = pd.read_csv(
            'input/pedidos.csv')[['Id. de pedido', 'Ciudad de destinatario', 'Región de destinatario', 'Código postal de destinatario', 'País de destinatario']]
        proveedores_df = pd.read_csv(
            'input/proveedores.csv')[['Id. de proveedor', 'Ciudad', 'Región', 'Código postal', 'País']]

        self.logger.info('==> Renaming columns')
        clientes_df = clientes_df.rename(index=str, columns={
                                         'Id. de cliente': 'id', 'Ciudad': 'state', 'Región': 'region', 'Código postal': 'postal_code', 'País': 'country'})
        empleados_df = empleados_df.rename(index=str, columns={
                                           'Id. de empleado': 'id', 'Ciudad': 'state', 'Región': 'region', 'Código postal': 'postal_code', 'País': 'country'})
        pedidos_df = pedidos_df.rename(index=str, columns={'Id. de pedido': 'id', 'Ciudad de destinatario': 'state',
                                                           'Región de destinatario': 'region', 'Código postal de destinatario': 'postal_code', 'País de destinatario': 'country'})
        proveedores_df = proveedores_df.rename(index=str, columns={
                                               'Id. de proveedor': 'id', 'Ciudad': 'state', 'Región': 'region', 'Código postal': 'postal_code', 'País': 'country'})

        self.logger.info('==> Replacing id columns')
        clientes_df['id'] = clientes_df['id'].apply(
            lambda orig_id: 'cli|{}'.format(orig_id))
        empleados_df['id'] = empleados_df['id'].apply(
            lambda orig_id: 'emp|{}'.format(orig_id))
        pedidos_df['id'] = pedidos_df['id'].apply(
            lambda orig_id: 'ped|{}'.format(orig_id))
        proveedores_df['id'] = proveedores_df['id'].apply(
            lambda orig_id: 'prov|{}'.format(orig_id))

        out = clientes_df.append(empleados_df).append(
            pedidos_df).append(proveedores_df)

        self.logger.info('==> Writting')
        with self.output().open('w') as out_file:
            out.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_addresses.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')
    columns = ['id', 'state', 'region', 'postal_code', 'country']

    def requires(self):
        return Fetch()

    def output(self):
        return luigi.LocalTarget('output/luigi/out_addresses_cleaned.csv')

    def run(self):
        self.logger.info('==> Reading: {}'.format(self.input().path))
        addresses_df = pd.read_csv(self.input().path)

        addresses_df.loc[addresses_df['region'].isnull(),
                         'region'] = 'no-region'
        addresses_df.loc[addresses_df['region'] ==
                         'Isla de Wight', 'region'] = 'Isle de Wight'
        addresses_df.loc[addresses_df['state'].isnull(), 'state'] = 'no-state'
        addresses_df.loc[addresses_df['postal_code'].isnull(),
                         'postal_code'] = 'no-postal-code'
        addresses_df.loc[addresses_df['country'].isnull(),
                         'country'] = 'no-country'

        with self.output().open('w') as out_file:
            addresses_df.to_csv(out_file, index=False)

    def output(self):
        return luigi.LocalTarget('output/luigi/out_addresses_cleaned.csv')


class Insert(luigi.Task):
    table = 'dim_addresses'
    columns = ['id', 'state', 'region', 'country', 'postal_code']

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
