import csv
import logging
import luigi
import pandas, sys
import pandas as pd

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    # - Proveedores
    #     - Dirección
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    # - Pedidos
    #     - Dirección de destinatario
    #     - Ciudad de destinatario
    #     - Región de destinatario
    #     - Código postal de destinatario
    #     - País de destinatario
    # - Empleados
    #     - Dirección
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    # - Clientes
    #     - Dirección
    #     - Ciudad
    #     - Región
    #     - Código postal
    #     - País
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        print('Using input csv')
        clientes_df = pd.read_csv('input/clientes.csv')[['Dirección', 'Ciudad', 'Región', 'Código postal', 'País']]
        empleados_df = pd.read_csv('input/empleados.csv')[['Dirección', 'Ciudad', 'Región', 'Código postal', 'País']]
        pedidos_df = pd.read_csv('input/pedidos.csv')[['Dirección de destinatario', 'Ciudad de destinatario', 'Región de destinatario', 'Código postal de destinatario', 'País de destinatario']]
        proveedores_df = pd.read_csv('input/proveedores.csv')[['Dirección', 'Ciudad', 'Región', 'Código postal', 'País']]


    def output(self):
        return luigi.LocalTarget('/tmp/uade/luigi/out_direcciones.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')
    columns = ['id', 'name']

    def requires(self):
      return Fetch()

    def output(self):
        return luigi.LocalTarget("/tmp/uade/luigi/out_categorias_cleaned.csv")

    def run(self):
        result = []
        with self.input().open('r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    id = row[0]
                    # name = row[1].strip().strip('"')
                    name = lib.TransforHelper.text_clean(row[1])
                    if not name:
                        name = 'no_name'
                    result.append({'id': id, 'name': name})

        with self.output().open('w') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=self.columns)
            writer.writeheader()

            for row in result:
                newRow = lib.SliceableDict(row).slice(*self.columns)
                writer.writerow(newRow)

class Insert(luigi.Task):
    table = 'dim_categorias'
    columns = [ 'id', 'name' ]

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
