import csv
import datetime
import logging
import luigi
import numpy as np
import pandas as pd
import sys

import src.etl.luigi.uade_lib as lib


class Fetch(luigi.Task):
    logger = logging.getLogger('luigi-interface')

    def run(self):
        # Doesn't work on mac
        # connection = lib.NeptunoDB.connection()
        print('Using input csv')

    def output(self):
        return luigi.LocalTarget('input/categorias.csv')


class Clean(luigi.Task):
    logger = logging.getLogger('luigi-interface')
    columns = ['id', 'name']

    def requires(self):
      return Fetch()

    def output(self):
        return luigi.LocalTarget("output/luigi/out_categories_cleaned.csv")

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

        result.append({'id': 9999, 'name': 'others'})

        with self.output().open('w') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=self.columns)
            writer.writeheader()

            for row in result:
                newRow = lib.SliceableDict(row).slice(*self.columns)
                writer.writerow(newRow)

class Insert(luigi.Task):
    table = 'dim_categories'
    columns = [ 'id', 'name' ]

    def requires(self):
        return Clean()

    def run(self):
        lib.PgInsert.exec(self.table, self.columns, self.input().path)
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_categories_done_{}.txt'.format(datetime.datetime.now().isoformat()))
