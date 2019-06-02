import csv
import logging
import luigi

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
