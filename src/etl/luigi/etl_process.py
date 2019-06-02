import datetime
import luigi
import subprocess


import src.etl.luigi.etl_address as address
import src.etl.luigi.etl_category as category
import src.etl.luigi.etl_products as products
import src.etl.luigi.etl_providers as providers

# TODO: doesn't work
class ProcessAll(luigi.Task):
    def requires(self):
        tasks_to_run = []
        tasks_to_run.append(address.Insert())
        tasks_to_run.append(category.Insert())
        tasks_to_run.append(products.Insert())
        tasks_to_run.append(providers.Insert())

        for task in tasks_to_run:
            yield task

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write('ok')

    def output(self):
        return luigi.LocalTarget('output/luigi/out_process_done.txt')

    # def run(self):
    #     subprocess.call(["touch", "pipelineComplete.txt"])

    # def output(self):
    #     return luigi.LocalTarget("pipelineComplete.txt")