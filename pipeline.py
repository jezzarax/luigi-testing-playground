import pandas as pd
import luigi

class DataGenA(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("./data/a.csv")

    def run(self):
        self.output().makedirs()

        data_to_output = pd.DataFrame([
            [1, "customer 1"],
            [2, "customer 2"],
        ], columns=["id", "title"])

        data_to_output.to_csv(self.output().path, index=False)



