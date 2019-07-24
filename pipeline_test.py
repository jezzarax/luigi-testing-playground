import pipeline
import pandas as pd
import luigi
import tempfile, os

def test_data_generator_a(monkeypatch):
    (_, tfname) = tempfile.mkstemp()
    def mock_output(*args, **argw):
        return luigi.LocalTarget(tfname)

    monkeypatch.setattr(pipeline.DataGenA, "output", mock_output)
    dg = pipeline.DataGenA()
    dg.run()
    
    task_output = pd.read_csv(tfname)

    assert len(task_output) == 2
    assert task_output.id.sum() == 3
