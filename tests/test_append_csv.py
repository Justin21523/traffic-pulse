from __future__ import annotations

import pandas as pd

from trafficpulse.storage.datasets import append_csv, load_csv


def test_append_csv_creates_file(tmp_path) -> None:
    path = tmp_path / "obs.csv"
    df = pd.DataFrame([{"a": 1, "b": 2}])
    append_csv(df, path)
    out = load_csv(path)
    assert out.to_dict(orient="records") == [{"a": 1, "b": 2}]


def test_append_csv_aligns_to_existing_header(tmp_path) -> None:
    path = tmp_path / "obs.csv"
    append_csv(pd.DataFrame([{"a": 1, "b": 2}]), path)
    append_csv(pd.DataFrame([{"a": 3, "c": 999}]), path)
    out = load_csv(path)
    assert list(out.columns) == ["a", "b"]
    assert out["a"].tolist() == [1, 3]
    assert out["b"].isna().tolist() == [False, True]

