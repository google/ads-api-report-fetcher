from gaarf.bq_executor import extract_datasets


def test_extract_datasets():
    macros = {
        "start_date": ":YYYYMMDD",
        "bq_dataset": "dataset_1",
        "dataset_new": "dataset_2",
        "legacy_dataset_old": "dataset_3",
        "wrong_dts": "dataset_4"
    }

    expected = ["dataset_1", "dataset_2", "dataset_3"]
    datasets = extract_datasets(macros)
    assert datasets == expected
