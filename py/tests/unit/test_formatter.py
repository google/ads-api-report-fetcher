import gaarf.io.formatter as formatter  # type: ignore


def test_results_formatter_format_with_empty_results():
    results = formatter.ResultsFormatter.format([])
    assert results == []


def test_results_formatter_format_with_non_empty_results():
    results = formatter.ResultsFormatter.format([1, 2, 3])
    assert results == [[1], [2], [3]]
