import pytest
from gaarf.base_query import BaseQuery


@pytest.fixture
def fake_query():
    class FakeQuery(BaseQuery):
        def __init__(self):
            self.query_text = "SELECT 1"
    fake_query = FakeQuery()
    return fake_query


@pytest.fixture
def wrong_query():
    class WrongQuery(BaseQuery):
        def __init__(self):
            self.query = "SELECT 1"
    wrong_query = WrongQuery()
    return wrong_query


def test_base_query_init_raises_error():
    with pytest.raises(NotImplementedError):
        base_query = BaseQuery()


def test_implemented_str(fake_query):
    assert str(fake_query) == "SELECT 1"


def test_not_implemented_str(wrong_query):
    with pytest.raises(NotImplementedError):
        print(wrong_query)
