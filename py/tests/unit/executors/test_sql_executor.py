from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy

from gaarf.executors import sql_executor


class TestSqlAlchemyQueryExecutor:
  @pytest.fixture
  def engine(self):
    return sqlalchemy.create_engine('sqlite:///:memory:')

  @pytest.fixture
  def executor(self, engine):
    return sql_executor.SqlAlchemyQueryExecutor(engine)

  def test_execute_returns_data_saved_to_db(self, executor, engine):
    query = 'CREATE TABLE test AS SELECT 1 AS one;'
    executor.execute(script_name='test', query_text=query)

    with engine.connect() as connection:
      result = connection.execute(sqlalchemy.text('select one from test'))
      for row in result:
        assert row.one == 1

  def test_execute_returns_data_to_caller(self, executor):
    query = 'SELECT 1 AS one;'
    expected_result = pd.DataFrame(data=[[1]], columns=['one'])
    result = executor.execute(script_name='test', query_text=query)
    assert result.equals(expected_result)
