import pytest

from gaarf.query_post_processor import PostProcessorMixin


@pytest.fixture
def template_query_with_if_else():
  return "SELECT field_one, {% if key == 'field_2' %} field_two {% else %} field_three {% endif %} FROM some_table"


@pytest.fixture
def template_query_with_for_loop():
  return 'SELECT field_one, {% for day in cohort_days %} {{day}} AS day_{{day}}, {% endfor %} FROM some_table'


@pytest.fixture
def macro_template_query_with_for_loop():
  return 'SELECT field_one, {field_2} AS field_two, {% for day in cohort_days %} {{day}} AS day_{{day}}, {% endfor %} FROM some_table'


def test_expand_jinja_if_template_parameters_are_provided_as_comma_separated_string(
  template_query_with_for_loop,
):
  expected_query = 'SELECT field_one, 1 AS day_1, 2 AS day_2, FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_for_loop, {'cohort_days': '1,2'}
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_template_parameters_are_provided_as_list(
  template_query_with_for_loop,
):
  expected_query = 'SELECT field_one, 1 AS day_1, 2 AS day_2, FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_for_loop, {'cohort_days': [1, 2]}
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_empty_template_parameter_is_provided(
  template_query_with_for_loop,
):
  expected_query = 'SELECT field_one, FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_for_loop
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_single_template_parameter_is_provided_returns_if_block(
  template_query_with_if_else,
):
  expected_query = 'SELECT field_one, field_two FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_if_else, {'key': 'field_2'}
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_single_template_parameter_is_provided_returns_else_block(
  template_query_with_if_else,
):
  expected_query = 'SELECT field_one, field_three FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_if_else, {'key': 'field_3'}
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_no_single_template_parameter_is_provided_returns_else_block(
  template_query_with_if_else,
):
  expected_query = 'SELECT field_one, field_three FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_if_else
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_if_empty_single_template_parameter_is_provided_returns_else_block(
  template_query_with_if_else,
):
  expected_query = 'SELECT field_one, field_three FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_if_else, {'key': None}
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_expand_jinja_not_template_parameter_is_provided(
  template_query_with_for_loop,
):
  expected_query = 'SELECT field_one, FROM some_table'
  rendered_query = PostProcessorMixin().expand_jinja(
    template_query_with_for_loop
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_replace_params_template(macro_template_query_with_for_loop):
  expected_query = 'SELECT field_one, field_two AS field_two, 1 AS day_1, 2 AS day_2, FROM some_table'
  rendered_query = PostProcessorMixin().replace_params_template(
    macro_template_query_with_for_loop,
    params={
      'macro': {'field_2': 'field_two'},
      'template': {'cohort_days': '1,2'},
    },
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_replace_params_empty_template_is_ignored(
  macro_template_query_with_for_loop,
):
  expected_query = 'SELECT field_one, field_two AS field_two, FROM some_table'
  rendered_query = PostProcessorMixin().replace_params_template(
    macro_template_query_with_for_loop,
    params={'macro': {'field_2': 'field_two'}},
  )
  assert rendered_query.replace('  ', ' ') == expected_query


def test_replace_empty_params(macro_template_query_with_for_loop):
  expected_query = 'SELECT field_one, {field_2} AS field_two, FROM some_table'
  rendered_query = PostProcessorMixin().replace_params_template(
    macro_template_query_with_for_loop, params={}
  )
  assert rendered_query.replace('  ', ' ') == expected_query
