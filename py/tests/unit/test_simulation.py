import pytest

from gaarf.simulation import simulate_data, SimulatorSpecification
from gaarf.query_editor import QuerySpecification


@pytest.fixture
def query():
    return """
        SELECT
            segments.date AS date,
            campaign.advertising_channel_sub_type AS campaign_type,
            campaign.id as campaign_id,
            campaign.name AS campaign_name,
            asset.youtube_video_asset.youtube_video_id AS video_id
        FROM campaign
        """


@pytest.fixture
def query_specification(query):
    return QuerySpecification(query).generate()


@pytest.fixture
def default_simulator_specification():
    return SimulatorSpecification()


@pytest.fixture
def default_report(query, default_simulator_specification):
    return simulate_data(query, default_simulator_specification)


def test_n_rows_have_been_generated(default_report,
                                    default_simulator_specification):
    assert len(default_report) == default_simulator_specification.n_rows


def test_n_cols_have_been_generated(default_report, query_specification):
    assert len(default_report.results[0]) == len(query_specification.fields)


@pytest.fixture
def allowed_enums():
    return ["APP_CAMPAIGN", "APP_CAMPAIGN_FOR_ENGAGEMENT"]


@pytest.fixture
def replacements():
    return {"asset.youtube_video_asset.youtube_video_id": ["12345", "54321"]}


@pytest.fixture
def custom_simulator_specification(allowed_enums, replacements):
    return SimulatorSpecification(
        allowed_enums={"campaign.advertising_channel_sub_type": allowed_enums},
        replacements=replacements)


@pytest.fixture
def custom_report(query, custom_simulator_specification):
    return simulate_data(query, custom_simulator_specification)


def test_allowed_enums_were_simulated(custom_report,
                                      custom_simulator_specification,
                                      allowed_enums):
    report_iterator = iter(custom_report)
    assert next(report_iterator).get("campaign_type") in allowed_enums
    assert next(report_iterator).get("campaign_type") in allowed_enums


def test_replacements_were_applied(custom_report, custom_simulator_specification,
                      replacements):
    report_iterator = iter(custom_report)
    replacement = replacements.get("asset.youtube_video_asset.youtube_video_id")
    assert next(report_iterator).get("video_id") in replacement
    assert next(report_iterator).get("video_id") in replacement
