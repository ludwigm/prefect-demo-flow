# Core Library
import io
import json
import urllib.request
from typing import Any, Dict
from datetime import datetime

# Third party
import pandas as pd
import prefect
from prefect import Flow, task
from prefect.environments import LocalEnvironment
from prefect.tasks.aws.s3 import S3Upload
from prefect.core.parameter import Parameter
from prefect.engine.results import LocalResult
from prefect.engine.executors import LocalDaskExecutor

DEFAULT_BUCKET = "ludwigm-bucket"
DEFAULT_COUNTRY = "Germany"
COVID_DATA_URL = "https://opendata.ecdc.europa.eu/covid19/casedistribution/json"
FLOW_NAME = "Covid analysis workflow"


@task(checkpoint=True, result=LocalResult(), target="{task_name}-{today}")
def download_data() -> pd.DataFrame:
    with urllib.request.urlopen(COVID_DATA_URL) as url:
        covid_data = json.loads(url.read().decode())["records"]
        covid_df = pd.DataFrame(covid_data)
        return covid_df


@task
def filter_data(covid_df: pd.DataFrame, country: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info(f"Filtering data for country: {country}")
    return covid_df[covid_df.countriesAndTerritories == country].copy()


@task
def enrich_data(covid_df: pd.DataFrame) -> pd.DataFrame:
    enriched_df = covid_df.copy()
    enriched_df["year_month"] = enriched_df["year"] + "_" + enriched_df["month"]
    return enriched_df


@task
def prepare_data_for_upload(covid_df: pd.DataFrame) -> Dict[str, str]:
    csv_string = io.StringIO()
    covid_df.to_csv(csv_string)
    filename = f"covid-monthly-{datetime.now().isoformat()}.csv"
    return {"csv": csv_string.getvalue(), "filename": filename}


@task
def aggregate_data(covid_df: pd.DataFrame) -> pd.DataFrame:
    return (
        covid_df.groupby("year_month")
        .agg({"cases": "sum", "deaths": "sum"})
        .sort_index()
    )


@task
def print_data(data: Any) -> None:
    # Only prints locally and does not log to cloud
    print(data)


upload_to_s3 = S3Upload()


def create_flow():
    local_parallelizing_environment = LocalEnvironment(executor=LocalDaskExecutor())

    with Flow(FLOW_NAME, environment=local_parallelizing_environment) as flow:
        country = Parameter("country", default=DEFAULT_COUNTRY)
        bucket = Parameter("bucket", default=DEFAULT_BUCKET)
        covid_df = download_data()
        filtered_covid_df = filter_data(covid_df, country)
        prepared_df = enrich_data(filtered_covid_df)
        aggregated_df = aggregate_data(prepared_df)
        print_data(aggregated_df)
        csv_results = prepare_data_for_upload(aggregated_df)
        upload_to_s3(csv_results["csv"], csv_results["filename"], bucket=bucket)

    return flow
