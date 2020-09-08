# Core Library
import io
import json
import urllib.request
from datetime import datetime

# Third party
import pandas as pd
from prefect import Flow, task
from prefect.tasks.aws.s3 import S3Upload
from prefect.core.parameter import Parameter
from prefect.engine.results import LocalResult


@task(checkpoint=True, result=LocalResult(), target="{task_name}-{today}")
def download_data():
    with urllib.request.urlopen(
        "https://opendata.ecdc.europa.eu/covid19/casedistribution/json"
    ) as url:
        covid_data = json.loads(url.read().decode())["records"]
        covid_df = pd.DataFrame(covid_data)
        return covid_df


@task
def filter_data(covid_df, countries):
    return covid_df[covid_df.countriesAndTerritories.isin(countries)]


@task
def enrich_data(covid_df):
    covid_df["year_month"] = covid_df["year"] + "_" + covid_df["month"]
    return covid_df


@task
def prepare_data_for_upload(covid_df):
    csv_string = io.StringIO()
    covid_df.to_csv(csv_string)
    filename = f"covid-monthly-{datetime.now().isoformat()}.csv"
    return {"csv": csv_string.getvalue(), "filename": filename}


@task
def aggregate_data(covid_df):
    return (
        covid_df.groupby("year_month")
        .agg({"cases": "sum", "deaths": "sum"})
        .sort_index()
    )


@task
def print_data(data):
    # Only prints locally and does not log to cloud
    print(data)


upload_to_s3 = S3Upload()


def create_flow():
    with Flow("Covid analysis workflow") as flow:
        countries = Parameter("countries", default=["Germany"])
        bucket = Parameter("bucket", default="ludwigm-bucket")
        covid_df = download_data()
        filtered_covid_df = filter_data(covid_df, countries)
        prepared_df = enrich_data(filtered_covid_df)
        aggregated_df = aggregate_data(prepared_df)
        print_data(aggregated_df)
        csv_results = prepare_data_for_upload(aggregated_df)
        upload_to_s3(csv_results["csv"], csv_results["filename"], bucket=bucket)

    return flow


# def main():
#     flow = create_flow()
#     flow_id = flow.register(project_name="Demo")
#     client = Client()
#     flow_run_id = client.create_flow_run(
#         flow_id=flow_id
#     )
#     print(f"Flow Run id: {flow_run_id}")
#     flow.run_agent(show_flow_logs=True)
