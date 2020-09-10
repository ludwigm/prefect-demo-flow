# Third party
import click
from prefect.client.client import Client

# First party
from demo_flow.flow import DEFAULT_BUCKET, DEFAULT_COUNTRY, create_flow

PROJECT_NAME = "Demo"

bucket_option = click.option(
    "-b", "--bucket", "bucket", required=False, default=DEFAULT_BUCKET,
)

country_filter = click.option(
    "-c", "--country", "country", required=False, default=DEFAULT_COUNTRY,
)


@click.group()
def main():
    pass


@main.command()
def visualize():
    flow = create_flow()
    flow.visualize()


@main.command()
@bucket_option
@country_filter
def run_local(bucket: str, country: str):
    flow = create_flow()
    flow.run(parameters={"bucket": bucket, "country": country})


@main.command()
@bucket_option
@country_filter
def run_with_cloud(bucket: str, country: str):
    flow = create_flow()
    flow_id = flow.register(project_name=PROJECT_NAME)
    client = Client()
    flow_run_id = client.create_flow_run(
        flow_id=flow_id, parameters={"bucket": bucket, "country": country}
    )
    print(f"Flow Run id: {flow_run_id}")
    flow.run_agent(show_flow_logs=True)


@main.command()
def register_flow():
    flow = create_flow()
    flow.register(project_name=PROJECT_NAME)


@main.command()
def start_agent():
    flow = create_flow()
    flow.run_agent(show_flow_logs=True)


if __name__ == "__main__":
    main()
