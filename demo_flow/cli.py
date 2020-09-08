# Third party
import click
from prefect.client.client import Client

# First party
from demo_flow.flow import create_flow


@click.group()
def main():
    pass


@main.command()
def visualize():
    flow = create_flow()
    flow.visualize()


@main.command()
def run_local():
    flow = create_flow()
    flow.run()


# TODO register command
# TODO agent listen command


@main.command()
def run_with_cloud():
    flow = create_flow()
    flow_id = flow.register(project_name="Demo")
    client = Client()
    # TODO pass params
    flow_run_id = client.create_flow_run(flow_id=flow_id)
    print(f"Flow Run id: {flow_run_id}")
    flow.run_agent(show_flow_logs=True)
