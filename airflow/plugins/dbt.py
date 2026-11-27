from docker_utils import execute_in_container


def run_dbt_models(
    container_name: str,
    project_dir: str,
    target: str | None = None,
):
    """
    Build all dbt models in Snowflake.
    """
    command = ["dbt", "run"]

    if target:
        command.extend(["--target", target])

    execute_in_container(
        container_name=container_name,
        command=command,
        step_name="dbt run",
        workdir=project_dir,
    )

def test_dbt_models(
    container_name: str,
    project_dir: str,
    target: str | None = None,
):
    """
    Execute all dbt data quality tests.
    """
    command = ["dbt", "test"]

    if target:
        command.extend(["--target", target])

    execute_in_container(
        container_name=container_name,
        command=command,
        step_name="dbt test",
        workdir=project_dir,
    )

def generate_dbt_docs(
    container_name: str,
    project_dir: str,
    target: str | None = None,
):
    """
    Generate dbt documentation artifacts.
    """
    command = [
        "dbt",
        "docs",
        "generate",
    ]

    if target:
        command.extend(["--target", target])

    execute_in_container(
        container_name=container_name,
        command=command,
        step_name="dbt docs generate",
        workdir=project_dir,
    )