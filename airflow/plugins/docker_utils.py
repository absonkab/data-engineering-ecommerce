import docker

from airflow.exceptions import AirflowException


def execute_in_container(
    container_name: str,
    command: list[str],
    step_name: str,
    workdir: str | None = None,
):
    """
    Execute a command inside an existing Docker container.

    Parameters
    ----------
    container_name:
        Name of the target container.

    command:
        Command to execute.

    step_name:
        Friendly name displayed in Airflow logs.

    workdir:
        Working directory inside the container.
    """

    client = docker.from_env()

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status != "running":
            raise AirflowException(
                f"Container '{container_name}' is not running. "
                f"Current status: {container.status}"
            )

        print(f"Starting step: {step_name}")
        print(f"Container: {container_name}")
        print(f"Executing Command: {' '.join(command)}")

        result = container.exec_run(
            cmd=command,
            workdir=workdir,
            stdout=True,
            stderr=True,
            demux=True,
        )

        stdout, stderr = result.output or (None, None)

        if stdout:
            print(stdout.decode("utf-8", errors="replace"))

        if stderr:
            print(stderr.decode("utf-8", errors="replace"))

        if result.exit_code != 0:
            raise AirflowException(
                f"Step '{step_name}' failed "
                f"(exit code: {result.exit_code})"
            )

        print(f"Step '{step_name}' completed successfully.")

    finally:
        client.close()