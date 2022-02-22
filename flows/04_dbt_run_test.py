import shutil

import prefect
import pygit2
from prefect import task, Flow, Parameter
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.triggers import always_run


DBT_PROJECT = "dwh"


dbt = DbtShellTask(
    return_all=True,
    profile_name=DBT_PROJECT,
    profiles_dir="/Users/anna/.dbt",
    environment="dev",
    overwrite_profiles=False,
    log_stdout=True,
    log_stderr=True,
    helper_script=f"cd {DBT_PROJECT}",
)


@task(name="Clone DBT repo")
def pull_dbt_repo(repo_url: str, branch: str = None):
    pygit2.clone_repository(url=repo_url, path=DBT_PROJECT, checkout_branch=branch)


@task(name="Delete DBT folder if exists", trigger=always_run)
def delete_dbt_folder_if_exists():
    shutil.rmtree(DBT_PROJECT, ignore_errors=True)


@task(trigger=always_run)
def print_dbt_output(output):
    logger = prefect.context.get("logger")
    for line in output:
        logger.info(line)


with Flow("dbt") as flow:
    del_task = delete_dbt_folder_if_exists()
    dbt_repo_branch = Parameter("dbt_repo_branch", default=None)
    dbt_repo = Parameter(
        "dbt_repo_url", default="https://github.com/anna-geller/jaffle_shop"
    )
    pull_task = pull_dbt_repo(dbt_repo, dbt_repo_branch, upstream_tasks=[del_task])
    dbt_run = dbt(
        command="dbt run", task_args={"name": "DBT Run"}, upstream_tasks=[pull_task]
    )
    dbt_run_out = print_dbt_output(dbt_run, task_args={"name": "DBT Run Output"})
    dbt_test = dbt(
        command="dbt test", task_args={"name": "DBT Test"}, upstream_tasks=[dbt_run]
    )
    dbt_test_out = print_dbt_output(dbt_test, task_args={"name": "DBT Test Output"})
    del_again = delete_dbt_folder_if_exists(upstream_tasks=[dbt_test_out])
    flow.set_reference_tasks([dbt_run])


if __name__ == "__main__":
    flow.run()
