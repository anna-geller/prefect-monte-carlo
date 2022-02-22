import pendulum
from prefect import Flow
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.executors import LocalDaskExecutor
from prefect.schedules import CronSchedule


PROJECT_NAME = "dwh"
schedule = CronSchedule(
    cron="30 17 * * *",
    start_date=pendulum.datetime(2022, 2, 13, 0, 0, tz="Europe/Berlin"),
)


with Flow("dwh_flow_of_flows", executor=LocalDaskExecutor(), schedule=schedule) as flow:
    orders = create_flow_run(
        flow_name="dwh_raw_orders",
        project_name=PROJECT_NAME,
        task_args={"name": "Raw Orders"},
    )
    orders_flow_run_view = wait_for_flow_run(
        orders,
        stream_logs=True,
        raise_final_state=True,
        task_args={"name": "Orders Flow Run"},
    )

    customers = create_flow_run(
        flow_name="dwh_raw_customers",
        project_name=PROJECT_NAME,
        task_args={"name": "Raw Customers"},
    )
    customers_flow_run_view = wait_for_flow_run(
        customers,
        stream_logs=True,
        raise_final_state=True,
        task_args={"name": "Customers Flow Run"},
    )

    payments = create_flow_run(
        flow_name="dwh_raw_payments",
        project_name=PROJECT_NAME,
        task_args={"name": "Raw Payments"},
        upstream_tasks=[orders_flow_run_view, customers_flow_run_view],
    )
    payments_flow_run_view = wait_for_flow_run(
        payments,
        stream_logs=True,
        raise_final_state=True,
        task_args={"name": "Payments Flow Run"},
    )

    dbt = create_flow_run(
        flow_name="dbt",
        project_name=PROJECT_NAME,
        task_args={"name": "dbt"},
        upstream_tasks=[payments_flow_run_view],
    )
    dbt_flow_run_view = wait_for_flow_run(
        dbt,
        stream_logs=True,
        raise_final_state=True,
        task_args={"name": "dbt Flow Run"},
    )
    sales_forecast = create_flow_run(
        flow_name="sales_forecast",
        project_name=PROJECT_NAME,
        task_args={"name": "Sales Forecast"},
        upstream_tasks=[dbt_flow_run_view],
    )
    sales_forecast_flow_run_view = wait_for_flow_run(
        sales_forecast,
        stream_logs=True,
        raise_final_state=True,
        task_args={"name": "Sales Forecast Flow Run"},
    )

if __name__ == "__main__":
    flow.visualize()
