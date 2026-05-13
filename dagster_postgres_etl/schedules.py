import dagster as dg

from .assets import (
    stg_customers,
    stg_orders,
    stg_payments,
    customer_analytics
)


daily_etl_job = dg.define_asset_job(
    name="daily_etl_job",
    selection=[
        stg_customers,
        stg_orders,
        stg_payments,
        customer_analytics
    ]
)


daily_etl_schedule = dg.ScheduleDefinition(
    job=daily_etl_job,
    cron_schedule="0 9 * * *"
)