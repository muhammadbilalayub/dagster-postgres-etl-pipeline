from dagster import Definitions

from .assets import (
    stg_customers,
    stg_orders,
    stg_payments,
    customer_analytics
)

from .resources import PostgresResource

from .schedules import (
    daily_etl_job,
    daily_etl_schedule
)


defs = Definitions(

    assets=[
        stg_customers,
        stg_orders,
        stg_payments,
        customer_analytics
    ],

    jobs=[
        daily_etl_job
    ],

    schedules=[
        daily_etl_schedule
    ],

    resources={
        "postgres": PostgresResource()
    }
)