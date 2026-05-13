import dagster as dg


@dg.sensor(job_name="all_assets_job")
def simple_sensor(context):

    # always triggers run (for learning)
    should_run = True

    if should_run:

        return dg.SensorResult(
            run_requests=[
                dg.RunRequest()
            ]
        )

    return dg.SensorResult()