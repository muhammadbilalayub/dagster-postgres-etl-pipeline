import pandas as pd
import dagster as dg


@dg.asset(required_resource_keys={"postgres"})
def stg_customers(context):

    postgres = context.resources.postgres

    conn = postgres.get_connection()
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_customers (
            customer_id INT,
            customer_name TEXT,
            city TEXT
        )
    """)

    # Incremental fix
    cur.execute("TRUNCATE TABLE stg_customers")

    # Read CSV
    df = pd.read_csv("data/customers.csv")

    # Clean column names
    df.columns = df.columns.str.strip().str.lower()

    # Insert data
    for _, row in df.iterrows():

        cur.execute(
            """
            INSERT INTO stg_customers (
                customer_id,
                customer_name,
                city
            )
            VALUES (%s, %s, %s)
            """,
            (
                int(row["customer_id"]),
                row["customer_name"],
                row["city"]
            )
        )

    conn.commit()

    cur.close()
    conn.close()

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": len(df)
        }
    )


@dg.asset(required_resource_keys={"postgres"})
def stg_orders(context):

    postgres = context.resources.postgres

    conn = postgres.get_connection()
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id INT,
            customer_id INT,
            amount FLOAT
        )
    """)

    # Incremental fix
    cur.execute("TRUNCATE TABLE stg_orders")

    # Read CSV
    df = pd.read_csv("data/orders.csv")

    # Clean column names
    df.columns = df.columns.str.strip().str.lower()

    # Insert data
    for _, row in df.iterrows():

        cur.execute(
            """
            INSERT INTO stg_orders (
                order_id,
                customer_id,
                amount
            )
            VALUES (%s, %s, %s)
            """,
            (
                int(row["order_id"]),
                int(row["customer_id"]),
                float(row["amount"])
            )
        )

    conn.commit()

    cur.close()
    conn.close()

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": len(df)
        }
    )


@dg.asset(required_resource_keys={"postgres"})
def stg_payments(context):

    postgres = context.resources.postgres

    conn = postgres.get_connection()
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_payments (
            payment_id INT,
            customer_id INT,
            payment_amount FLOAT
        )
    """)

    # Incremental fix
    cur.execute("TRUNCATE TABLE stg_payments")

    # Read CSV
    df = pd.read_csv("data/payments.csv")

    # Clean column names
    df.columns = df.columns.str.strip().str.lower()

    # Insert data
    for _, row in df.iterrows():

        cur.execute(
            """
            INSERT INTO stg_payments (
                payment_id,
                customer_id,
                payment_amount
            )
            VALUES (%s, %s, %s)
            """,
            (
                int(row["payment_id"]),
                int(row["customer_id"]),
                float(row["payment_amount"])
            )
        )

    conn.commit()

    cur.close()
    conn.close()

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": len(df)
        }
    )


@dg.asset(
    deps=[
        "stg_customers",
        "stg_orders",
        "stg_payments"
    ],
    required_resource_keys={"postgres"}
)
def customer_analytics(context):

    postgres = context.resources.postgres

    conn = postgres.get_connection()
    cur = conn.cursor()

    # Recreate analytics table
    cur.execute("""
        DROP TABLE IF EXISTS customer_analytics
    """)

    cur.execute("""
        CREATE TABLE customer_analytics AS

        SELECT
            c.customer_id,
            c.customer_name,
            c.city,

            COUNT(o.order_id) AS total_orders,

            COALESCE(SUM(o.amount), 0) AS total_order_amount,

            COALESCE(SUM(p.payment_amount), 0) AS total_payment_amount

        FROM stg_customers c

        LEFT JOIN stg_orders o
            ON c.customer_id = o.customer_id

        LEFT JOIN stg_payments p
            ON c.customer_id = p.customer_id

        GROUP BY
            c.customer_id,
            c.customer_name,
            c.city
    """)

    conn.commit()

    cur.close()
    conn.close()

    return dg.MaterializeResult(
        metadata={
            "status": "customer_analytics created successfully"
        }
    )