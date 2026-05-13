FROM python:3.11

WORKDIR /app

COPY . /app

# 👇 IMPORTANT LINE ADD
COPY data /app/data

RUN pip install --upgrade pip
RUN pip install dagster dagster-webserver pandas psycopg2-binary python-dotenv

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]