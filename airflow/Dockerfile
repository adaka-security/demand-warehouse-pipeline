FROM apache/airflow:2.10.4-python3.11

# pymssql instead of pyodbc on purpose: it installs as a pure pip wheel and
# avoids the Microsoft ODBC driver setup inside the container.
#
# pyiceberg is deliberately NOT installed here. Its sql catalog extra needs
# SQLAlchemy 2.x while Airflow 2.10 needs SQLAlchemy 1.4, and the two cannot
# coexist in one environment. The iceberg DAG uses @task.virtualenv instead,
# which builds an isolated venv for that task at runtime.
RUN pip install --no-cache-dir \
    pymssql==2.3.2 \
    clickhouse-connect==0.8.11 \
    numpy==1.26.4 \
    pandas==2.2.3 \
    virtualenv
