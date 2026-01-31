FROM apache/airflow:3.1.6
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt -c https://raw.githubusercontent.com/apache/airflow/constraints-3.1.6/constraints-3.11.txt
