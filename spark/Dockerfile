FROM apache/spark:4.0.0

USER root

WORKDIR /opt/spark-app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY app ./app
COPY config ./config
COPY run.py .
COPY run_silver.py .
COPY run_silver_batch.py .
COPY run_gold.py .
COPY validate_gold.py .
COPY validate_gold_metrics.py .

RUN mkdir -p \
    /opt/spark-app/bronze \
    /opt/spark-app/silver \
    /opt/spark-app/gold \
    /opt/spark-app/quarantine \
    /opt/spark-app/checkpoint \
    /opt/spark-app/checkpoint_silver

ENV PYTHONPATH=/opt/spark-app

CMD ["python3", "run_gold.py"]