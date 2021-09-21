FROM python:3.8-slim

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./metric-forecasting /app/

WORKDIR /app


CMD [ "python", "metric_streaming.py" ]
