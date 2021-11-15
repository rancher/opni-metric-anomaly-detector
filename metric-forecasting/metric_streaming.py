# Standard Library
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime

# Third Party
import uvicorn
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from metric_anomaly_detector import MetricAnomalyDetector
from prometheus_api_client import PrometheusConnect
from fastapi import FastAPI, Response
from prometheus_client import Gauge, generate_latest, REGISTRY

app = FastAPI()

PROMETHEUS_ENDPOINT = os.getenv("PROMETHEUS_ENDPOINT", "http://localhost:9090")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)

prom = PrometheusConnect(url=PROMETHEUS_ENDPOINT, disable_ssl=True)
LOOP_TIME_SECOND = 60.0  # unit: second, type: float
ROLLING_TRAINING_SIZE = 1440

ES_ENDPOINT = os.getenv("ES_ENDPOINT", "https://localhost:9200")
ES_USERNAME = os.getenv("ES_USERNAME", "admin")
ES_PASSWORD = os.getenv("ES_PASSWORD", "admin")

ES_RESERVED_KEYWORDS = {
    "_id",
    "_index",
    "_if_seq_no",
    "_if_primary_term",
    "_parent",
    "_percolate",
    "_retry_on_conflict",
    "_routing",
    "_timestamp",
    "_type",
    "_version",
    "_version_type",
}

es = AsyncElasticsearch(
    [ES_ENDPOINT],
    port=9200,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    http_compress=True,
    verify_certs=False,
    use_ssl=True,
    timeout=10,
    max_retries=5,
    retry_on_timeout=True,
)

metrics_list = [
    "cpu_usage",
    "memory_usage",
    "disk_usage",
]  ## TODO: default metrics and their queries should be configured in a file.

GAUGE_DICT = dict()
MAD_DICT = dict()
PROMETHEUS_CUSTOM_QUERIES = {
    'cpu_usage': '100 * (1- (avg(irate(node_cpu_seconds_total{mode="idle"}[5m]))))',
    'memory_usage': '100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))',
    'disk_usage': '(sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"})- sum(node_filesystem_free_bytes{device!~"rootfs|HarddiskVolume.+"})) / sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"}) * 100 ',
}
COLUMNS_LIST = [
    'is_anomaly',
    'alert_score',
    'alert_len',
    'alert_id'
    'is_alert',
    'y',
    'yhat',
    'yhat_lower',
    'yhat_upper',
]

@app.on_event("startup")
async def startup_event():
    logger.info("loading history on webserver startup")
    for metric_name in PROMETHEUS_CUSTOM_QUERIES.keys():
        MAD_DICT[metric_name] = MetricAnomalyDetector(metric_name)
        GAUGE_DICT[metric_name] = Gauge(metric_name, metric_name, ['value_type'])
        await load_history_data(metric_name, MAD_DICT[metric_name])

@app.get("/")
@app.get("/metrics")
async def get_metrics():
    metrics_payloads = []
    for metric_name, query in PROMETHEUS_CUSTOM_QUERIES.items():
        current_data = prom.custom_query(query=query)
        prediction = MAD_DICT[metric_name].run(current_data)
        if prediction:
            metrics_payloads.append(prediction)
            for column in COLUMNS_LIST:
                if prediction[column] is not None:
                    GAUGE_DICT[metric_name].labels(value_type=column).set(prediction[column])
                else:
                    logging.info(f"no data for {column} in {metric_name} prediction")
        else:
            logging.warning(f"no prediction data for {metric_name}")
     # send data to ES
    try:
        async for ok, result in async_streaming_bulk(
            es, doc_generator(metrics_payloads)
        ):
            action, result = result.popitem()
            if not ok:
                logging.error("failed to {} document {}".format())
    except (BulkIndexError, ConnectionTimeout) as exception:
        logging.error("Failed to index data")
        logging.error(exception)
    return Response(content=generate_latest(REGISTRY).decode('utf-8'), media_type='text; charset=utf-8')


async def doc_generator(metrics_payloads):
    for mp in metrics_payloads:
        yield {
            "_index": "mymetrics",
            "_id": uuid.uuid4(),
            "_source": {
                k: mp[k]
                for k in mp
                if not (isinstance(mp[k], str) and not mp[k])
                and k not in ES_RESERVED_KEYWORDS
            },
        }


async def load_history_data(metric_name, mad: MetricAnomalyDetector):
    query = {
        "query": {"match": {"metric_name": metric_name}},
        "sort": [{"timestamp": {"order": "desc"}}],
    }
    try:
        history_data = await es.search(
            index="mymetrics", body=query, size=ROLLING_TRAINING_SIZE
        )
        mad.load(reversed(history_data["hits"]["hits"]))
    except Exception as e:
        logger.warning("fail to load history metrics data!")

def convert_time(ts):
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    uvicorn.run(app, port=8000, host='0.0.0.0')
