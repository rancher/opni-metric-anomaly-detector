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
    'is_alert',
    'y',
    'yhat',
    'yhat_lower',
    'yhat_upper',
]

@app.on_event("startup")
async def startup_event():
    for metric_name in PROMETHEUS_CUSTOM_QUERIES:
        MAD_DICT[metric_name] = MetricAnomalyDetector(metric_name)
        GAUGE_DICT[metric_name] = Gauge(metric_name, metric_name, ['value_type'])
        await load_history_data(metric_name, MAD_DICT[metric_name])

@app.get("/")
@app.get("/metrics")
async def get_metrics():
    for metric_name, query in PROMETHEUS_CUSTOM_QUERIES:
        current_data = prom.custom_query(query=query)
        prediction = MAD_DICT[metric_name].run(current_data)
        for column in COLUMNS_LIST:
            GAUGE_DICT[metric_name].labels(value_type=column).set(prediction[column])
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


async def update_metrics(inference_queue):
    mad_dict = {}
    for metric_name in metrics_list:  # init models
        mad = MetricAnomalyDetector(metric_name)
        await load_history_data(metric_name, mad)
        mad_dict[metric_name] = mad
    while True:
        new_data = await inference_queue.get()
        starttime = time.time()
        metrics_payloads = []
        for metric_name in metrics_list:
            if len(new_data[metric_name]) == 0:
                continue
            json_payload = mad_dict[metric_name].run(new_data[metric_name])  # run MAD
            if json_payload:
                metrics_payloads.append(json_payload)

        # send data to ES
        try:
            async for ok, result in async_streaming_bulk(
                es, doc_generator(metrics_payloads)
            ):
                action, result = result.popitem()
                if not ok:
                    logger.error("failed to {} document {}".format())
        except (BulkIndexError, ConnectionTimeout) as exception:
            logger.error("Failed to index data")
            logger.error(exception)


def convert_time(ts):
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


async def scrape_prometheus_metrics(inference_queue):
    starttime = time.time()
    wait_time = (
        int(starttime) // LOOP_TIME_SECOND * LOOP_TIME_SECOND
        + LOOP_TIME_SECOND
        - starttime
    )
    await asyncio.sleep(wait_time)  # wait to the start of next minute
    starttime = time.time()
    logger.debug(f"wait time : {wait_time}, current time : {convert_time(starttime)}")
    while True:
        thistime = time.time()
        # metrics to collect.
        cpu_usage = prom.custom_query(
            query='100 * sum(irate({__name__=~"node_cpu_seconds_total|windows_cpu_time_total",mode!="idle"}[5m])) / (sum(irate({__name__=~"node_cpu_seconds_total|windows_cpu_time_total",mode!="idle"}[5m])) + sum(irate({__name__=~"node_cpu_seconds_total|windows_cpu_time_total",mode="idle"}[5m])))'
        )
        memory_usage = prom.custom_query(
            query="100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))"
        )
        disk_usage = prom.custom_query(
            query='(sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"})- sum(node_filesystem_free_bytes{device!~"rootfs|HarddiskVolume.+"})) / sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"}) * 100 '
        )
        inference_queue_payload = {
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
            "disk_usage": disk_usage,
        }
        await inference_queue.put(inference_queue_payload)
        await asyncio.sleep(  # to make sure it scrapes every LOOP_TIME seconds.
            LOOP_TIME_SECOND - ((time.time() - starttime) % LOOP_TIME_SECOND)
        )

async def run_metrics_server():
    uvicorn.run(app, port=8000, host='0.0.0.0')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    inference_queue = asyncio.Queue(loop=loop)
    prometheus_scraper_coroutine = scrape_prometheus_metrics(inference_queue)
    update_metrics_coroutine = update_metrics(inference_queue)
    uvicorn_server_coroutine = run_metrics_server()

    loop.run_until_complete(
        asyncio.gather(prometheus_scraper_coroutine, update_metrics_coroutine, uvicorn_server_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
