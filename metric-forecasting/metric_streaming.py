# Standard Library
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime

# Third Party
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from fastapi import FastAPI
from metric_anomaly_detector import MetricAnomalyDetector
from prometheusConnector import PrometheusConnect, list_clusters, push_metrics

app = FastAPI()

PROMETHEUS_ENDPOINT = os.getenv("PROMETHEUS_ENDPOINT", "http://localhost:9090")

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)


prom = PrometheusConnect(
    url=PROMETHEUS_ENDPOINT,
    cert=("/run/cortex/certs/client/tls.crt", "/run/cortex/certs/client/tls.key"),
    verify="/run/cortex/certs/server/ca.crt",
)
logger.info(f"prom connect status check OK?: {prom.check_prometheus_connection()}")
LOOP_TIME_SECOND = float(
    os.getenv("LOOP_TIME_SECOND", 60.0)
)  # 60.0  # unit: second, type: float
ROLLING_TRAINING_SIZE = 1440

ES_ENDPOINT = os.getenv("ES_ENDPOINT", None)
if ES_ENDPOINT:
    ES_USERNAME = os.getenv("ES_USERNAME", "admin")
    ES_PASSWORD = os.getenv("ES_PASSWORD", "admin")
    ES_INDEX = "opni-metric"

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
        use_ssl=False,
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
    "cpu_usage": 'sum (rate (container_cpu_usage_seconds_total{id="/"}[5m])) / sum (machine_cpu_cores) * 100',
    "memory_usage": 'sum (container_memory_working_set_bytes{id="/"}) / sum (machine_memory_bytes) * 100',
    "disk_usage": 'sum (container_fs_usage_bytes{id="/"}) / sum (container_fs_limit_bytes{id="/"}) * 100',
}
COLUMNS_LIST = [
    "is_anomaly",
    "alert_score",
    "alert_len",
    "alert_id",
    "is_alert",
    "y",
    "yhat",
    "yhat_lower",
    "yhat_upper",
]


async def update_metrics(inference_queue):
    mad_dict = {}

    while True:
        new_data = await inference_queue.get()
        cluster_id = new_data["cluster_id"]
        if cluster_id not in mad_dict:
            mad_dict[cluster_id] = {}
            for metric_name in metrics_list:  # init models
                mad = MetricAnomalyDetector(metric_name, cluster_id)
                # await load_history_data(metric_name, mad)
                mad_dict[cluster_id][metric_name] = mad
        starttime = time.time()
        metrics_payloads = []
        for metric_name in metrics_list:
            if len(new_data[metric_name]) == 0:
                continue
            json_payload = mad_dict[cluster_id][metric_name].run(
                new_data[metric_name]
            )  # run MAD
            if json_payload:
                push_metrics(json_payload)
                metrics_payloads.append(json_payload)

        if ES_ENDPOINT:
            try:
                async for ok, result in async_streaming_bulk(
                    es, doc_generator(metrics_payloads)
                ):
                    action, result = result.popitem()
                    if not ok:
                        logging.error("failed to {} document {}".format())
                logger.info("pushed to ES.")
            except (BulkIndexError, ConnectionTimeout) as exception:
                logging.error("Failed to index data")
                logging.error(exception)


async def doc_generator(metrics_payloads):
    for mp in metrics_payloads:
        yield {
            "_index": ES_INDEX,
            "_id": uuid.uuid4(),
            "_source": {
                k: mp[k]
                for k in mp
                if not (isinstance(mp[k], str) and not mp[k])
                and k not in ES_RESERVED_KEYWORDS
            },
        }


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
    logger.info(f"wait time : {wait_time}, current time : {convert_time(starttime)}")
    while True:
        thistime = time.time()
        active_clusters = list_clusters()
        for cluster_id in active_clusters:
            # metrics to collect.
            cpu_usage = prom.custom_query(
                query=PROMETHEUS_CUSTOM_QUERIES["cpu_usage"],
                cluster_id=cluster_id,
            )
            memory_usage = prom.custom_query(
                query=PROMETHEUS_CUSTOM_QUERIES["memory_usage"],
                cluster_id=cluster_id,
            )
            disk_usage = prom.custom_query(
                query=PROMETHEUS_CUSTOM_QUERIES["disk_usage"],
                cluster_id=cluster_id,
            )
            inference_queue_payload = {
                "cluster_id": cluster_id,
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "disk_usage": disk_usage,
            }
            logger.info(inference_queue_payload)
            await inference_queue.put(inference_queue_payload)
        await asyncio.sleep(  # to make sure it scrapes every LOOP_TIME seconds.
            LOOP_TIME_SECOND - ((time.time() - starttime) % LOOP_TIME_SECOND)
        )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    inference_queue = asyncio.Queue(loop=loop)
    prometheus_scraper_coroutine = scrape_prometheus_metrics(inference_queue)
    update_metrics_coroutine = update_metrics(inference_queue)

    loop.run_until_complete(
        asyncio.gather(prometheus_scraper_coroutine, update_metrics_coroutine)
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
