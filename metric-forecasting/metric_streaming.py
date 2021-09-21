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
from metric_anomaly_detector import MetricAnomalyDetector
from prometheus_api_client import PrometheusConnect

PROMETHEUS_ENDPOINT = os.getenv("PROMETHEUS_ENDPOINT", "http://localhost:9090")
# IS_LOCAL = True if PROMETHEUS_ENDPOINT == "http://localhost:9090" else False

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)

prom = PrometheusConnect(url=PROMETHEUS_ENDPOINT, disable_ssl=True)
LOOP_TIME_SECOND = float(60.0)  # unit: second, type: float

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
    use_ssl=False,
    timeout=10,
    max_retries=5,
    retry_on_timeout=True,
)


async def doc_generator(metrics_payloads):
    # for index, document in df.iterrows():
    #     doc_kv = document[pd.notnull(document)].to_dict().items()
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
        history_data = await es.search(index="mymetrics", body=query, size=1440)
        mad.load(reversed(history_data["hits"]["hits"]))
    except Exception as e:
        logger.warning("fail to load history metrics data!")


async def update_metrics(inference_queue):
    # if not IS_LOCAL:
    #     nw = NatsWrapper()
    #     await nw.connect()
    # ## orderedDict?
    metrics_list = [
        "cpu_usage",
        "memory_usage",
        "disk_usage",
    ]  ## TODO: default metrics and their queries should be configured in a file.
    mad_dict = {}
    for metric_name in metrics_list:
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
            json_payload = mad_dict[metric_name].run(new_data[metric_name])
            metrics_payloads.append(json_payload)
            # if not IS_LOCAL:
            # await nw.publish(nats_subject="forecasted_metric_bounds", payload_df=json.dumps(json_payload).encode()) ## plan to change the subjact to "forecasted_metrics"

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

        # logger.debug(f"anomaly detection time spent : {time.time() - starttime}")


def convert_time(ts):
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


async def scrape_prometheus_metrics(inference_queue):
    starttime = time.time()
    wait_time = int(starttime) // 60 * 60 + 60 - starttime
    await asyncio.sleep(wait_time)
    starttime = time.time()
    logger.debug(f"wait time : {wait_time}, current time : {convert_time(starttime)}")
    while True:
        thistime = time.time()
        ## TODO: should have a preprocessing service to scrape metrics from prometheus every minute, and fillin missing values if necessarys
        current_cpu_usage_data = prom.custom_query(
            query='100 * (1- (avg(irate(node_cpu_seconds_total{mode="idle"}[2m]))))'
        )
        memory_usage = prom.custom_query(
            query="100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))"
        )
        disk_usage = prom.custom_query(
            query='(sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"})- sum(node_filesystem_free_bytes{device!~"rootfs|HarddiskVolume.+"})) / sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"}) * 100 '
        )
        inference_queue_payload = {
            "cpu_usage": current_cpu_usage_data,
            "memory_usage": memory_usage,
            "disk_usage": disk_usage,
        }
        await inference_queue.put(inference_queue_payload)
        # logger.debug(f"query time spent: {time.time() - thistime}")
        await asyncio.sleep(
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
