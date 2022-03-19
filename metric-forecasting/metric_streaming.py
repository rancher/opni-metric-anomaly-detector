# Standard Library
import asyncio
import logging
import os
import time
from datetime import datetime

# Third Party
import requests
from fastapi import FastAPI
from metric_anomaly_detector import MetricAnomalyDetector
from prometheusConnector import PrometheusConnect, list_clusters

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
LOOP_TIME_SECOND = 60.0  # unit: second, type: float
ROLLING_TRAINING_SIZE = 1440


metrics_list = [
    "cpu_usage",
    "memory_usage",
    "disk_usage",
]  ## TODO: default metrics and their queries should be configured in a file.

GAUGE_DICT = dict()
MAD_DICT = dict()
PROMETHEUS_CUSTOM_QUERIES = {
    "cpu_usage": '100 * (1- (avg(irate(node_cpu_seconds_total{mode="idle"}[5m]))))',
    "memory_usage": "100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))",
    "disk_usage": '(sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"})- sum(node_filesystem_free_bytes{device!~"rootfs|HarddiskVolume.+"})) / sum(node_filesystem_size_bytes{device!~"rootfs|HarddiskVolume.+"}) * 100 ',
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


def push_metrics(json_payload):

    payload = {
        "clusterID": json_payload["cluster_id"],
        "timeseries": [
            {
                "labels": [
                    {
                        "name": "is_anomaly",
                        "value": str(json_payload["is_alert"]),
                    },
                    {
                        "name": "is_alert",
                        "value": str(json_payload["is_alert"]),
                    },
                ],
                "samples": [
                    {
                        "value": json_payload["yhat"],
                        "timestampMs": int(
                            time.mktime(json_payload["timestamp"].timetuple()) * 1000
                        ),
                    }
                ],
                "exemplars": [],
            }
        ],
        "metadata": [
            {
                "type": "2",
                "metricFamilyName": "predicted_yhat_" + json_payload["metric_name"],
                "help": "Predicted yhat value for" + json_payload["metric_name"],
                "unit": "percentage",
            }
        ],
    }
    response = requests.post(
        "http://opni-monitoring.opni-monitoring.svc:11080/CortexAdmin/write_metrics",
        headers=None,
        params=None,
        json=payload,
    )
    logger.info(
        f"push metrics for cluster_id : {json_payload['cluster_id']} at time {json_payload['timestamp']}"
    )


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
