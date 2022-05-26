# Standard Library
import logging
import os
import time
from urllib.parse import urlparse

# Third Party
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel(LOGGING_LEVEL)

########
# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 3
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]

OPNI_GATEWAY_MANAGEMENT_ENDPOINT = os.getenv("OPNI_GATEWAY_MANAGEMENT_API", "http://opni-monitoring-internal:11080")

def push_metrics(json_payload):

    for m in ["y", "yhat", "yhat_lower", "yhat_upper"]:
        payload = {
            "clusterID": json_payload["cluster_id"],
            "timeseries": [
                {
                    "labels": [
                        {
                            "name": "__name__",
                            "value": "new_" + m + "_" + json_payload["metric_name"],
                        },
                        # {
                        #     "name": "is_anomaly",
                        #     "value": str(json_payload["is_anomaly"]),
                        # },
                        # {
                        #     "name": "is_alert",
                        #     "value": str(json_payload["is_alert"]),
                        # },
                    ],
                    "samples": [
                        {
                            "value": json_payload[m],
                            "timestampMs": int(
                                time.mktime(json_payload["timestamp"].timetuple())
                                * 1000
                            ),
                        }
                    ],
                    "exemplars": [],
                }
            ],
            "metadata": [
                {
                    "type": "2",
                    "metricFamilyName": "new_" + m + "_" + json_payload["metric_name"],
                    "help": "Predicted "
                    + m
                    + " value for"
                    + json_payload["metric_name"],
                    "unit": "percentage",
                }
            ],
        }
        response = requests.post(
            OPNI_GATEWAY_MANAGEMENT_ENDPOINT + "/CortexAdmin/write_metrics",
            headers=None,
            params=None,
            json=payload,
        )
    logger.info(f"status : {response.status_code}")
    logger.info(
        f"push metrics for cluster_id : {json_payload['cluster_id']} at time {json_payload['timestamp']}, metric: {json_payload['metric_name']}"
    )
    logger.info(
        f"y: {json_payload['y']}, yhat_lower: {json_payload['yhat_lower']}, yhat_upper: {json_payload['yhat_upper']}"
    )


def list_clusters():
    """
    Check Promethus connection.
    :param params: (dict) Optional dictionary containing parameters to be
        sent along with the API request.
    :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
    """
    response = requests.get(
        OPNI_GATEWAY_MANAGEMENT_ENDPOINT + "/management/clusters",
        headers=None,
        params=None,
    )
    if response.ok:
        return [r["id"] for r in response.json()["items"]]
    else:
        return []


class PrometheusConnect:
    """
    A Class for collection of metrics from a Prometheus Host.
    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    :param disable_ssl: (bool) If set to True, will disable ssl certificate verification
        for the http requests made to the prometheus host
    :param retry: (Retry) Retry adapter to retry on HTTP errors
    """

    def __init__(
        self,
        url: str = "http://127.0.0.1:9090",
        headers: dict = None,
        disable_ssl: bool = False,
        retry: Retry = None,
        verify=None,
        cert=None,
    ):
        """Functions as a Constructor for the class PrometheusConnect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None
        self.ssl_verification = not disable_ssl
        self.verify = verify
        self.cert = cert
        logger.info(self.verify)
        logger.info(self.cert)

        if retry is None:
            retry = Retry(
                total=MAX_REQUEST_RETRIES,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=RETRY_ON_STATUS,
            )

        self._session = requests.Session()
        self._session.mount(self.url, HTTPAdapter(max_retries=retry))

    def check_prometheus_connection(self, params: dict = None):
        """
        Check Promethus connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._session.get(
            f"{self.url}/",
            verify=self.verify,
            cert=self.cert,
            headers=self.headers,
            params=params,
        )
        return response.ok

    def list_metrics(self):
        """
        Check Promethus connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._session.get(
            f"{self.url}/metrics",
            verify=self.verify,
            cert=self.cert,
            headers=None,
            params=None,
        )
        return response.text

    def custom_query(self, query: str, cluster_id: str, params: dict = None):
        """
        Send a custom query to a Prometheus Host.
        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.
        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = None
        query = str(query)
        # using the query API to get raw data
        response = self._session.get(
            f"{self.url}/prometheus/api/v1/query",
            params={**{"query": query}, **params},
            verify=self.verify,
            cert=self.cert,
            headers={"X-Scope-OrgID": cluster_id},
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise Exception(
                "HTTP Status Code {} ({!r})".format(
                    response.status_code, response.content
                )
            )

        return data
