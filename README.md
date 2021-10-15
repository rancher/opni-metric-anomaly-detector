# Opni Metrics Service

## Installation
#### Prerequisites:
Kubernetes cluster running with Prometheus and Grafana and Opni installed.
Rancher user can install Prometheus and Grafana by [enable Rancher monitoring](https://rancher.com/docs/rancher/v2.5/en/monitoring-alerting/guides/enable-monitoring/).


#### Build and Run
To build the Docker image and then push the image:
```
docker build -t opni-metrics-service ./
docker tag opni-metrics-service [ACCOUNT_NAME]/opni-metrics-service
docker push [ACCOUNT_NAME]/opni-metrics-service

```

then run
```
kubectl apply -f metric_anomaly_detector.yaml
```
Make sure to update the metric_anomaly_detector.yaml file to point to the correct image path!

#### Import Grafana Dashboard
1. Navigate to Grafana and log in. For a Rancher Monitoring user, the default username/password is `admin/prom-operator`. Otherwise, it is likely to be `admin/admin`.
2. [Add Elasticsearch as a Data Source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/#add-a-data-source), and fill in these fields with following values:
```
URL: https://opni-es-client.opni.svc:9200
Basic Auth: enable
Skip TLS Verify: enable
Basic Auth Details:
    User: admin
    Password: <opni-es-password>
Index name: mymetrics
Time field name: timestamp
Version: 7.0+
```
Then click `Save & Test`. The expected response should be `Index OK. Time field name OK.`
3. [Import dashboard](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard) and upload the json file `grafana-dashboard.json` in this repo.

A dashboard named `MetricAnomaly` should now be available.


## Contributing
We use `pre-commit` for formatting auto-linting and checking import. Please refer to [installation](https://pre-commit.com/#installation) to install the pre-commit or run `pip install pre-commit`. Then you can activate it for this repo. Once it's activated, it will lint and format the code when you make a git commit. It makes changes in place. If the code is modified during the reformatting, it needs to be staged manually.

```
# Install
pip install pre-commit

# Install the git commit hook to invoke automatically every time you do "git commit"
pre-commit install

# (Optional)Manually run against all files
pre-commit run --all-files
```
