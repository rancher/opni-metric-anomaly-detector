# metric-anomaly-poc

## Installation
Prerequisites:
Kubernetes cluster running with Prometheus and Grafana (setup Rancher monitoring) and have Opni installed.

To build the metric-anomaly-detection Docker image and then push the image:
```
docker build -t opni-metric-forecasting-service ./
docker tag opni-metric-forecasting-service-service [ACCOUNT_NAME]/opni-metric-forecasting-service
docker push [ACCOUNT_NAME]/opni-metric-forecasting-service

```

then run
```
kubectl apply -f metric_forecasting.yaml
```
Make sure to update the metric_forecasting.yaml file to point to the correct image path!

## Development (To be updated)
How to run the development version of opni in your cluster:
- Clone rancher/opni
- Switch to the branch `nats-metrics-listener`
- Download Tilt (https://tilt.dev)
- If you need a cluster, use the script `hack/create-k3d-cluster.sh`. Otherwise, if you are using an existing cluster, you must perform the following additional steps:
  - Create a publically available repo in your docker hub account called "opni-manager"
  - Insert the following lines into the opni Tiltfile:
    - Above the call to `docker_build_with_restart`: `default_registry('docker.io/your-user-name')`
    - Near the top of the file under the existing call to allow_k8s_contexts: `allow_k8s_contexts('your-context-name')`
- Run `tilt up`
