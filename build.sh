IMAGE_NAME=tybalex/opni-metrics-service:dev
docker build . -t $IMAGE_NAME -f ./Dockerfile

docker push $IMAGE_NAME
