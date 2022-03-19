IMAGE_NAME=tybalex/opni-metrics-service:dev22
docker build . -t $IMAGE_NAME -f ./Dockerfile

docker push $IMAGE_NAME
