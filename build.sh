IMAGE_NAME=tybalex/opni-metric:dev1
docker build . -t $IMAGE_NAME -f ./Dockerfile

docker push $IMAGE_NAME
