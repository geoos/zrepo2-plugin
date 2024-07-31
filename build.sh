VERSION=0.21
docker buildx build --push --platform linux/amd64 -t docker.homejota.net/geoos/zrepo2-plugin:latest -t docker.homejota.net/geoos/zrepo2-plugin:$VERSION .
