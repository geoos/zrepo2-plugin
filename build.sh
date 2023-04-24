VERSION=0.06
docker buildx build --push --platform linux/amd64,linux/arm64 -t docker.homejota.net/geoos/zrepo2-plugin:latest -t docker.homejota.net/geoos/zrepo2-plugin:$VERSION .
