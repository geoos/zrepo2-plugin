# docker buildx build --push --platform linux/amd64,linux/arm64 -t docker.homejota.net/geoos/zrepo2-plugin:latest -t docker.homejota.net/geoos/zrepo2-plugin:0.01 .

#FROM docker.homejota.net/geoos/node16-python3
FROM node:16-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --production

COPY . .
CMD ["node", "index"]