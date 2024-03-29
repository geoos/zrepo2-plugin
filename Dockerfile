FROM docker.homejota.net/geoos/gdal-node16-nco-cdo:0.25
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --production

COPY . .
CMD ["node", "index"]