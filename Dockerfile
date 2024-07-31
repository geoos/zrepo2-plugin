FROM docker.homejota.net/geoos/gdal-node20-nco-cdo:0.31
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --production

COPY . .
CMD ["node", "index"]