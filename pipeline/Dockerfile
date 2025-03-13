FROM node:23-alpine

WORKDIR /opt/pipeline

COPY package.json .
RUN npm install

COPY src .