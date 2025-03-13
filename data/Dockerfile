FROM python:alpine

ENV DATA_PATH="/data"
VOLUME [ "/data/downloaded" ]

WORKDIR /app
COPY prepare.py .
CMD [ "./prepare.py" ]