FROM python:latest
MAINTAINER Jonathan Beezley

ADD . /service
RUN pip install /service
ENTRYPOINT ["checksum-service"]

