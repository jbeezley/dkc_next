FROM python:latest
MAINTAINER Jonathan Beezley

ADD . /
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "checksum_service.py"]

