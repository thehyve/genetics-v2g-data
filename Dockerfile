FROM python:3.10.13-slim-bookworm

RUN apt update && \
    apt install openjdk-17-jdk --yes

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'

RUN pip install pyspark pandas && \
    mkdir input/ output/

COPY process_QTL_datasets_from_sumstats.w_hack.py process_qtl.py
