# hash:sha256:8aa5bf944582400c59b1a69db7fe71c6e48c0dab37a9e4de5573a5c51c7d828c
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-ask-pass /

RUN pip3 install -U --no-cache-dir \
    aind-codeocean-pipeline-monitor==0.5.2 \
    aind-session \
    codeocean \
    pandas==2.2.3 \
    panel==1.5.4 \
    streamlit==1.40.2
