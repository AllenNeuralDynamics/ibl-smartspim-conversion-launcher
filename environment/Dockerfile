# hash:sha256:42d76c30f1d6bb3e71799d002affe6d8b7e62502c55d476f637f307353d85cf1
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG API_KEY
ARG API_SECRET
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip3 install -U --no-cache-dir \
    aind-codeocean-pipeline-monitor==0.5.2 \
    aind-session==0.3.12 \
    pandas==2.2.3 \
    panel==1.5.4 \
    streamlit==1.40.2
