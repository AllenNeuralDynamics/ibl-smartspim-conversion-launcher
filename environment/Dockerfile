# hash:sha256:5316b44bfc85bf8040be86c01aca82b9dbedeef7814f5a67323e87ccc49b9034
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-ask-pass /

RUN pip3 install -U --no-cache-dir \
    aind-codeocean-pipeline-monitor==0.5.2 \
    aind-session==0.3.22 \
    codeocean==0.12.0 \
    pandas==2.2.3 \
    panel==1.5.4 \
    streamlit==1.40.2
