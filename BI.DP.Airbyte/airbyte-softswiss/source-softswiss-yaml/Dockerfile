FROM python:3.10-alpine as base
# python:3.10-alpine
# python:3.9.13-alpine3.15
# build and load all requirements
FROM base as builder
WORKDIR /airbyte/integration_code

# upgrade pip to the latest version
RUN apk --no-cache upgrade \
    && pip install --upgrade pip \
    && apk --no-cache add tzdata build-base


COPY setup.py ./
# install necessary packages to a temporary folder
RUN pip install --prefix=/install .

# build a clean environment
FROM base
WORKDIR /airbyte/integration_code

# copy all loaded and built libraries to a pure basic image
COPY --from=builder /install /usr/local
# add default timezone settings
COPY --from=builder /usr/share/zoneinfo/Etc/UTC /etc/localtime
RUN echo "Etc/UTC" > /etc/timezone

# bash is installed for more convenient debugging.
RUN apk --no-cache add bash

# copy payload code only
COPY main.py ./
COPY source_softswiss_yaml ./source_softswiss_yaml

COPY common_components/source_customcomponents-0.0.0-py3-none-any.whl ./
RUN pip install source_customcomponents-0.0.0-py3-none-any.whl

ARG APISIX_ENDPOINT
ARG APISIX_ENABLED

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENV APISIX_ENDPOINT=$APISIX_ENDPOINT
ENV APISIX_ENABLED=$APISIX_ENABLED
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-softswiss-yaml
