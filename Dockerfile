FROM python:3.9-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apk add tzdata && \
    apk add gcc musl-dev libffi-dev && \
    apk add rust libxml2-dev libxslt-dev cargo openssl-dev && \
    pip3 install --no-cache-dir  -r requirements.txt && \
    apk del gcc cargo rust proj && \
    rm -rf /root/.cargo/

ENV API_URL ""
ENV API_KEY ""
ENV API_SECRET ""

ENV FROST_SERVER ""
ENV FROST_USER ""
ENV FROST_PASSWORD ""

ENV TZ "Europe/Berlin"

COPY src/ ./

CMD [ "python", "-u", "import_ecocounter.py"]
