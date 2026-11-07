FROM python:3.12-slim-bookworm
LABEL org.opencontainers.image.version="docker-051.1"

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/app/data
WORKDIR /app
RUN mkdir -p /app/runtime /app/data \
    && chown 10001:10001 /app/data && chmod 700 /app/data
# Build context is the exported repository, never the project workspace.
COPY run.py /app/runtime/run.py
COPY LICENSE NOTICE /app/runtime/
COPY independent/*.py /app/runtime/independent/
COPY web/*.html web/*.js web/*.css /app/runtime/web/
COPY docker-entrypoint.py /usr/local/bin/mmx-entrypoint.py
EXPOSE 45300
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import json,urllib.request; o=urllib.request.build_opener(urllib.request.ProxyHandler({})); r=o.open('http://127.0.0.1:45300/health',timeout=2); assert json.load(r).get('service')=='independent-x-core'"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "/usr/local/bin/mmx-entrypoint.py"]
