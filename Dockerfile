FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 同一镜像，三种角色由 ROLE 决定：coordinator / site-a / site-b
ENV ROLE=coordinator \
    PORT=8000 \
    DATA_DIR=/data

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

CMD ["python", "-m", "app.main"]
