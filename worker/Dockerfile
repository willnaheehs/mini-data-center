FROM python:3.11-slim

WORKDIR /app/worker

COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/worker.py ./worker.py
COPY common /app/common

CMD ["python", "-u", "worker.py"]
