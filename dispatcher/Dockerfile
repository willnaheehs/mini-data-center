FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dispatcher.py .

CMD ["python", "-u", "dispatcher.py"]
