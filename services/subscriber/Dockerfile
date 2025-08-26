FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/subscriber/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/subscriber /app/services/subscriber

EXPOSE 8001

CMD ["uvicorn", "services.subscriber.main:app", "--host", "0.0.0.0", "--port", "8001"]


