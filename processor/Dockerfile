FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python redis psycopg2-binary
COPY main.py .
CMD ["python", "main.py"]
