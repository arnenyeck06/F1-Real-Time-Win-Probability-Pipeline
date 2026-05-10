FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python requests python-dotenv
COPY main.py .
CMD ["python", "main.py"]
