FROM python:3.11-slim
WORKDIR /app
RUN pip install fastapi uvicorn redis kafka-python joblib lightgbm numpy
COPY . .
CMD ["python", "main.py"]
