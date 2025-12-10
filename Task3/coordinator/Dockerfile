FROM python:3.10

WORKDIR /app

RUN pip install fastapi uvicorn httpx ddtrace
COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY ./app/ .
CMD ["ddtrace-run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]