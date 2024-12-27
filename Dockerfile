FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY cleaning/ cleaning/
COPY ingestion/ ingestion/
COPY metrics/ metrics/
COPY README.md README.md

CMD ["bash"]
