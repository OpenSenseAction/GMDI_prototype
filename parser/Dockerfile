FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
COPY example_data/openMRG_cmls_20150827_12hours.nc ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]