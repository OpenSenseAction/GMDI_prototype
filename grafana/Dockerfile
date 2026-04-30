FROM python:3.11-slim
RUN pip install --no-cache-dir requests
COPY init_grafana.py /init_grafana.py
CMD ["python", "/init_grafana.py"]
