FROM python:3.11-slim
RUN pip install --no-cache-dir prometheus-client==0.20.0 requests==2.32.3
COPY exporter.py /exporter.py
CMD ["python", "/exporter.py"]
