# Sử dụng image Python cơ bản
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    requests \
    pandas \
    confluent-kafka \
    vnstock \
    beautifulsoup4

# Copy file Python của bạn vào container
COPY ./crawler.py /app/crawler.py

# Chạy file Python khi container được khởi động
CMD ["bash", "-c", "python /app/crawler.py && tail -f /dev/null"]
