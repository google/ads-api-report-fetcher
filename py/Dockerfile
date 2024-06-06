FROM python:3.10-alpine
ENV PYTHONDONTWRITEBYTECODE=1
ADD requirements.txt .
RUN pip install --require-hashes -r requirements.txt --no-deps --no-cache-dir
ENTRYPOINT ["gaarf"]
