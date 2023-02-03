FROM python:3.8-slim
WORKDIR /app
ADD requirements.txt .
RUN pip install --require-hashes -r requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS /app/service_account.json
ENTRYPOINT ["gaarf"]
