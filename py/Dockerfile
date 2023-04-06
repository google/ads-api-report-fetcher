FROM python:3.10-slim-buster
ADD requirements.txt .
RUN pip install --require-hashes -r requirements.txt
ENTRYPOINT ["gaarf"]
