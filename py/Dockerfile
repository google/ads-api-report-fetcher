FROM python:3.7
WORKDIR /app

# Installing from PyPi:
RUN pip install google-ads-api-report-fetcher

# Installing from sources:
#RUN apt-get install git
#RUN git clone https://github.com/google/ads-api-report-fetcher.git .
#RUN cd js && pip install -r requirements.txt
#WORKDIR /app/py

ENV GOOGLE_APPLICATION_CREDENTIALS /app/service_account.json

ENTRYPOINT ["./gaarf"]
CMD ["ads-scripts/*.sql"]