FROM node:16
WORKDIR /app

# Installing from npm
RUN npm i google-ads-api-report-fetcher -g

# Installing from sources:
#RUN apt-get install git
#RUN git clone https://github.com/google/ads-api-report-fetcher.git .
#RUN cd js && npm i -ci
#WORKDIR /app/js

ENV GOOGLE_APPLICATION_CREDENTIALS /app/service_account.json

ENTRYPOINT ["gaarf"]
CMD ["ads-scripts/*.sql"]