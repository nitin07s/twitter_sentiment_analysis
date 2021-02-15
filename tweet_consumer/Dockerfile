FROM python:3.8-alpine3.11

RUN apk update && apk add build-base

WORKDIR /tweet_consumer
COPY . .

# Download data and modules required by the NLTK library
# for pre-processing tweets
RUN pip install -r requirements.txt

CMD ["python", "run.py"]