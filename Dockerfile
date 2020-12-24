FROM python:3.8.5

RUN apt-get -qq update && apt-get -qq -y install --no-install-recommends build-essential

RUN pip install pipenv
COPY Pipfile* /tmp/
RUN cd /tmp && pipenv lock --dev --requirements > requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /aoe2stats
