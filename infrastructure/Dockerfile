FROM python:3.6.7-stretch

RUN apt-get update && \
    apt-get install -y git

RUN mkdir /code

COPY requirements.txt /code

RUN mkdir /triage

RUN pip install -r /code/requirements.txt

COPY run.py /code

ENTRYPOINT [ "python" , "/code/run.py" ]
