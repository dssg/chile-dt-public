FROM python:3.6

LABEL triage.version="bleeding-edge" \
      creator="Center for Data Science and Public Policy (DSaPP)" \
      maintainer="Adolfo De Unánue <adolfo@uchicago.edu>"

RUN apt update && \
    apt-get --yes install graphviz

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
