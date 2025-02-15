FROM apache/superset

USER root
RUN pip install google-cloud-bigquery sqlalchemy-bigquery

USER superset
