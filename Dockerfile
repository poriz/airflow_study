FROM apache/airflow:latest
USER root
ENV AIRFLOW_UID=50000
ENV AIRFLOW_GID=0
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install -r /requirements.txt