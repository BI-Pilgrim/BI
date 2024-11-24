FROM python:3.11-bullseye

WORKDIR /dags/
ENV AIRFLOW_HOME /airflow_config
ENV TZ="Asia/Kolkata"

RUN pip install "apache-airflow[crypto,log]==2.10.3"

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY airflow_default_config /airflow_config
COPY ./dags /dags/
COPY setup.sh /airflow_config/

EXPOSE 8080

CMD ["bash", "/airflow_config/setup.sh"]