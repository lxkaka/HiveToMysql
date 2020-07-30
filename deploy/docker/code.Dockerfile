FROM airflow/base:1.10.10-condapy3.7

RUN mkdir -p /root/airflow

ARG ENV
WORKDIR /root/airflow
COPY requirements.txt /root/airflow
COPY deploy/k8sconfig /root/airflow
RUN pip install -r requirements.txt
COPY src/ /root/airflow/
COPY deploy/$ENV/airflow.cfg /root/airflow/
RUN echo /root/airflow > /usr/local/lib/python3.7/site-packages/.pth
