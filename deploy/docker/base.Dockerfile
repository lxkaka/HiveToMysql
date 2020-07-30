FROM conda/miniconda3
RUN apt-get update && apt-get install -y libsasl2-dev sasl2-bin libsasl2-2 libsasl2-modules default-libmysqlclient-dev gcc build-essential
COPY .condarc /root
RUN mkdir -p /root/airflow
WORKDIR /root/airflow
COPY requirements.txt /root/airflow
RUN conda install python=3.7
RUN conda install pyarrow=0.17.1 -c conda-forge
RUN pip install -r requirements.txt