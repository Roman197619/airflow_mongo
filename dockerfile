FROM apache/airflow:3.1.7

# copy requirements
COPY requirements.txt /

# install packages as non-root 'airflow' user to avoid pip root warning
# ensure user local bin is on PATH so installed packages are importable
ENV PATH=/home/airflow/.local/bin:$PATH
USER 50000

RUN pip install --upgrade --user pip setuptools wheel \
	&& pip install --no-cache-dir --user -r /requirements.txt \
	&& pip install --no-cache-dir --user apache-airflow-providers-mongo