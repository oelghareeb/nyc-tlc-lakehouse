FROM bitnamilegacy/spark:3.5.2

USER root

# Install Python, pip, and Jupyter
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install jupyterlab pyspark==3.5.2 pandas numpy matplotlib

ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
