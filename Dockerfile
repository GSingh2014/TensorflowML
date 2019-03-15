FROM apache/nifi:latest
USER root
WORKDIR /opt/nifi/nifi-current
RUN mkdir TensorML
COPY data_loader TensorML
RUN chown nifi TensorML
