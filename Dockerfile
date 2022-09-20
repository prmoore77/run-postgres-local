FROM --platform=linux/amd64 postgres:14

WORKDIR /tmp

RUN apt-get update --yes && \
    apt-get dist-upgrade --yes && \
    apt-get install --yes \
    build-essential \
    git \
    postgresql-server-dev-14 \
    clang-11

RUN git clone https://github.com/adjust/parquet_fdw.git && \
    cd parquet_fdw && \
    ./install_arrow.sh && \
    make install
