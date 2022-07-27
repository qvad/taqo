FROM --platform=linux/amd64 ubuntu:20.04

ENV PYTHON_VERSION "3.10.5"
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt install -y postgresql postgresql-contrib postgresql-client git wget curl unzip libpq-dev \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install -b /usr/bin \
    && apt install -y build-essential libreadline-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev jq ssh \
    && mkdir python-source \
    && cd python-source/ \
    && wget -c https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
    && tar -Jxf Python-${PYTHON_VERSION}.tar.xz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure --enable-optimizations \
    && make altinstall -j4 \
    && update-alternatives --install /usr/bin/python python /usr/local/bin/python${PYTHON_VERSION%.*} 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/local/bin/pip${PYTHON_VERSION%.*} 1 \
    && apt install -y uuid-dev libreadline-dev maven ninja-build cmake curl rsync python3-pip python3-venv zip autoconf libtool pkg-config libicu-dev bison flex libncurses5-dev \
    && apt install -y asciidoctor \
    && apt install -y ruby-coderay