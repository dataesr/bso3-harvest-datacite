FROM ubuntu:18.04

RUN  apt-get update \
  && apt-get install -y wget \
     gnupg2

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.8 \
    python3.8-dev \
    libpython3.8 \
    libpython3.8-dev \
    python3-wheel \
    python3-distutils \
    python3-apt \
    build-essential \
    jq \
    locales \
    locales-all \
    g++ \
    git \
    make \
    npm \
    curl \
    zstd \
    tar \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py

# Install last version of NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_17.x | bash -
RUN apt-get install -y nodejs

WORKDIR /src

ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8

COPY requirements.txt /src/requirements.txt
COPY requirements-dev.txt /src/requirements-dev.txt

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}
RUN pip3 install -r requirements-dev.txt --proxy=${HTTP_PROXY}
RUN npm install elasticdump -g

COPY --from=golang:1.18 /usr/local/go/ /usr/local/go/
ENV PATH="/usr/local/go/bin:${PATH}"

COPY . /src

RUN go version
RUN cd /src && git clone https://github.com/miku/dcdump.git

COPY affiliation_main.go /src/dcdump/cmd/main.go

RUN cd /src/dcdump && git checkout 3691d32a4b6d6029847acd5ce13eadc2e370a29f && make