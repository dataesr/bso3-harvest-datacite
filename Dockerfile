FROM ubuntu:18.04

RUN  apt-get update \
  && apt-get install -y wget \
     gnupg2

RUN wget -qO - https://www.mongodb.org/static/pgp/server-3.4.asc | apt-key add -

RUN echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.6 \
    python3-pip \
    libpython3.6 \
    jq \
    mongodb-org \
    locales \
    locales-all \
    python3-setuptools \
    g++ \
    git \
    make \
    python3-dev \
    npm \
    curl \
    zstd \
    tar \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install last version of NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
RUN apt-get install -y nodejs

WORKDIR /src

ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8

COPY requirements.txt /src/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}

COPY --from=golang:1.18 /usr/local/go/ /usr/local/go/
ENV PATH="/usr/local/go/bin:${PATH}"

#RUN wget -L "https://golang.org/dl/go1.18.1.linux-amd64.tar.gz" && tar -xf "go1.18.1.linux-amd64.tar.gz"
#ENV PATH="/usr/local/go/bin:${PATH}"

#RUN export PATH="/usr/local/go/bin:${PATH}" && git clone https://github.com/miku/dcdump.git && cd dcdump && make

COPY . /src

RUN go version
RUN git clone https://github.com/miku/dcdump.git && cd dcdump && mv /src/affiliation_main.go cmd/dcdump/main.go && make

COPY . /src
