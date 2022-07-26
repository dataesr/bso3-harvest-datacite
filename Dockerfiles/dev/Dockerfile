FROM golang:1.19-rc-bullseye

RUN apt-get update
RUN apt-get install -y python3.9 \
    python3-pip \
    make

WORKDIR /src

COPY requirements.txt /src/requirements.txt
COPY requirements-dev.txt /src/requirements-dev.txt

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}
RUN pip3 install -r requirements-dev.txt --proxy=${HTTP_PROXY}

RUN go version
RUN cd /src && git clone https://github.com/miku/dcdump.git

COPY affiliation_main.go /src/dcdump/cmd/main.go

RUN cd /src/dcdump && make