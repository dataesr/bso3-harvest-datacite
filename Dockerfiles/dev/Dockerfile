FROM golang:1.19-rc-bullseye

RUN apt-get update
RUN apt-get install -y python3.9 \
    python3-pip \
    make

WORKDIR /src

COPY requirements.txt /src/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}

RUN go version
RUN cd /src && git clone https://github.com/miku/dcdump.git && cd dcdump && make