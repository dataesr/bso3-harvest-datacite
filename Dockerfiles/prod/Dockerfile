FROM golang:1.19-rc-bullseye

#RUN go install github.com/miku/dcdump/cmd/dcdump@latest

RUN apt-get update
RUN apt-get install -y python3.9 \
    python3-pip

WORKDIR /src

COPY . /src

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --proxy=${HTTP_PROXY}

RUN go version
RUN cd /src && git clone https://github.com/miku/dcdump.git && cd dcdump && make
