FROM golang:1.22.1

WORKDIR /app
RUN chmod -R 777 /app
COPY go.mod go.sum ./

# Download and cache dependencies (assumes go.mod and go.sum are tidy)
RUN go mod download

ADD flows2fim-snapshot.tar.gz /app/

RUN tar -tzf flows2fim-snapshot.tar.gz

RUN tar -xzf /app/flows2fim-snapshot.tar.gz  -C /app && rm /app/flows2fim-snapshot.tar.gz

RUN apt-get update && \
    apt-get install -y gdal-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN git config --global --add safe.directory /app

RUN apt-get update && apt-get install -y sudo

RUN sudo --version
