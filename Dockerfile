FROM golang:1.22.1

WORKDIR /app
COPY go.mod go.sum ./
RUN ls -al /app
COPY . ./
RUN ls -al /app
# Download and cache dependencies (assumes go.mod and go.sum are tidy)
RUN go mod download


RUN apt-get update && \
    apt-get install -y gdal-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN git config --global --add safe.directory /app
