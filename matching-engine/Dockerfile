FROM rust:1.87-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/matching-engine
COPY . .

RUN cargo build --release


FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    libcurl4 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/matching-engine/target/release/matching-engine /usr/local/bin/matching-engine

EXPOSE 50051
EXPOSE 9102

ENV KAFKA_BROKERS=kafka:9092
CMD ["matching-engine"]