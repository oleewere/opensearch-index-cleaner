FROM rust:1.65 AS build

COPY . /app/
WORKDIR /app/

RUN cargo build --release

FROM debian:buster-slim AS opensearch-index-cleaner
RUN apt-get update && apt-get install -y openssl && apt-get clean
COPY --from=build /app/target/release/opensearch-index-cleaner /app/opensearch-index-cleaner
WORKDIR /app
ENTRYPOINT ["/app/opensearch-index-cleaner"]
