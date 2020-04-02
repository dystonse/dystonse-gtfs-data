FROM rust:1.41 as builder
WORKDIR /usr/src/myapp
RUN apt-get update && apt-get install -y protobuf-compiler
COPY ./Cargo.* ./
RUN mkdir src && echo "fn main() { println!(\"Hello, world!\"); }" > src/main.rs
RUN cargo fetch
RUN cargo build --release
RUN rm src/main.rs 
COPY . .
RUN touch src/main.rs
RUN cargo build --release
RUN cargo install --offline --path .

FROM debian:buster-slim
RUN apt-get update && apt-get install -y libssl1.1
COPY --from=builder /usr/local/cargo/bin/dystonse-gtfs-importer /usr/local/bin/dystonse-gtfs-importer
WORKDIR /

# Set time zone. Taken from https://serverfault.com/a/683651
ENV TZ=Europe/Berlin
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD dystonse-gtfs-importer -v automatic /files/$GTFS_DATA_SOURCE_ID/
