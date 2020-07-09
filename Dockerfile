FROM rust:1.41 as builder
WORKDIR /usr/src/myapp
RUN apt-get update && apt-get install -y protobuf-compiler
COPY ./Cargo.* ./
RUN mkdir src && echo "fn main() { println!(\"Hello, world!\"); }" > src/main.rs
RUN cargo fetch
RUN RUSTFLAGS=-g cargo build --release
RUN rm src/main.rs 
COPY . .
RUN touch src/main.rs
RUN RUSTFLAGS=-g cargo build --release

FROM debian:buster-slim
RUN apt-get update && apt-get install -y libssl1.1 libfontconfig gnuplot-nox
COPY --from=builder /usr/src/myapp/target/release/dystonse-gtfs-data /usr/local/bin/dystonse-gtfs-data
WORKDIR /

# Set time zone. Taken from https://serverfault.com/a/683651
ENV TZ=Europe/Berlin
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV RUST_BACKTRACE=full