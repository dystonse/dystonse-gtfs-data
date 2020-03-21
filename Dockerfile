FROM rust:1.41 as builder
WORKDIR /usr/src/myapp
RUN apt-get update && apt-get install -y protobuf-compiler
COPY ./Cargo.* ./
RUN mkdir src && echo "fn main() { println!(\"Hello, world!\"); }" > src/main.rs
RUN cargo fetch
RUN cargo build --release
COPY . .
RUN cargo build --release
RUN cargo install --path .

FROM debian:buster-slim
COPY --from=builder /usr/local/cargo/bin/myapp /usr/local/bin/myapp
WORKDIR /
COPY --from=builder /usr/src/myapp/data/* ./
CMD ["myapp","vbn-gtfsrt-2020-03-18T07:42:01+01:00.pb"]
