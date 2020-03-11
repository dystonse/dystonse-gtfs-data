# docker-rust-test

This is a simple test repository. It contains a hello-world-application written in Rust, and some docker fluff:

 * compile inside a docker container
 * copy binary into another container

Use `docker buildx build --platform linux/amd64,linux/arm/v7 -t dystonse/rust-test:latest --push .` to build and push the containers for both `linux/amd64` and `linux/arm/v7` architectures.