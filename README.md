# docker-rust-test

This is a simple test repository. It contains a hello-world-application written in Rust, and some docker fluff:

 * compile inside a docker container
 * copy binary into another container

Use `docker buildx build --platform linux/amd64,linux/arm/v7 -t dystonse/rust-test:latest --push .` to build and push the containers for both `linux/amd64` and `linux/arm/v7` architectures.

You might have to enable experimental features first, e.g. using `export DOCKER_CLI_EXPERIMENTAL=enabled`.

Also, you might have to create and activate a builder, as documented [here for Docker Desktop (Mac and Windows)](https://docs.docker.com/docker-for-mac/multi-arch/) or [here for Linux hosts](https://mirailabs.io/blog/multiarch-docker-with-buildx/).