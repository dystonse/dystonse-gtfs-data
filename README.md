# dystonse-gtfs-importer

This is a Rust crate that reads a static gtfs schedule file and any number of gtfs-realtime .pb or .zip files (given as command line arguments), matches the realtime data to the schedule data and writes everything into a mysql database.

## How to use this

`DB_PASSWORD=<password> cargo run [--release] -- [-v] manual <gtfs file path> <gfts-rt file path(s)>`

A mysql database (setup info is specified in [dystonse-docker](https://github.com/dystonse/dystonse-docker)) needs to be running before you can use this crate.

The `DB_…`parameters can either be defined as environment variables (using the upper case names like `DB_PASSWORD`) or as command line parameters (using lower-case variants without the `db`-prefix, e.g. `--password`). Default values are provided for `DB_USER`, `DB_HOST`, `DB_PORT` and `DB_DATABASE`. In contrast, `DB_PASSWORD` always has to be specified when running this.

without `-v`, the only output on stdout is a list of the gtfs-realtime filenames that have been parsed successfully.

## Automatic mode
Instead of `manual` mode, you can use `automatic` or `batch` mode:

`DB_PASSWORD=<password> cargo run [--release] -- [-v] automatic <dir>`

In automatic mode:

1. The importer will search for all schedules in `<dir>/schedule` and all realtime files in `<dir>/rt` and compute for each schedule which rt-files belong to that schedule. In this context, each realtime file belongs to the newest schedule that is older then the realtime data, as indicated by the date within the filenames.
2. Beginning with the oldest schedule, the importer will import each realtime file and move it to `<dir>/imported` on success or `<dir>/failed` if the import failed for reasons within the realtime file. _**TODO:** The exact criteria for failing are to be defined, currently no file will be moved into `<dir>/failed`_
3. When all known files are processed, the importer will look for new files that appeared duing its operation. If new files are found, it repeats from step 1.
4. If no new files were found during step 3, the importer will wait for a minute and then continue with step 3.

In batch mode, it works exactly as in automatic mode, but the importer exits after step 2.

_**TODO:** We need to avoid reading files that are currently being written too. Maybe we should ignore files that have been modified very recently, e.g. within the last 2 minutes._

## Docker integration

This started out as a simple test repository for compiling Rust applications in docker. It used to contain a hello-world-application written in Rust, and some docker fluff:

 * compile inside a docker container
 * copy binary into another container

We used it to test rust development in general, and to check if this works with cross-compiling.

## How to cross-compile

_NOTE: The following parts are probably outdated. We will update them when we have fixed the docker config for the current crate, so that it can be compiled into a usable docker image again_

Use `docker buildx build --platform linux/amd64,linux/arm/v7 -t dystonse/rust-test:latest --push .` to build and push the containers for both `linux/amd64` and `linux/arm/v7` architectures.

You might have to enable experimental features first, e.g. using `export DOCKER_CLI_EXPERIMENTAL=enabled`.

Also, you might have to create and activate a builder, as documented [here for Docker Desktop (Mac and Windows)](https://docs.docker.com/docker-for-mac/multi-arch/) or [here for Linux hosts](https://mirailabs.io/blog/multiarch-docker-with-buildx/).

## Known problems with cross-compiling
We hit a problem when cross-compiling a rust application with dependencies on Docker Desktop for Mac. While building the arm/v7 container, `cargo build` can't read some git-specific directory, as explained in [this issue](https://github.com/rust-lang/cargo/issues/7451).

It boils down to a broken emulation of system calls when emulating a 32-Bit system on a 64-bit host using qemu. The actual bug - if you call it a bug - is not in `qemu` but in `libc`.

A good workaround should be to use a host kernel which has been compiled with the `CONFIG_X86_X32` configuration flag. Docker Desktop for Mac used a virtualized Linux host using HyperKit. The linux image is build with `LinuxKit`, however, we could not verify if the image shipped with Docker Desktop has the `CONFIG_X86_X32` configuration flag (probably not).

But the same error occurs when cross-compiling on another host, which runs a Debian Linux natively. According to its `/boot/config-4.19.0-8-amd64` file, the `CONFIG_X86_X32` configuration is enabled there, so it should have worked.
