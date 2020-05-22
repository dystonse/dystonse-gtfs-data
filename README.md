# dystonse-gtfs-data

**This repository is a part of the multi-repository project `dystonse`. See the [main repository](https://github.com/dystonse/dystonse) for more information.**

This is a Rust crate that works with static gtfs schedules (as zip or directory), gtfs-realtime data (as .pb or .zip files) and a mysql database (setup info is specified in [dystonse-docker](https://github.com/dystonse/dystonse-docker)) to read, import or anaylse the data.

In **import** mode, it matches the realtime data to the schedule data and writes everything into the mysql database.

In **analyse** mode, it can count the data entries per time and output some simple statistics. More features are yet to come.

## How to use this

Basic syntax is `dystonse-gtfs-data [global options] <command> <subcommand> [args]`, or if you run it via cargo, `cargo run [--release] -- [global options] <command> <subcommand> [args]`.

There are a lot of database parameters to be defined globally. Those `DB_…`parameters can either be defined as environment variables (using the upper case names like `DB_PASSWORD`) or as command line parameters (using lower-case variants without the `db`-prefix, e.g. `--password`). Default values are provided for `DB_USER`, `DB_HOST`, `DB_PORT` and `DB_DATABASE`. In contrast, `DB_PASSWORD` and `GTFS_DATA_SOURCE_ID` always have to be specified when running this, where `GTFS_DATA_SOURCE_ID` is a string identifier that will be written as-is into the database for each entry. In the syntax examples below, we use a mix of env vars and command line parameters.

You can also use `dystonse-gtfs-data [command [subcommand]] --help` to get information about the command syntax.

## Importing data
### `import manual` mode

`DB_PASSWORD=<password> dystonse-gtfs-data [-v] --source <source> import manual <gtfs file path> <gfts-rt file path(s)>`

without `-v`, the only output on stdout is a list of the gtfs-realtime filenames that have been parsed successfully.

### `import automatic` and `import batch` mode
Instead of `manual` mode, you can use `automatic` or `batch` mode:

`DB_PASSWORD=<password> dystonse-gtfs-data -- [-v] --source <source> import automatic <dir>`

In automatic mode:

1. The importer will search for all schedules in `<dir>/schedule` and all realtime files in `<dir>/rt` and compute for each schedule which rt-files belong to that schedule. In this context, each realtime file belongs to the newest schedule that is older than the realtime data, as indicated by the date within the filenames.
2. Beginning with the oldest schedule, the importer will import each realtime file and move it to `<dir>/imported` on success or `<dir>/failed` if the import failed for reasons within the realtime file (if the filename is not suitable to extract a date, or if the file could not be parsed).
3. When all known files are processed, the importer will look for new files that appeared during its operation. If new files are found, it repeats from step 1.
4. If no new files were found during step 3, the importer will wait for a minute and then continue with step 3.

In `batch` mode, it works exactly as in `automatic` mode, but the importer exits after step 2.

## Analysing data
This has currently only one subcommand: `count`.

### `count` mode
For a given source id, this will count the number of valid real time entries for each time interval. An entry is considered valid if its `delay_arrival` is between -10 hours and +10 hours. The whole time span for which there is real time data will be split into parts of length corresponding  to the `interval` parameter, which has a default value of `1h` (one hour).

Simple statistics are output to `stdout` as CSV like this (space padding added for clarity, they won't be present in the real output):

```
time_min;            time_max;            stop_time update count; average delay; rt file count; rt file size
2020-03-16 00:41:02; 2020-03-16 04:41:02;                     72;       11.6111;            12;        18279
[...]
```

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
