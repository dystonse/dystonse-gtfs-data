# dystonse-gtfs-data

**This repository is a part of the multi-repository project `dystonse`. See the [main repository](https://github.com/dystonse/dystonse) for more information.**

This is a Rust crate that works with static gtfs schedules (as zip or directory), gtfs-realtime data (as .pb or .zip files) and a mysql database (setup info is specified in [dystonse-docker](https://github.com/dystonse/dystonse-docker)) to read, import or anaylse the data.

In **import** mode, it matches the realtime data to the schedule data and writes everything into the mysql database.

In **analyse** mode, it can compute delay probability curves both for specific and general data sets, and save them as small machine-readable files or human-readable images in different formats. It can also count the data entries per time and output some simple statistics.

In **predict** mode, it can look up and return the delay probability curve that is most useful for predicting the delay of a specified trip, stop, time, and (optional) delay at a specified earlier stop.

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
In addition to the global arguments (for database connection etc., see above), the `analyse` command needs `dir` and `schedule` arguments - a directory where data should be read from/written to, and the filename of a schedule file to use for the analyses.

Additional required arguments depend on the subcommand you want to use:

### `count` mode
For a given source id, this will count the number of valid real time entries for each time interval. An entry is considered valid if its `delay_arrival` is between -10 hours and +10 hours. The whole time span for which there is real time data will be split into parts of length corresponding  to the `interval` parameter, which has a default value of `1h` (one hour).

Simple statistics are output to `stdout` as CSV like this (space padding added for clarity, they won't be present in the real output):

```
time_min;            time_max;            stop_time update count; average delay; rt file count; rt file size
2020-03-16 00:41:02; 2020-03-16 04:41:02;                     72;       11.6111;            12;        18279
[...]
```
### `graph` mode
This will compute visual schedules of the given `route-ids` (or `all`) and save them as png images in a directory structure sorted by agency and route. See [this post on our blog in german language](http://blog.dystonse.org/opendata/2020/04/20/datensammlung-2.html) for more info about visual schedules (_Bildfahrpläne_).

### `compute-specific-curves` mode
This will compute specific delay probability curves for a given set of `route-ids` (or for all route-ids available in the schedule, if `all` is used instead). As long as there are enough data points in the database, it creates the following things for each route variant and each time slot:
 * curves of the general distribution of delays at each stop (one curve each for arrival and one for departure delays)
 * curve sets of the distribution of arrival delays at each stop, depending on the departure delay at another (earlier) stop (one curve set for each pair of two stops)
 
### `compute-default-curves` mode
This will compute aggregated delay probability curves divided by the following general categories:
 * route type: tram/subway/rail/bus/ferry
 * route section: beginning/middle/end, see [here](https://github.com/dystonse/dystonse-gtfs-data/blob/master/src/types/route_sections.rs) for the specification.
 * time slot: 11 separate time categories defined by weekdays and hours, see [here](https://github.com/dystonse/dystonse-gtfs-data/blob/master/src/types/time_slots.rs) for the specification.

### `compute-curves` mode
This will compute delay probability curves, using the collected data in the database. The curves (both specific and default) are saved into a file named "all_curves.exp" in the specified data directory. When the argument `route-ids` is given, the specific curves are only computed for the given route-ids. When the argument `all` is given, all available route-ids from the schedule are used.

### `draw-curves` mode
This will compute specific delay probability curve sets for the given `route-ids` and output them as diagrams in svg file format with human-readable title (in german) and labels/captions. One file is created for each pair of stops in each route variant and each time slot, sorted into a directory structure.

## Prediction lookup
In addition to the global arguments (for database connection etc., see above), the `predict` command needs `dir` and `schedule` arguments - a directory where the precomputed curves should be read from, and the filename of a schedule file to use for looking up all data that are not contained in the curve files.

Additional required arguments depend on the subcommand you want to use. Currently, only the `single` subcommand is implemented.

### `single` mode
This will lookup a single curve or curve set depending on the values of the arguments, and print the output to the command line (we are currently working on a more useful interface for this output).
The following arguments are needed: 
 * `route-id`, `trip-id` and `stop-id` (according to the schedule) of where you want to get a prediction for
 * `event-type`: arrival or departure
 * `date-time` date and time of when you want to be at the specified stop
 * (optional) `start-stop-id` of a previous stop where the vehicle has already been
 * (optional) `initial-delay` at the previous stop. If `start-stop-id` is given, but `initial-delay` is not given, the result will be a curve set instead of a single curve
 * (optional) `use-realtime`: if given instead of `start-stop-id` and `initial-delay`, the predictor module will try to look up a useful `start-stop-id` and `initial-delay` from the database (if there are current realtime data for this trip) . Obviusly, this works only in a very narrow time window, where the vehicle has already started its trip, but not yet arrived at `stop-id`.
 
 ### `start`mode
 (not yet implemented.)

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
