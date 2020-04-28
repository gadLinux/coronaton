# Coronaton challenge in Rust



## Building

Point the Arrow and DataFusion deps to your local install because we need
snapshot version. And...

> cargo build --release

Built on nightly, it seems cargo segfaults. We have to run several times to 
be built.

## Running

Will show the help: 
> cargo run --release -- --help

Run with 15,5Gb and data dir. 15GB because it uses the memory on -b for each file:

> cargo run --release -- -b 239013 -d /mnt/optional2/gaguilar/hackaton-datos/data/

Timing it: 

> time cargo run --release -- -b 239013 -d /mnt/optional2/gaguilar/hackaton-datos/data/

Avoid cargo overhead:

> time ./target/release/coronaton -b 239013 -d /mnt/optional2/gaguilar/hackaton-datos/data/

## Commands


## Build docker

You can build the image with. Note: Now builds on the docker machine the target so it takes 
a looooooooong time.

> docker build -t coronaton-v1 .


## Runnind Docker and binding to the data volume

> docker run -d -it --cpus=8 --name coronaton-v1 --mount type=bind,source=<source_dir>,target=/data <image>

We have:

 Source: in /mnt/optional2/gaguilar/hackaton-datos/data
 And image: coronaton-v1:latest
 
So we do

> docker run -d -it --cpus=8 --name coronaton-v1 --mount type=bind,source=/mnt/optional2/gaguilar/hackaton-datos/data,target=/data coronaton-v1:latest


## Docker check output

> docker logs -f <container_id>

## Logging

You can set desired log level by setting the environment variable RUST_LOG=debug as defined here: 
https://docs.rs/env_logger/0.7.1/env_logger/

The logs will be shown as part as the application

RUST_LOG=debug will set logs to debug


## Debugging

After compiling the target you can backtrace the crashes by adding RUST_BACKTRACE=1 environment
variable.
