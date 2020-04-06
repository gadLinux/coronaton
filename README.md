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

Run with 15,5Gb and data dir:

> cargo run --release -- -b 16252928 -d /mnt/optional2/gaguilar/hackaton-datos/data/

Timing it: 

> time cargo run --release -- -b 16252928 -d /mnt/optional2/gaguilar/hackaton-datos/data/

Avoid cargo overhead:

> time ./target/release/coronaton -b 16252928 -d /mnt/optional2/gaguilar/hackaton-datos/data/

## Commands


## Build docker

You can build the image with:

> docker build -t coronaton-v1 .


## Runnind Docker and binding to the data volume

> docker run -d -it --name coronaton-v1 --mount type=bind,source=<source_dir>,target=/data <image>

We have:

 Source: in /mnt/optional2/gaguilar/hackaton-datos/data
 And image: coronaton-v1:latest
 
So we do

> docker run -d -it --name coronaton-v1 --mount type=bind,source=/mnt/optional2/gaguilar/hackaton-datos/data,target=/data coronaton-v1:latest
 

## Docker check output

> docker logs -f <container_id>

