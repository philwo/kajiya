# 🔥 鍛冶屋 (Kajiya)

Kajiya is an RBE-compatible REAPI backend implementation used as a testing
server during development of Chromium's new build tooling. It is not meant
for production use, but can be very useful for local testing of any remote
execution related code.

## How to use

```shell
$ go build && ./kajiya

# Build Bazel using kajiya as the backend.
$ bazel build --remote_executor=grpc://localhost:50051 //src:bazel

# Build Chromium with autoninja + reclient using kajiya as the backend.
$ gn gen out/default --args="use_remoteexec=true"
$ env \
    RBE_automatic_auth=false \
    RBE_service="localhost:50051" \
    RBE_service_no_security=true \
    RBE_service_no_auth=true \
    RBE_compression_threshold=-1 \
    autoninja -C out/default -j $(nproc) chrome
```

## Features

Kajiya can act as an REAPI remote cache and/or remote executor. By default, both
services are provided, but you can also run an executor without a cache, or a
cache without an executor:

```shell
# Remote execution without caching
$ ./kajiya -cache=false

# Remote caching without execution (clients must upload action results)
$ ./kajiya -execution=false
```
