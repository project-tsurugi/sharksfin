# transactional key-value store API

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev doxygen libnuma-dev
```

### Install modules

#### tsurugidb modules

This requires below [tsurugidb](https://github.com/project-tsurugi/tsurugidb) modules to be installed.

* [shirakami](https://github.com/project-tsurugi/shirakami) if you want to run this API with it.


## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

available options:
* `-DBUILD_TESTS=ON` - build test programs
* `-DBUILD_MEMORY=ON` - build API in-memory implementation
* `-DBUILD_SHIRAKAMI=OFF` - never build shirakami bridge
* `-DBUILD_EXAMPLES=ON` - build example programs
* `-DBUILD_DOCUMENTS=ON` - build documents by doxygen
* `-DBUILD_STRICT=OFF` - don't treat compile warnings as build errors
* `-DINSTALL_EXAMPLES=ON` - also install example programs (requires `BUILD_EXAMPLES` is enables)
* `-DEXAMPLE_IMPLEMENTATION=...` - link the specified target-name implementation to example programs
  * `memory` - link to in-memory implementation
  * `shirakami` - link to shirakami implementation (default)
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate dependant installation directory
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries

### install

```sh
cmake --build . --target install
```

### run tests

```sh
ctest -V
```

### generate documents

```sh
cmake --build . --target doxygen
```

### Customize logging setting
Sharksfin internally uses [glog](https://github.com/google/glog) so you can pass glog environment variables such as `GLOG_logtostderr=1` to customize the logging output of executable that uses sharksfin.

```sh
GLOG_logtostderr=1 ./sharksfin-cli -Dlocation=./db1 put 0 A
```

`GLOG_minloglevel` can be used to set the log level (0=INFO, 1=WARN, 2=ERROR, 3=FATAL). Default log level is WARN.

```sh
GLOG_minloglevel=0 ./sharksfin-cli -Dlocation=./db1 put 0 A
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

