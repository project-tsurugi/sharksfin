# transactional key-value store API

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* [shirakami](https://github.com/project-tsurugi/shirakami) if you want to run this API with it.
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

## How to setup shirakami 

If you want to run sharksfin with shirakami, install it before building the api code. Follow the procedure below.
(see [instruction](https://github.com/project-tsurugi/shirakami/blob/master/README.md) for details)

```sh
cd sharksfin/third_party/shirakami
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=<installation directory> ..
ninja
ninja install
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_MEMORY=OFF` - never build API in-memory implementation
* `-DBUILD_SHIRAKAMI=OFF` - never build shirakami bridge
* `-DBUILD_SHIRAKAMI_WP=ON` - (temporary) enables building with shirakami build with BUILD_WP=ON/BUILD_CPR=OFF. Use with BUILD_SHIRAKAMI=ON.
* `-DBUILD_EXAMPLES=OFF` - never build example programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DINSTALL_EXAMPLES=ON` - also install example programs (requires `BUILD_EXAMPLES` is enables)
* `-DEXAMPLE_IMPLEMENTATION=...` - link the specified target-name implementation to example programs
  * `memory` - link to in-memory implementation (default)
  * `shirakami` - link to shirakami implementation
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate dependant installation directory
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries
  
### install

```sh
ninja install
```

### run tests

```sh
ctest
```

### generate documents

```sh
ninja doxygen
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

