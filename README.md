# transactional key-value store API

## Requirements

* CMake `>= 3.5`
* C++ Compiler `>= C++17`
* [FOEDUS](https://github.com/large-scale-oltp-team/foedus_code) if you want to run this API with it.
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libleveldb-dev libboost-filesystem-dev doxygen
```

## How to setup FOEDUS

The API runs with LevelDB mock by default. If you want to run it with FOEDUS, install FOEDUS before building the api code and specify `-DBUILD_FOEDUS_BRIDGE=ON` for cmake command line used in the build step (see the build step below). Follow the procedure to install FOEDUS.

```sh
cd sharksfin/third_party/foedus
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=<installation directory> -DGFLAGS_INTTYPES_FORMAT=C99  ..
ninja
ninja install
```

Here `-DCMAKE_INSTALL_PREFIX` is the optional parameter to install FOEDUS into custom directory. `<installation directory>` can be any directory you like to install FOEDUS library and header files.
If it's not specified, FOEDUS will be installed in /usr/local.

For details of FOEDUS environment setup, refer to the [guide](https://github.com/large-scale-oltp-team/foedus_code/tree/master/foedus-core).

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_MOCK=OFF` - never build API mock implementation
* `-DBUILD_FOEDUS_BRIDGE=ON` - build FOEDUS bridge
* `-DBUILD_EXAMPLES=OFF` - never build example programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DINSTALL_EXAMPLES=ON` - also install example programs (requires `BUILD_EXAMPLES` is enables)
* `-DEXAMPLE_IMPLEMENTATION=...` - link the specified target-name implementation to example programs
  * `mock` - link to mock implementation (default)
  * `foedus-bridge` - link to FOEDUS (requires `-DBUILD_FOEDUS_BRIDGE`)
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate FOEDUS installation directory

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

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

