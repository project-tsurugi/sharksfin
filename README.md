# transactional key-value store API

## Requirements

* CMake `>= 3.5`
* C++ Compiler `>= C++17`
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libleveldb-dev libboost-filesystem-dev doxygen graphviz
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
* `-DBUILD_MOCK=OFF` - never build API mock implementation
* `-DBUILD_FOEDUS_BRIDGE=ON` - build FOEDUS bridge
* `-DBUILD_EXAMPLES=OFF` - never build example programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DINSTALL_EXAMPLES=ON` - also install example programs (requires `BUILD_EXAMPLES` is enables)
* `-DEXAMPLE_IMPLEMENTATION=...` - link the specified target-name implementation to example programs
  * `mock` - link to mock implementation (default)
  * `foedus-bridge` - link to FOEDUS (requires `-DBUILD_FOEDUS_BRIDGE`)

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

