# transactional key-value store API

## Requirements

* CMake `>= 3.5`
* C++ Compiler `>= C+17`
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libleveldb-dev doxygen graphviz
```

## How to build

```sh
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
ninja
```

available options:
* `-DBUILD_MOCK=OFF` - never build API momck
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths

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

