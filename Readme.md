# TileDB Unit Test Arrays

This repo contains arrays used for unit testing by TileDB. It also contains a helpful application `test_array_builer`
which can build new tiledb arrays based on a globally installed version of tiledb.

## Building New Arrays

To build new arrays first compile then run the `test_array_builder`:

```
git clone https://github.com/TileDB-Inc/TileDB-Unit-Test-Arrays.git
cd TileDB-Unit-Test-Arrays
mkdir build
cd build
cmake ..
make
./test_array_builder
```

The test array builder will create a folder called `arrays` in the current working directory of where it is run.
In the arrays folder will be a sub folder with the tiledb version. This can be copied to the top level arrays directory
and it will be used by TileDB for unit testing.