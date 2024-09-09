# TileDB Unit Test Arrays

This repo contains arrays used for unit testing by TileDB. It also contains a helpful application `test_array_builder`
which can build new tiledb arrays based on a globally installed version of tiledb.

## Building New Arrays

To build new arrays first compile then run the `test_array_builder`. Run
the following commands, replacing "/your/path/to/TileDB/dist/" with your own
installation path.

```
git clone https://github.com/TileDB-Inc/TileDB-Unit-Test-Arrays.git
cd TileDB-Unit-Test-Arrays
mkdir build
cd build
cmake .. -DCMAKE_PREFIX_PATH="/your/path/to/TileDB/dist/"
make
./test_array_builder
```

The test array builder will create a folder called `arrays` in the current working directory of where it is run.
In the arrays folder will be a sub folder with the tiledb version. This can be copied to the top level directory
and it will be used by TileDB for unit testing.

## Reviewers
Reviewing PRs in this repository is not always trivial. Many PRs add 5k+ files which cannot be opened in the browser, and the repository itself takes a long time to clone. Additionally, there is no alternative, older branch with less data to be cloned. To overcome these hurdles, here is one suggested way to add a review:
- Install GitHub commandline tools (on macOs it's a simple `brew install gh`)
- Login to your account by running `gh auth login`
- In order to submit a review, you need to be in a git repository, and you need to have one of the remotes pointing to this repo, here's how to do it without cloning this repo:
- Go in any git repo you have locally, run `git remote add testarrays git@github.com:TileDB-Inc/TileDB-Unit-Test-Arrays.git`. This is needed because the next command requires you at least have a remote pointing to the default repository you set.
- Run `gh repo set-default git@github.com:TileDB-Inc/TileDB-Unit-Test-Arrays.git`.
- Now you can submit your review with: `gh pr review --approve <PR #>`

