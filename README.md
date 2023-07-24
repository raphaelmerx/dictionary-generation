# Getting dictionary from parallel corpus using mGiza

## Installation

1. Install [Snakemake](https://snakemake.readthedocs.io/): `brew install snakemake`
2. Install git submodules (mosesdecoder and mgiza): `git submodule update --init --recursive`
3. Compile mgiza:
```bash
cd mgiza/mgizapp
cmake .
make
```

## Usage

1. Define config variables `LANG1`, `LANG2` and the parallel corpus file prefixes in `TRAIN_PREFIXES`. E.g.:
```
# config.yaml
LANG1: "en"
LANG2: "tpi"

TRAIN_PREFIXES:
    - "bible"  # assuming you have bible.en and bible.tpi files in this directory
```
2. Run snakemake : `snakemake --cores 2`

It will output a file `lang1-lang2.dic`, e.g.
```
en      tpi
disease sik
sick    sik
illness sik
tuberculosis    tb
tb      tb
he      em
his     em
him     em
a       wanpela
```
