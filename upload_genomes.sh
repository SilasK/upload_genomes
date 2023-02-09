#! /usr/bin/env bash

#all_maifests
snakemake -s ~/upload_genomes/Snakefile -d ~/s/CMMG/ "Batches/batch1/manifest_validated"  $@
