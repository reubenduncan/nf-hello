# Kenya Sand Dam CheckM2 QC

Standalone CheckM2 quality control for retained MAG candidate FASTAs. It is
designed for the Kenya Sand Dam 2025 nf-core/mag output and does not repeat
read processing, assembly, binning or annotation.

The input manifest contains one S3 FASTA URI per retained bin and groups them
by binner/coassembly. The workflow downloads the CheckM2 v1.1.0 database once,
runs at most six independent batches concurrently, publishes each batch report,
and merges them into `checkm2_summary.tsv`.
