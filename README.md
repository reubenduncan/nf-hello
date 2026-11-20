# hello

A small Nextflow pipeline that demonstrates parameters, staged file inputs, and text outputs.

By default, it writes one text file for each of four greetings under `results/greetings/`. Each file contains a greeting, a numeric run label, and the contents of `assets/input.txt`.

```bash
nextflow run . \
  --recipient "Nextflow learner" \
  --tone curious \
  --run_number 2.5 \
  --uppercase true
```

Use `--input_file path/to/message.txt` to include another text file. The available options and their groups are described in [nextflow_schema.json](nextflow_schema.json).

https://nextflow.io/
