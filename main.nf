nextflow.enable.dsl = 2

params.manifest = null
params.outdir = null
params.checkm2_database = 'https://zenodo.org/records/14897628/files/checkm2_database.tar.gz'
params.serial_gene_batches = 'FLYE-MaxBin2-group-167-sf'

if (!params.manifest) {
    error "Provide --manifest with a TSV containing batch_id and uri columns."
}
if (!params.outdir) {
    error "Provide --outdir for published CheckM2 summaries."
}

process DOWNLOAD_CHECKM2_DATABASE {
    tag 'CheckM2 database'
    label 'process_single'

    publishDir "${params.outdir}/database", mode: 'copy', overwrite: false

    input:
    val database_url

    output:
    path 'checkm2_database.tar.gz', emit: database

    script:
    """
    python - <<'PY'
    import time
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    url = '${database_url}'
    destination = 'checkm2_database.tar.gz'

    for attempt in range(1, 6):
        try:
            request = Request(url, headers={'User-Agent': 'Kenya-Sand-Dam-CheckM2-QC/1.0'})
            with urlopen(request, timeout=120) as response, open(destination, 'wb') as output:
                expected_bytes = response.headers.get('Content-Length')
                bytes_written = 0
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    bytes_written += len(chunk)
            if expected_bytes is not None and bytes_written != int(expected_bytes):
                raise IOError(f'Incomplete download: {bytes_written} of {expected_bytes} bytes')
            break
        except (HTTPError, URLError, OSError, TimeoutError) as error:
            if attempt == 5:
                raise
            time.sleep(attempt * 10)
    PY
    """

    stub:
    """
    touch checkm2_database.tar.gz
    """
}

process CHECKM2_QC {
    tag "${batch_id} (${bins.size()} bins)"
    label 'process_medium'

    publishDir "${params.outdir}/batches", mode: 'copy', pattern: '*_checkm2_report.tsv', overwrite: true

    input:
    tuple val(batch_id), path(bins, stageAs: 'input_bins/*')
    path checkm2_database

    output:
    tuple val(batch_id), path("${batch_id}_checkm2_report.tsv"), emit: report

    script:
    """
    mkdir input_fasta checkm2_db
    for bin in input_bins/*.fa.gz; do
        gunzip -c "\$bin" > "input_fasta/\$(basename "\${bin%.gz}")"
    done

    tar -xzf ${checkm2_database} -C checkm2_db
    db=\$(find -L checkm2_db -type f -name '*.dmnd' -print -quit)
    test -n "\$db"

    checkm2 predict \\
        --input input_fasta/*.fa \\
        --output-directory '${batch_id}' \\
        --threads ${task.cpus} \\
        --database_path "\$db"

    cp '${batch_id}/quality_report.tsv' '${batch_id}_checkm2_report.tsv'
    """

    stub:
    """
    printf 'Name\tCompleteness\tContamination\n' > '${batch_id}_checkm2_report.tsv'
    """
}

process CHECKM2_QC_SERIAL_GENES {
    tag "${batch_id} (${bins.size()} bins; serial gene calling)"
    label 'process_medium'

    publishDir "${params.outdir}/batches", mode: 'copy', pattern: '*_checkm2_report.tsv', overwrite: true

    input:
    tuple val(batch_id), path(bins, stageAs: 'input_bins/*')
    path checkm2_database

    output:
    tuple val(batch_id), path("${batch_id}_checkm2_report.tsv"), emit: report

    script:
    """
    mkdir input_fasta protein_fasta checkm2_db
    for bin in input_bins/*.fa.gz; do
        gunzip -c "\$bin" > "input_fasta/\$(basename "\${bin%.gz}")"
    done

    tar -xzf ${checkm2_database} -C checkm2_db
    db=\$(find -L checkm2_db -type f -name '*.dmnd' -print -quit)
    test -n "\$db"

    for bin in input_fasta/*.fa; do
        stem=\$(basename "\${bin%.fa}")
        echo "Calling genes in \$stem" >&2
        prodigal -i "\$bin" -a "protein_fasta/\${stem}.faa" -p meta -q
    done

    checkm2 predict \\
        --genes \\
        --input protein_fasta/*.faa \\
        --output-directory '${batch_id}' \\
        --threads ${task.cpus} \\
        --database_path "\$db"

    cp '${batch_id}/quality_report.tsv' '${batch_id}_checkm2_report.tsv'
    """

    stub:
    """
    printf 'Name\tCompleteness\tContamination\n' > '${batch_id}_checkm2_report.tsv'
    """
}

process MERGE_CHECKM2_REPORTS {
    tag 'merge CheckM2 summaries'
    label 'process_single'

    publishDir "${params.outdir}", mode: 'copy', overwrite: true

    input:
    path reports

    output:
    path 'checkm2_summary.tsv', emit: summary

    script:
    """
    awk 'FNR == 1 && NR != 1 { next } { print }' ${reports} > checkm2_summary.tsv
    """

    stub:
    """
    printf 'Name\tCompleteness\tContamination\n' > checkm2_summary.tsv
    """
}

workflow {
    serial_batch_ids = params.serial_gene_batches.tokenize(',') as Set

    batches = Channel
        .fromPath(params.manifest, checkIfExists: true)
        .splitCsv(header: true, sep: '\t')
        .map { row -> tuple(row.batch_id, file(row.uri)) }
        .groupTuple()
        .map { batch_id, batch_bins -> tuple(batch_id, batch_bins.sort { a, b -> a.name <=> b.name }) }
        .branch { batch_id, batch_bins ->
            serial: serial_batch_ids.contains(batch_id)
            standard: !serial_batch_ids.contains(batch_id)
        }

    DOWNLOAD_CHECKM2_DATABASE(params.checkm2_database)
    CHECKM2_QC(
        batches.standard,
        DOWNLOAD_CHECKM2_DATABASE.out.database
    )
    CHECKM2_QC_SERIAL_GENES(
        batches.serial,
        DOWNLOAD_CHECKM2_DATABASE.out.database
    )

    batch_reports = CHECKM2_QC.out.report.mix(CHECKM2_QC_SERIAL_GENES.out.report)
    reports = batch_reports
        .map { batch_id, report -> report }
        .collect()
    MERGE_CHECKM2_REPORTS(reports)

    emit:
    checkm2_summary = MERGE_CHECKM2_REPORTS.out.summary
    batch_reports = batch_reports
}
