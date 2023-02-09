

from pathlib import Path

snakemake_dir = Path(workflow.snakefile).parent.resolve()

configfile:
    "/gpfs/home/rdkiesersi1/upload_genomes/config.yaml"


localrules: create_bach_tables
checkpoint create_bach_tables:
    input:
        config["genome_table"]
    output:
        directory("Batches")
    params:
        chunksize = 100
    run:
        import pandas as pd
        with pd.read_table(input[0], chunksize=params.chunksize,index_col=0) as reader:
            for i,chunk in enumerate(reader):

                batch_dir= Path(output[0])/f"batch{i:000d}"

                batch_dir.mkdir(parents=True, exist_ok=False)

                chunk.to_csv(batch_dir/"genome_table.tsv",sep='\t')




rule create_manifests_batch:
    input:
        table= "Batches/batch{batch}/genome_table.tsv",
        script= snakemake_dir/"scripts/genome_upload.py"
    output:
        directory("Batches/batch{batch}/MAG_upload")
    params:
        outdir= lambda wc, output: Path(output[0]).parent,
        extra= "-u {bioproject}  --centre_name '{center}' --webin '{webin_user}' --password '{webin_password}' ".format(**config),
        is_mag = "--mags" if bool(config["is_mag"]) else "--bins"
    log:
        "log/create_manifest/batch{batch}.log"
    shell:
        "python {input.script} "
        " --genome_info {input.table} "
        " --out {params.outdir} "
        " {params.extra} "
        " {params.is_mag} "
        "  --force "
        " --live "
        " &> {log} "



def get_manifest_input(wildcards):
    manifests_folder= Path(checkpoints.create_manifests_batch.get(**wildcards).output[0])/"manifests"

    return list(manifests_folder.iterdir())



rule validate:
    input:
        dir= "Batches/batch{batch}/MAG_upload",
        script= snakemake_dir/"scripts/webin-cli-5.2.0.jar"
    output:
        touch("Batches/batch{batch}/manifest_validated")
    params:
        outdir= lambda wc, output: Path(output[0]).parent,
        proxy = " -Dhttps.proxyHost=204.79.90.44 -Dhttps.proxyPort=8080",
        extra = " -username {webin_user} -password '{webin_password}' -centername '{center}' ".format(**config)
                
    log:
        "log/validate/batch{batch}.log"
    shell:
        " set -x \n"
        "(\n"
        " for manifest in {input.dir}/manifests/*.manifest ; \n "
        " do \n"
        "java {params.proxy} -jar {input.script} "
        " {params.extra} "
        " -context genome "
        " -manifest $manifest "
        " -outputdir {params.outdir} "
        " -validate "
        "\ndone\n"
        ") &> {log} "



"""

      -context=TYPE        Submission type: genome, transcriptome, sequence,
                             reads, taxrefset
      -manifest=FILE       Manifest text file containing file and metadata
                             fields.
      -userName, -username=USER
                           Webin submission account name or e-mail address.
      -password=PASSWORD   Webin submission account password.
      -passwordFile=FILE   File containing the Webin submission account
                             password.
      -passwordEnv=VAR     Environment variable containing the Webin submission
                             account password.
      -inputDir, -inputdir=DIRECTORY
                           Root directory for the files declared in the
                             manifest file. By default the current working
                             directory is used as the input directory.
      -outputDir, -outputdir=DIRECTORY
                           Root directory for any output files written in
                             <context>/<name>/<validate,process,submit>
                             directory structure. By default the manifest file
                             directory is used as the output directory. The
                             <name> is the unique name from the manifest file.
                             The validation reports are written in the
                             <validate> sub-directory.
      -centerName, -centername=CENTER
                           Mandatory center name for broker accounts.
      -validate            Validate files without uploading or submitting them.
      -submit              Validate, upload and submit files.
      -test                Use the test submission service.
      -ascp                Use Aspera (if Aspera Cli is available) instead of
                             FTP when uploading files. The path to the
                             installed "ascp" program must be in the PATH
                             variable.
      -help                Show this help message and exit.
      -fields              Show manifest fields for all contexts or for the
                             given -context.
      -version             Print version information and exit.
"""




def get_all_batches(wildcards):

    parent_dir = checkpoints.create_bach_tables.get().output[0]

    all_batches = glob_wildcards( parent_dir+ "/batch{batch}/genome_table.tsv").batch

    return all_batches

def get_all_manifests(wildcards):

    all_batches = get_all_batches(wildcards)

    return expand(rules.validate.output[0],batch=all_batches)



rule all_maifests:
    input:
        get_all_manifests