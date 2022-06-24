"""
Taxonomic classification of reads
"""

import subprocess
from pathlib import Path
from typing import Tuple

from latch import small_task, workflow
from latch.types import LatchFile


@small_task
def taxonomy_classification_task(
    read1: LatchFile,
    read2: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_db: LatchFile,
    sample: str,
) -> LatchFile:
    """Classify metagenomic reads with Kaiju"""

    output_name = f"{sample}_kaiju.out"
    kaiju_out = Path(output_name).resolve()

    _kaiju_cmd = [
        "kaiju",
        "-t",
        kaiju_ref_nodes.local_path,
        "-f",
        kaiju_ref_db.local_path,
        "-i",
        read1.local_path,
        "-j",
        read2.local_path,
        "-z",
        "2",
        "-o",
        str(kaiju_out),
    ]

    subprocess.run(_kaiju_cmd)

    return LatchFile(str(kaiju_out), f"latch:///kaiju/{output_name}")


@small_task
def kaiju2krona_task(
    kaiju_out: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    sample: str,
) -> LatchFile:
    """Convert Kaiju output to Krona-readable format"""

    output_name = f"{sample}_kaiju2krona.out"
    krona_txt = Path(output_name).resolve()

    _kaiju2krona_cmd = [
        "kaiju2krona",
        "-t",
        kaiju_ref_nodes.local_path,
        "-n",
        kaiju_ref_names.local_path,
        "-i",
        kaiju_out.local_path,
        "-o",
        str(krona_txt),
    ]

    subprocess.run(_kaiju2krona_cmd)

    return LatchFile(str(krona_txt), f"latch:///kaiju/{output_name}")


@small_task
def kaiju2table_task(
    kaiju_out: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    sample: str,
    taxon: str,
) -> LatchFile:
    """Convert Kaiju output to TSV format"""

    output_name = f"{sample}_kaiju.tsv"
    kaijutable_tsv = Path(output_name).resolve()

    _kaiju2table_cmd = [
        "kaiju2table",
        "-t",
        kaiju_ref_nodes.local_path,
        "-n",
        kaiju_ref_names.local_path,
        "-r",
        taxon,
        "-p",
        "-e",
        "-o",
        str(kaijutable_tsv),
        kaiju_out.local_path,
    ]

    subprocess.run(_kaiju2table_cmd)

    return LatchFile(str(kaijutable_tsv), f"latch:///kaiju/{output_name}")


@small_task
def plot_krona_task(krona_txt: LatchFile, sample: str) -> LatchFile:
    """Make Krona plot from Kaiju results"""
    output_name = f"{sample}_krona.html"
    krona_html = Path(output_name).resolve()

    _kaiju2krona_cmd = ["ktImportText", "-o", str(krona_html), krona_txt.local_path]

    subprocess.run(_kaiju2krona_cmd)

    return LatchFile(str(krona_html), f"latch:///kaiju/{output_name}")


@workflow
def kaiju_classification(
    read1: LatchFile,
    read2: LatchFile,
    kaiju_ref_db: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    tax_rank: str = "species",
    sample_name: str = "kaiju_sample",
) -> Tuple[LatchFile, LatchFile]:
    """Fast taxonomic classification of high-throughput sequencing reads

    Kaiju
    -----

    # Taxonomic classification with Kaiju

    Kaiju performs taxonomic classification of
    whole-genome sequencing metagenomics reads.
    Reads are assigned to taxa by using a reference database
    of protein sequences.
    Read more about it [here](https://github.com/bioinformatics-centre/kaiju)

    Kaiju paper: https://doi.org/10.1038/ncomms11257

    __metadata__:
        display_name: Taxonomic classification with Kaiju
        author:
            name: João Vitor Ferreira Cavalcante
            email:
            github: https://github.com/jvfe/
        repository: https://github.com/jvfe/kaiju-latch
        license:
            id: MIT

    Args:

        sample_name:
          Input sample name.

          __metadata__:
            display_name: Sample name

        read1:
          Paired-end read 1 file.

          __metadata__:
            display_name: Read1
            _tmp:
                section_title: Whole-metagenomes sequencing reads

        read2:
          Paired-end read 2 file.

          __metadata__:
            display_name: Read2

        kaiju_ref_db:
          Kaiju reference database '.fmi' file.

          __metadata__:
            display_name: Kaiju reference database (FM-index)
            _tmp:
                section_title: Kaiju database files

        kaiju_ref_nodes:
          Kaiju reference nodes, 'nodes.dmp' file.

          __metadata__:
            display_name: Kaiju reference database nodes

        kaiju_ref_names:
          Kaiju reference taxon names, 'names.dmp' file.

          __metadata__:
            display_name: Kaiju reference database names

        tax_rank:
          Taxonomic rank for summary table output (kaiju2table).

          __metadata__:
            display_name: Taxonomic rank (kaiju2table)
            _tmp:
                section_title: Other parameters
    """
    kaiju_out = taxonomy_classification_task(
        read1=read1,
        read2=read2,
        kaiju_ref_db=kaiju_ref_db,
        kaiju_ref_nodes=kaiju_ref_nodes,
        sample=sample_name,
    )
    kaiju2krona_out = kaiju2krona_task(
        kaiju_out=kaiju_out,
        sample=sample_name,
        kaiju_ref_nodes=kaiju_ref_nodes,
        kaiju_ref_names=kaiju_ref_names,
    )
    krona_plot = plot_krona_task(krona_txt=kaiju2krona_out, sample=sample_name)
    kaiju2table_out = kaiju2table_task(
        kaiju_out=kaiju_out,
        sample=sample_name,
        kaiju_ref_nodes=kaiju_ref_nodes,
        kaiju_ref_names=kaiju_ref_names,
        taxon=tax_rank,
    )
    return kaiju2table_out, krona_plot
