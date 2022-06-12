"""
Taxonomic classification of reads
"""

import subprocess
from pathlib import Path

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

    # A reference to our output.
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

    # return LatchFile(str(kaiju_out), f"latch:///{output_name}")
    return LatchFile(str(kaiju_out))


@small_task
def kaiju2krona_task(
    kaiju_out: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    sample: str,
) -> LatchFile:

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

    # return LatchFile(str(krona_txt), f"latch:///{output_name}")
    return LatchFile(str(krona_txt))


@small_task
def plot_krona_task(krona_txt: LatchFile, sample: str) -> LatchFile:
    output_name = f"{sample}_krona.html"
    krona_html = Path(output_name).resolve()

    _kaiju2krona_cmd = ["ktImportText", "-o", str(krona_html), krona_txt.local_path]

    subprocess.run(_kaiju2krona_cmd)

    # return LatchFile(str(krona_html), f"latch:///{output_name}")
    return LatchFile(str(krona_html))


@workflow
def classify_viruses(
    read1: LatchFile,
    read2: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    kaiju_ref_db: LatchFile,
    sample_name: str = "kaiju_sample",
) -> LatchFile:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2

    __metadata__:
        display_name: Assemble and Sort FastQ Files
        author:
            name:
            email:
            github:
        repository:
        license:
            id: MIT

    Args:

        read1:
          Paired-end read 1 file to be assembled.

          __metadata__:
            display_name: Read1

        read2:
          Paired-end read 2 file to be assembled.

          __metadata__:
            display_name: Read2
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
    return plot_krona_task(krona_txt=kaiju2krona_out, sample=sample_name)


# if __name__ == "__main__":
#     classify_viruses(
#         read1=LatchFile("/root/reference/viruses_R1.fastq"),
#         read2=LatchFile("/root/reference/viruses_R2.fastq"),
#         kaiju_ref_db=LatchFile("/root/reference/kaiju_db_viruses.fmi"),
#         kaiju_ref_nodes=LatchFile("/root/reference/nodes.dmp"),
#         kaiju_ref_names=LatchFile("/root/reference/names.dmp"),
#     )
