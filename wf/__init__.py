"""
Assemble and sort some COVID reads...
"""

import subprocess
from pathlib import Path

from latch import medium_task, small_task, workflow
from latch.types import LatchFile

# Kaiju index path
KAIJU_IDX = "/root/reference/"


@medium_task
def taxonomy_classification_task(read1: LatchFile, read2: LatchFile) -> LatchFile:
    global KAIJU_IDX
    # A reference to our output.
    output_name = "virus_kaiju.out"
    kaiju_out = Path(output_name).resolve()

    _kaiju_cmd = [
        "kaiju",
        "-t",
        f"{KAIJU_IDX}nodes.dmp",
        "-f",
        f"{KAIJU_IDX}kaiju_db_viruses.fmi",
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

    return LatchFile(str(kaiju_out), f"latch:///{output_name}")


@small_task
def kaiju2krona_task(kaiju_out: LatchFile) -> LatchFile:
    global KAIJU_IDX
    output_name = "virus_kaiju2krona.out"
    krona_txt = Path(output_name).resolve()

    _kaiju2krona_cmd = [
        "kaiju2krona",
        "-t",
        f"{KAIJU_IDX}nodes.dmp",
        "-n",
        f"{KAIJU_IDX}names.dmp",
        "-i",
        kaiju_out.local_path,
        "-o",
        str(krona_txt),
    ]

    subprocess.run(_kaiju2krona_cmd)

    return LatchFile(str(krona_txt), f"latch:///{output_name}")


@small_task
def plot_krona_task(krona_txt: LatchFile) -> LatchFile:
    output_name = "virus_krona.html"
    krona_html = Path(output_name).resolve()

    _kaiju2krona_cmd = ["ktImportText", "-o", str(krona_txt), krona_txt.local_path]

    subprocess.run(_kaiju2krona_cmd)

    return LatchFile(str(krona_html), f"latch:///{output_name}")


@workflow
def classify_viruses(read1: LatchFile, read2: LatchFile) -> LatchFile:
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
    kaiju_out = taxonomy_classification_task(read1=read1, read2=read2)
    kaiju2krona_out = kaiju2krona_task(kaiju_out=kaiju_out)
    return plot_krona_task(krona_txt=kaiju2krona_out)
