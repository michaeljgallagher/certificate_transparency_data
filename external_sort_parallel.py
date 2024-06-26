import heapq
import json
import multiprocessing as mp
import os
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List


def sort_chunk(chunk: List[str]) -> str:
    """
    Sort the chunk by fingerprint.

    Args:
        chunk (List[str]): A list of JSON lines

    Returns:
        str: Path of sorted chunk
    """
    chunk = [json.loads(line) for line in chunk]
    chunk.sort(key=lambda x: x["data"]["leaf_cert"]["fingerprint"])
    with NamedTemporaryFile(mode="w+", delete=False) as temp_f:
        for line in chunk:
            json.dump(line, temp_f)
            temp_f.write("\n")
        return temp_f.name


def chunk_and_sort_parallel(
    input_file: str, chunk_size: int, num_processes: int
) -> List[str]:
    """
    Read file in chunks and sort each chunk by fingerprint in parallel.
    Return the paths to the sorted chunks.

    Args:
        input_file (str): Path to input file
        chunk_size (int): Size (in bytes) to read in for each chunk
        num_processes (int): The number of parallel processes to use

    Returns:
        List[str]: A list of paths to the sorted chunks
    """
    sorted_chunks = []
    with open(input_file, "r") as f:
        with mp.Pool(processes=num_processes) as pool:
            while chunks := list(
                filter(None, [f.readlines(chunk_size) for _ in range(num_processes)])
            ):
                results = pool.map(sort_chunk, chunks)
                sorted_chunks.extend(results)
    return sorted_chunks


def merge_sorted_chunks(sorted_chunks: List[str], output_file: str) -> None:
    """
    Merge sorted chunks into a single file.
    Only write out the duplicate fingerprints, with each line of the form:
    {"fingerprint": <fingerprint>, "certificates": [A, B, C, ...]}

    Args:
        sorted_chunks List[str]: A list of paths to the sorted chunks
        output_file (str): Path to output file

    Returns:
        None
    """
    with open(output_file, "w") as out:
        # Open all sorted files in a list
        open_files = [open(f, "r") for f in sorted_chunks]

        # Min-heap with the first line of each file
        # (fingerprint, idx, jsondata)
        heap = []
        for i, f in enumerate(open_files):
            line = f.readline()
            if line:
                cert_log = json.loads(line)
                heapq.heappush(
                    heap, (cert_log["data"]["leaf_cert"]["fingerprint"], i, cert_log)
                )

        # Pop from the heap, and push another line from the same file
        # Keep track of current fingerprint and list of certificate logs
        # When the fingerprint changes, write to output if len(certificates) > 1
        fingerprint = None
        certificates = []
        while heap:
            cur, i, cert_log = heapq.heappop(heap)
            if cur != fingerprint:
                if len(certificates) > 1:
                    json.dump(
                        {
                            "fingerprint": fingerprint,
                            "certificates": certificates,
                        },
                        out,
                    )
                    out.write("\n")
                fingerprint = cur
                certificates = []
            certificates.append(cert_log)
            nxt = open_files[i].readline()
            if nxt:
                nxt_cert_log = json.loads(nxt)
                heapq.heappush(
                    heap,
                    (nxt_cert_log["data"]["leaf_cert"]["fingerprint"], i, nxt_cert_log),
                )

        # Process remaining data
        if len(certificates) > 1:
            json.dump(
                {
                    "fingerprint": fingerprint,
                    "certificates": certificates,
                },
                out,
            )
            out.write("\n")

        # Clean up
        for f in open_files:
            f.close()
            os.unlink(f.name)


if __name__ == "__main__":
    # Adjust as needed
    input_file = "ctl_records_sample.jsonlines"
    output_file = "duplicates.jsonlines"
    num_processes = mp.cpu_count()
    chunk_size = (1 << 30) // num_processes

    print(
        f"{datetime.now().isoformat(timespec="seconds")} Splitting file into sorted chunks"
    )
    sorted_chunks = chunk_and_sort_parallel(input_file, chunk_size, num_processes)

    print(f"{datetime.now().isoformat(timespec="seconds")} Merging sorted chunks")
    merge_sorted_chunks(sorted_chunks, output_file)

    print(f"{datetime.now().isoformat(timespec="seconds")} Done!")
