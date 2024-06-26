# Certificate Transparency data

To handle duplicate extraction under memory constraints, we can utilize an [external sorting](https://en.wikipedia.org/wiki/External_sorting) approach. We'll read the file in chunks, sort the chunks, write them to disk, and then use a min-heap (priority queue) to merge them back into our output file.

In `external_sort.py`, each chunk is processed sequentially, with a chunk size of (roughly) 1 GiB (roughly because we need to ensure each chunk ends on a newline). For some performance improvements, `external_sort_parallel.py` uses multiprocessing to do this step in parallel, with chunk sizes of (1 GiB / <num_processors>) to keep memory usage consistent between both scripts. The merge step remains the same.

I also included a notebook with an attempt at the analysis. While the above scripts used only the standard library, the notebook requires some dependencies if you plan on running it yourself. You can `pip install -r requirements.txt` in a virtual environment, or if you end up having some conflicts with the package versions:

```sh
pip install editdistance tldextract polars duckdb black isort jupyter-black jupyterlab pyarrow
```
