# Hybrid experiments

Experiments with cost optimization of hybrid clouds using
[Malloovia](https://github.com/asi-uniovi/malloovia/) and
[Simllovia](https://github.com/asi-uniovi/simlloovia/).

## Introduction

In order to execute the experiments, follow these steps:

1. Install the required packages with:

```shell
pip install -r requirements.txt
```

2. Clean output files with:

```shell
    snakemake clean -c1
```

3. Execute the experiments with:

```shell
    snakemake -j 1
```

4. Plot the results using the notebook [`PlotResults.ipynb`](PlotResults.ipynb).

You also can open the notebook without executing the experiments to see the
result of the execution.
