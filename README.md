# ccagency-load-simulation-experiments

This repository contains programs that can be used to execute a series of experiments on the RED execution engine CC-Agency.
The results can be used to estimate the performance of the agency installation.

These experiments start a docker container which executes `echo hello`.

The results of previously executed experiments are summarized in CSV files located under the `results/` directory.
The python programs to execute, fetch and plot experiments are located under the `src/` directory.
The directory `experiment_templates/` contains a [RED](https://www.curious-containers.cc/docs/red-format) file that can be used as template for an experiment.
