# ccagency-load-simulation-experiments

This repository contains programs that can be used to execute an experiment on the RED execution engine CC-Agency and evaluate the results.
These results can be used to estimate the performance of the agency installation.

These experiments start a docker container which executes `echo hello`.

The results of previously executed experiments are summarized in CSV files located under the `results/` directory.
The python programs to execute, fetch and plot experiments are located under the `src/` directory.
The directory `experiment_templates/` contains a [RED](https://www.curious-containers.cc/docs/red-format) file that can be used as template for an experiment.

## Dependencies

The required software dependencies to execute the experiments are listed in `requirements.txt`. To install them execute `pip install -r requirements.txt`, possibly in a virtual environment.

To execute the experiments a running CC-Agency installation is required.
More information on how to setup CC-Agency can be found at the [Curious Containers Documentation](https://www.curious-containers.cc/docs/cc-agency-installation).


## Execution

### Execute experiments

To execute an experiment on your agency installation the program `src/execute_experiment.py` can be used.

```bash
python3 ./src/execute_experiment.py
```

In order to execute the experiments the *agency-url* of the agency on which the experiments are to be executed is asked, as well as the *agency-username* and corresponding *agency-password*.
The *agency-url* should look similar to `https://agency.example.de/cc`. You can test the *agency-url* be accessing it with you browser. This should return a hello world json object.

This will start 10.000 docker containers on the agency cluster. To reduce the amount of docker containers you can specify the amount with the `--num-batches` argument.

```bash
# start 100 docker containers
python3 ./src/execute_experiment.py --num-batches 100
```

#### Results

After executing the experiments there will be a `executed_experiments/` directory, that contains experiment meta information.

Make sure to remove this directory if you restart the experiment. Otherwise old experiments will be used for the following process.


### Fetch batch information

To create a compact representation of the executed experiment run the following program.

```
python3 ./src/create_csv.py
```

To fetch the experiment information the agency authentication information is requested again. This can also take some minutes.

The result of this program is a the csv file `results/processing_timestamps.csv`.
Before the program is executed, this csv file is already in the repository. It contains the results of a previously executed experiment and will be overwritten.


### Plot the results

To create a plot showing the result execute the program

```
python3 ./src/plot_results.py
```

This will create the files `state_changes.pdf` and `state_counts.pdf`.
