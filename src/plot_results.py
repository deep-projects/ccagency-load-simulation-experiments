import os

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from create_csv import RESULT_CSV_PATH, TIME_REGISTERED_LABEL, TIME_SUCCEEDED_LABEL, TIME_SCHEDULED_LABEL, \
    TIME_PROCESSING_LABEL, RESULTS_PATH

TIME_STEP = 4
TIME_LABEL = 'time'
NUM_SCHEDULED_LABEL = 'num batches scheduled'
NUM_REGISTERED_BATCHES_LABEL = 'number registered batches'
NUM_SCHEDULED_BATCHES_LABEL = 'number scheduled batches'
NUM_PROCESSING_BATCHES_LABEL = 'number processing batches'
NUM_SUCCEEDED_BATCHES_LABEL = 'number succeeded batches'


NEXT_STATE_LABEL = {
    TIME_REGISTERED_LABEL: TIME_SCHEDULED_LABEL,
    TIME_SCHEDULED_LABEL: TIME_PROCESSING_LABEL,
    TIME_PROCESSING_LABEL: TIME_SUCCEEDED_LABEL,
    TIME_SUCCEEDED_LABEL: None
}


def count_batches_in_state(data_frame, time, state_label):
    next_state_label = NEXT_STATE_LABEL[state_label]
    if next_state_label is None:
        return len(data_frame[(data_frame[state_label] <= time)])

    result = len(data_frame[(data_frame[state_label] <= time) & (data_frame[next_state_label] > time)])

    return result


def create_state_count_data_frame(data_frame):
    start_time = data_frame.min()[TIME_REGISTERED_LABEL]
    end_time = data_frame.max()[TIME_SUCCEEDED_LABEL]

    data = {
        TIME_LABEL: [],
        NUM_REGISTERED_BATCHES_LABEL: [],
        NUM_SCHEDULED_BATCHES_LABEL: [],
        NUM_PROCESSING_BATCHES_LABEL: [],
        NUM_SUCCEEDED_BATCHES_LABEL: []
    }

    for time in np.arange(start_time, end_time, TIME_STEP):
        data[TIME_LABEL].append(time)

        registered_batch_count = count_batches_in_state(data_frame, time, TIME_REGISTERED_LABEL)
        scheduled_batch_count = count_batches_in_state(data_frame, time, TIME_SCHEDULED_LABEL)
        processing_batch_count = count_batches_in_state(data_frame, time, TIME_PROCESSING_LABEL)
        succeeded_batch_count = count_batches_in_state(data_frame, time, TIME_SUCCEEDED_LABEL)

        data[NUM_REGISTERED_BATCHES_LABEL].append(registered_batch_count)
        data[NUM_SCHEDULED_BATCHES_LABEL].append(scheduled_batch_count)
        data[NUM_PROCESSING_BATCHES_LABEL].append(processing_batch_count)
        data[NUM_SUCCEEDED_BATCHES_LABEL].append(succeeded_batch_count)

    return pd.DataFrame(data=data)


def main():
    data_frame = pd.read_csv(RESULT_CSV_PATH, index_col=0)
    state_count_df = create_state_count_data_frame(data_frame)

    plot_state_count_df(state_count_df)


def plot_state_count_df(state_count_df):
    fig, ax = plt.subplots(1, 1)

    df = state_count_df.melt(TIME_LABEL, var_name='state', value_name='number of batches')

    sns.lineplot(
        x=TIME_LABEL,
        y='number of batches',
        hue='state',
        data=df,
        ax=ax
    )
    plot_path = os.path.join(RESULTS_PATH, 'state_counts.pdf')
    fig.savefig(plot_path, bibox_inches='tight')


if __name__ == '__main__':
    main()
