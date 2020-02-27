import os

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from create_csv import RESULT_CSV_PATH, TIME_REGISTERED_LABEL, TIME_SUCCEEDED_LABEL, TIME_SCHEDULED_LABEL, \
    TIME_PROCESSING_LABEL, RESULTS_PATH

NUM_BATCHES_TIME_STEP = 4
NUM_BINS_NEW_BATCHES = 15
NUM_BATCHES_LABEL = 'number of batch state changes'
STATE_LABEL = 'state changes'

TIME_LABEL = 'one minute time bins'
NUM_SCHEDULED_LABEL = 'num batches scheduled'
NUM_REGISTERED_BATCHES_LABEL = 'number registered batches'
NUM_SCHEDULED_BATCHES_LABEL = 'number scheduled batches'
NUM_PROCESSING_BATCHES_LABEL = 'number processing batches'
NUM_SUCCEEDED_BATCHES_LABEL = 'number succeeded batches'

NUM_NEW_SCHEDULED_LABEL = 'from registered to scheduled'
NUM_NEW_PROCESSING_LABEL = 'from scheduled to processing'
NUM_NEW_SUCCEEDED_LABEL = 'from processing to succeeded'

NEXT_STATE_LABEL = {
    TIME_REGISTERED_LABEL: TIME_SCHEDULED_LABEL,
    TIME_SCHEDULED_LABEL: TIME_PROCESSING_LABEL,
    TIME_PROCESSING_LABEL: TIME_SUCCEEDED_LABEL,
    TIME_SUCCEEDED_LABEL: None
}


def get_batches_in_state(data_frame, time, state_label):
    if state_label is TIME_SUCCEEDED_LABEL:
        return data_frame[(data_frame[state_label] <= time)]

    next_state_label = NEXT_STATE_LABEL[state_label]

    return data_frame[(data_frame[state_label] <= time) & (data_frame[next_state_label] > time)]


def count_batches_in_state(data_frame, time, state_label):
    return len(get_batches_in_state(data_frame, time, state_label))


def count_new_batches_in_state(data_frame, start_time, end_time, state_label):
    new_batches_in_state = data_frame[(data_frame[state_label] >= start_time) & (data_frame[state_label] < end_time)]
    return len(new_batches_in_state)


def create_state_count_data_frame(data_frame):
    """
    Creates a pandas Dataframe that contains the number of batches for the states 'registered', 'scheduled',
    'processing' and 'succeeded'. Each value is present for multiple timestamps.

    :param data_frame: The dataframe to get data from
    :type data_frame: pd.DataFrame
    :return:
    """
    start_time = data_frame.min()[TIME_REGISTERED_LABEL]
    end_time = data_frame.max()[TIME_SUCCEEDED_LABEL]

    data = {
        TIME_LABEL: [],
        NUM_REGISTERED_BATCHES_LABEL: [],
        NUM_SCHEDULED_BATCHES_LABEL: [],
        NUM_PROCESSING_BATCHES_LABEL: [],
        NUM_SUCCEEDED_BATCHES_LABEL: []
    }

    for time in np.arange(start_time, end_time, NUM_BATCHES_TIME_STEP):
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


def create_state_change_df(data_frame):
    start_time = data_frame.min()[TIME_REGISTERED_LABEL]
    end_time = data_frame.max()[TIME_SUCCEEDED_LABEL]

    # round up end_time to a number divisible by NUM_BINS_NEW_BATCHES
    # end_time = (int(end_time) // NUM_BINS_NEW_BATCHES + 1) * NUM_BINS_NEW_BATCHES
    end_time = (int(end_time) // 60 + 1) * 60

    data = {
        TIME_LABEL: [],
        NUM_NEW_SCHEDULED_LABEL: [],
        NUM_NEW_PROCESSING_LABEL: [],
        NUM_NEW_SUCCEEDED_LABEL: []
    }

    # times_linspace = np.linspace(start_time, end_time, NUM_BINS_NEW_BATCHES + 1).astype(int)
    times_linspace = np.arange(start_time, end_time+1, 60).astype(int)

    bin_start_time = times_linspace[0]
    end_times = times_linspace[1:]
    for bin_end_time in end_times:
        data[TIME_LABEL].append(int(bin_end_time) // 60)

        new_scheduled_batch_count = count_new_batches_in_state(
            data_frame, bin_start_time, bin_end_time, TIME_SCHEDULED_LABEL
        )
        new_processing_batch_count = count_new_batches_in_state(
            data_frame, bin_start_time, bin_end_time, TIME_PROCESSING_LABEL
        )
        new_succeeded_batch_count = count_new_batches_in_state(
            data_frame, bin_start_time, bin_end_time, TIME_SUCCEEDED_LABEL
        )

        data[NUM_NEW_SCHEDULED_LABEL].append(new_scheduled_batch_count)
        data[NUM_NEW_PROCESSING_LABEL].append(new_processing_batch_count)
        data[NUM_NEW_SUCCEEDED_LABEL].append(new_succeeded_batch_count)

        bin_start_time = bin_end_time

    return pd.DataFrame(data=data)


def analyse_data_frame(data_frame):
    start_time = data_frame.min()[TIME_REGISTERED_LABEL]
    end_time = data_frame.max()[TIME_SUCCEEDED_LABEL]

    max_scheduled_batch_count = 0
    max_processing_batch_count = 0

    for time in np.arange(start_time, end_time, 0.1):
        scheduled_batch_count = count_batches_in_state(data_frame, time, TIME_SCHEDULED_LABEL)
        processing_batch_count = count_batches_in_state(data_frame, time, TIME_PROCESSING_LABEL)

        max_scheduled_batch_count = max(max_scheduled_batch_count, scheduled_batch_count)
        max_processing_batch_count = max(max_processing_batch_count, processing_batch_count)

    print('max scheduled batch count: {}'.format(max_scheduled_batch_count))
    print('max processing batch count: {}'.format(max_processing_batch_count))
    print('total duration: {:.2f} sec'.format(end_time - start_time))


def main():
    data_frame = pd.read_csv(RESULT_CSV_PATH, index_col=0)
    # analyse_data_frame(data_frame)
    state_count_df = create_state_count_data_frame(data_frame)
    new_state_count_df = create_state_change_df(data_frame)

    plot_state_count_df(state_count_df)
    plot_new_state_count(new_state_count_df)


def plot_state_count_df(state_count_df):
    fig, ax = plt.subplots(1, 1)

    df = state_count_df.melt(TIME_LABEL, var_name=STATE_LABEL, value_name=NUM_BATCHES_LABEL)

    sns.lineplot(
        x=TIME_LABEL,
        y=NUM_BATCHES_LABEL,
        hue=STATE_LABEL,
        data=df,
        ax=ax
    )
    plot_path = os.path.join(RESULTS_PATH, 'state_counts.pdf')
    fig.savefig(plot_path, bibox_inches='tight')


def plot_new_state_count(data_frame):
    fig, ax = plt.subplots(1, 1)

    df = data_frame.melt(TIME_LABEL, var_name=STATE_LABEL, value_name=NUM_BATCHES_LABEL)

    sns.barplot(
        x=TIME_LABEL,
        y=NUM_BATCHES_LABEL,
        hue=STATE_LABEL,
        data=df,
        ax=ax
    )
    plot_path = os.path.join(RESULTS_PATH, 'state_changes.pdf')
    fig.savefig(plot_path, bibox_inches='tight')


if __name__ == '__main__':
    main()
