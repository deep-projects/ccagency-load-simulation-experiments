import json
import os
from threading import Lock

import pandas as pd
import requests
from multiprocessing.pool import ThreadPool

from execute_experiments import AuthenticationInfo, EXECUTED_EXPERIMENTS_DIR, get_state_dict

CACHE_DIRECTORY = 'cache'
RESULTS_PATH = 'results'
RESULT_CSV_PATH = os.path.join(RESULTS_PATH, 'processing_timestamps.csv')
BAR_WIDTH = 70
PROCESSING_DURATION_CSV_PATH = os.path.join(RESULTS_PATH, 'processing_durations.csv')
SUCCESS_RATE_CSV_PATH = os.path.join(RESULTS_PATH, 'success_rate.csv')

PROCESSING_DURATION_LABEL = 'processing duration in seconds'
SCHEDULING_DURATION_LABEL = 'scheduling duration in seconds'
TIME_REGISTERED_LABEL = 'timestamp_registered'
TIME_SCHEDULED_LABEL = 'timestamp_scheduled'
TIME_PROCESSING_LABEL = 'timestamp_processing'
TIME_SUCCEEDED_LABEL = 'timestamp_succeeded'
EXPERIMENT_ID_LABEL = 'experiment_id'


def get_experiment_ids_from_executed_experiments():
    experiment_ids = []
    for filename in os.listdir(EXECUTED_EXPERIMENTS_DIR):
        if os.path.isfile(os.path.join(EXECUTED_EXPERIMENTS_DIR, filename)):
            experiment_ids.append(os.path.splitext(filename)[0])

    return experiment_ids


def get_detailed_result_with_cache(agency, experiment_id, username, pw):
    if not os.path.isdir(CACHE_DIRECTORY):
        os.mkdir(CACHE_DIRECTORY)

    cache_filename = os.path.join(CACHE_DIRECTORY, 'result_{}.json'.format(experiment_id))
    if os.path.isfile(cache_filename):
        with open(cache_filename, 'r') as cache_file:
            return json.load(cache_file)
    else:
        detailed_result = get_detailed_result(agency, experiment_id, username, pw)

        with open(cache_filename, 'w') as cache_file:
            json.dump(detailed_result, cache_file)

        return detailed_result


def get_batches(agency, username, pw, experiment_id):
    url = '{}?experimentId={}'.format(os.path.join(agency, 'batches'), experiment_id)
    resp = requests.get(url, auth=(username, pw))

    batches = list(filter(lambda b: b['experimentId'] == experiment_id, resp.json()))

    return batches


class BatchFetcher:
    def __init__(self, agency, username, password, num_batches, experiment_id=None):
        self.agency = agency
        self.username = username
        self.password = password
        self.num_batches = num_batches
        self.lock = Lock()
        self.counter = 0
        self.experiment_id = experiment_id

    def __call__(self, batch):
        result = requests.get(
            os.path.join(self.agency, 'batches', batch['_id']),
            auth=(self.username, self.password)
        ).json()
        with self.lock:
            self.counter += 1

        percentage = self.counter / self.num_batches

        format_string = 'fetching {}: [{{:<{}}}/{{:<{}}}][{{:-<{}}}]'.format(
            self.experiment_id or 'batches',
            len(str(self.num_batches)),
            len(str(self.num_batches)),
            BAR_WIDTH
        )

        print(
            format_string.format(self.counter, self.num_batches, '#' * int(percentage * BAR_WIDTH)),
            end='\n' if self.counter == self.num_batches else '\r',
            flush=True
        )
        return result


def fetch_batches(batches, agency, username, pw, experiment_id=None):
    with ThreadPool(5) as p:
        batch_list = list(p.map(BatchFetcher(agency, username, pw, len(batches), experiment_id), batches))

    return batch_list


def get_total_time(batch_list):
    start_time = batch_list[0]['history'][0]['time']
    end_time = start_time
    for history in batch_list:
        history_start_time = min(history['history'], key=lambda he: he['time'])['time']
        if history_start_time < start_time:
            start_time = history_start_time

        history_end_time = max(history['history'], key=lambda he: he['time'])['time']
        if history_end_time > end_time:
            end_time = history_end_time

    return end_time - start_time


def get_detailed_result(agency, experiment_id, username, pw):
    batches = get_batches(agency, username, pw, experiment_id)

    state_dict = get_state_dict(batches)

    cache_filename = '{}/{}.json'.format(CACHE_DIRECTORY, experiment_id)
    if os.path.isfile(cache_filename):
        # read cache
        print('reading {} from cache'.format(experiment_id), flush=True)
        with open(cache_filename, 'r') as cache_file:
            batch_list = json.load(cache_file)
    else:
        batch_list = fetch_batches(batches, agency, username, pw, experiment_id)

        # create cache
        if not os.path.isdir('cache'):
            os.mkdir('cache')

        with open(cache_filename, 'w') as cache_file:
            json.dump(batch_list, cache_file)

    batch_histories = []
    batch_states = []
    mount = False
    for batch in batch_list:
        if 'mount' in batch:
            mount = batch['mount']

        batch_states.append(batch['state'])

        if batch['history']:
            batch_history = []
            for history_entry in batch['history']:
                batch_history.append({'state': history_entry['state'], 'time': history_entry['time']})
            batch_histories.append({'history': batch_history, 'node': batch['node']})

    return {
        'experimentId': experiment_id,
        'states': state_dict,
        'batchStates': batch_states,
        'batchHistories': batch_histories,
        'totalTime': get_total_time(batch_list),
        'mount': mount,
    }


def get_state_duration(history, state):
    begin_time = None
    for history_entry in history:
        if state == history_entry['state']:
            begin_time = history_entry['time']

    if begin_time is None:
        raise ValueError('Could not find time of state "{}"'.format(state))

    next_time = min(filter(lambda he: he['time'] > begin_time, history), key=lambda he: he['time'])['time']
    return next_time - begin_time


class BatchToStateDuration:
    def __init__(self, state):
        self.state = state

    def __call__(self, batch):
        try:
            return get_state_duration(batch['history'], self.state)
        except ValueError:
            return 0


def get_state_timestamp_from_history(history, state):
    history_entries = list(filter(lambda history_entry: history_entry['state'] == state, history['history']))
    if len(history_entries) != 1:
        raise ValueError('Found state {} {} times'.format(state, len(history_entries)))
    return history_entries[0]['time']


def get_state_timestamps(batch_list, state):
    return list(
        map(lambda batch_history: get_state_timestamp_from_history(batch_history, state), batch_list['batchHistories'])
    )


def get_state_durations(batch_list, state):
    return list(map(BatchToStateDuration(state), batch_list))


def detailed_results_to_data_frame(detailed_results):
    data = {
        EXPERIMENT_ID_LABEL: [],
        TIME_REGISTERED_LABEL: [],
        TIME_SCHEDULED_LABEL: [],
        TIME_PROCESSING_LABEL: [],
        TIME_SUCCEEDED_LABEL: [],
    }

    for experiment_id, detailed_result in detailed_results.items():
        num_batches = len(detailed_result['batchHistories'])

        registered_timestamps = get_state_timestamps(detailed_result, 'registered')
        scheduled_timestamps = get_state_timestamps(detailed_result, 'scheduled')
        processing_timestamps = get_state_timestamps(detailed_result, 'processing')
        succeeded_timestamps = get_state_timestamps(detailed_result, 'succeeded')

        assert(len(registered_timestamps) == num_batches)
        assert(len(scheduled_timestamps) == num_batches)
        assert(len(processing_timestamps) == num_batches)
        assert(len(succeeded_timestamps) == num_batches)

        data[EXPERIMENT_ID_LABEL].extend([experiment_id] * num_batches)
        data[TIME_REGISTERED_LABEL].extend(registered_timestamps)
        data[TIME_SCHEDULED_LABEL].extend(scheduled_timestamps)
        data[TIME_PROCESSING_LABEL].extend(processing_timestamps)
        data[TIME_SUCCEEDED_LABEL].extend(succeeded_timestamps)

    return pd.DataFrame(data=data)


def detailed_results_to_processing_time_data_frame(detailed_results):
    data = {
        'experimentId': [],
        SCHEDULING_DURATION_LABEL: [],
        PROCESSING_DURATION_LABEL: [],
        'states': []
    }

    for experiment_id, detailed_result in detailed_results.items():
        num_batches = len(detailed_result['batchHistories'])

        try:
            scheduled_durations = get_state_durations(detailed_result['batchHistories'], 'scheduled')
            processing_durations = get_state_durations(detailed_result['batchHistories'], 'processing')
        except ValueError:
            raise ValueError('Failed to get durations for experiment "{}"'.format(experiment_id))

        assert(len(processing_durations) == num_batches)
        assert(len(scheduled_durations) == num_batches)

        data['experimentId'].extend([experiment_id] * num_batches)
        data[SCHEDULING_DURATION_LABEL].extend(scheduled_durations)
        data[PROCESSING_DURATION_LABEL].extend(processing_durations)
        data['states'].extend(detailed_result['batchStates'])

    return pd.DataFrame(data=data)


def main():
    agency_auth_info = AuthenticationInfo.agency_from_user_input()

    if not os.path.isdir(RESULTS_PATH):
        os.mkdir(RESULTS_PATH)

    detailed_results = {}
    for experiment_id in get_experiment_ids_from_executed_experiments():
        detailed_results[experiment_id] = get_detailed_result_with_cache(
            agency_auth_info.hostname, experiment_id, agency_auth_info.username, agency_auth_info.password
        )

    times_df = detailed_results_to_data_frame(detailed_results)

    times_df.to_csv(RESULT_CSV_PATH)


if __name__ == '__main__':
    main()
