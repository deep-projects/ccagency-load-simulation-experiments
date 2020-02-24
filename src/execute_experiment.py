import argparse
import copy
import json
import os
import subprocess
import tempfile
import time
from collections import defaultdict
from getpass import getpass

import requests
from ruamel.yaml import YAML

TEMPLATE_PATH = 'experiment_templates/echo_template.red'

EXECUTED_EXPERIMENTS_DIR = 'executed_experiments'
DEFAULT_NUM_BATCHES = 10000

yaml = YAML(typ='safe')
yaml.default_flow_style = False


class AuthenticationInfo:
    def __init__(self, hostname, username, password):
        self.hostname = hostname
        self.username = username
        self.password = password

    @staticmethod
    def agency_from_user_input():
        hostname = input('agency url: ')
        username = input('agency username: ')
        password = getpass('agency password: ')

        return AuthenticationInfo(hostname, username, password)


def set_authentication_info(data, agency_auth_info):
    data['execution']['settings']['access']['url'] = agency_auth_info.hostname
    data['execution']['settings']['access']['auth']['username'] = agency_auth_info.username
    data['execution']['settings']['access']['auth']['password'] = agency_auth_info.password


def multiply_batches(template_data, num_batches):
    first_batch = template_data['batches'][0]
    new_batches = []
    for i in range(num_batches):
        new_batch = copy.deepcopy(first_batch)
        new_batches.append(new_batch)

    template_data['batches'] = new_batches


def get_arguments():
    parser = argparse.ArgumentParser(description='Executes test experiments on a cc-agency.')

    parser.add_argument(
        '--num-batches', type=int, default=DEFAULT_NUM_BATCHES, help='The template RED file whose batch is multiplied'
    )

    return parser.parse_args()


def execute_experiment(data):
    with tempfile.NamedTemporaryFile(mode='w') as execution_file:
        json.dump(data, execution_file)

        execution_file.flush()

        execution_result = subprocess.run(
            ['faice', 'exec', execution_file.name, '--debug', '--disable-retry'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )  # type: subprocess.CompletedProcess

    try:
        return yaml.load(execution_result.stdout)['response']['experimentId']
    except Exception as e:
        print('failed to execute experiment. faice stdout: {}'.format(execution_result.stdout))
        raise e


def dump_experiment_info(experiment_id):
    print('executing experiment {}'.format(experiment_id))

    info_data = {
        'experimentId': experiment_id,
    }

    dump_path = os.path.join(EXECUTED_EXPERIMENTS_DIR, experiment_id + '.json')

    with open(dump_path, 'w') as experiment_info_file:
        json.dump(info_data, experiment_info_file)


def get_batches(agency, username, pw, experiment_id):
    url = '{}?experimentId={}'.format(os.path.join(agency, 'batches'), experiment_id)
    resp = requests.get(url, auth=(username, pw))

    batches = list(filter(lambda b: b['experimentId'] == experiment_id, resp.json()))

    return batches


def get_state_dict(batches):
    state_dict = defaultdict(lambda: 0)
    for batch in batches:
        state_dict[batch['state']] += 1
    return dict(state_dict)


def check_finished(state_dict):
    return all(map(lambda k: k in ['succeeded', 'failed', 'cancelled'], state_dict.keys()))


def run_while_working(agency, experiment_id, username, pw, verbose=False):
    while True:
        batches = get_batches(agency, username, pw, experiment_id)
        state_dict = get_state_dict(batches)

        if check_finished(state_dict):
            if verbose:
                print('{: <100}'.format(str(state_dict)), flush=True)
            return state_dict
        elif verbose:
            print('{: <100}'.format(str(state_dict)), end='\r', flush=True)

        time.sleep(2)


def main():
    args = get_arguments()

    agency_auth_info = AuthenticationInfo.agency_from_user_input()

    if not os.path.isdir(EXECUTED_EXPERIMENTS_DIR):
        os.mkdir(EXECUTED_EXPERIMENTS_DIR)

    with open(TEMPLATE_PATH, 'r') as experiment_template:
        experiment_data = yaml.load(experiment_template)

    set_authentication_info(experiment_data, agency_auth_info)

    multiply_batches(experiment_data, args.num_batches)

    experiment_id = execute_experiment(experiment_data)

    dump_experiment_info(experiment_id)

    run_while_working(
        agency_auth_info.hostname, experiment_id, agency_auth_info.username, agency_auth_info.password, verbose=True
    )


if __name__ == '__main__':
    main()
