import pandas as pd

from create_csv import RESULT_CSV_PATH


def main():
    data_frame = pd.read_csv(RESULT_CSV_PATH, index_col=0)
    print(data_frame.describe())


if __name__ == '__main__':
    main()
