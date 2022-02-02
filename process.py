import glob
import json
import os
import pandas as pd

import warnings

warnings.filterwarnings('ignore')


def get_latest_filename(folder: str, filetype: str):
    list_of_files = glob.glob(
        f'{folder}/*{filetype}'
    )  # * means all if need specific format then *.csv
    return max(list_of_files, key=os.path.getctime)


class Process:

    def __init__(self, date_column: str):
        self.date_column = date_column

    def process(self, df: pd.DataFrame, columns: list):
        filtered = df[columns]
        filtered[self.date_column] = pd.to_datetime(filtered[self.date_column],
                                                    format='%m/%d/%Y')

        return filtered

    def get_min_max_dates(self, df: pd.DataFrame):
        min_date = df[self.date_column].min()
        max_date = df[self.date_column].max()

        return min_date, max_date

    def write_to_csv(self, df: pd.DataFrame, file_loc: str, min_date,
                     max_date):
        filename = f'{file_loc}_{min_date}_{max_date}.csv'
        df.to_csv(filename, index=False)


class Santander(Process):

    def __init__(self, date_column: str):
        Process.__init__(self, date_column)

    def main(self):
        santander_filename = get_latest_filename(folder='data/raw',
                                                 filetype='.csv')
        santander_raw = pd.read_csv(santander_filename, skiprows=3)
        santander_filtered = self.process(
            df=santander_raw, columns=['Date', 'Description', 'Amount'])
        min_date, max_date = self.get_min_max_dates(df=santander_filtered)
        self.write_to_csv(df=santander_filtered,
                          file_loc='data/cleaned/santander',
                          min_date=min_date.date(),
                          max_date=max_date.date())


class Landsbankinn(Process):

    def __init__(self, date_column: str):
        Process.__init__(self, date_column)
        self.latest_filename = get_latest_filename(folder='data/raw',
                                                   filetype='.xlsx')

    def run(self, columns, skiprows, type):
        landsbankinn_raw = pd.read_excel(self.latest_filename,
                                         skiprows=skiprows)
        landsbankinn_processed = self.process(df=landsbankinn_raw,
                                              columns=columns)
        min_date, max_date = self.get_min_max_dates(landsbankinn_processed)
        if type == 'debit':
            landsbankinn_processed['Description'] = landsbankinn_processed['Skýring'] + ': ' + landsbankinn_processed['Texti']
            landsbankinn_processed.drop(['Skýring', 'Texti'], axis=1, inplace=True)
        with open('config.json') as f:
            config = json.load(f)
        landsbankinn_processed.rename(columns=config['columns'])
        print(landsbankinn_processed)
        self.write_to_csv(df=landsbankinn_processed,
                          file_loc=f'data/cleaned/landsbankinn_{type}',
                          min_date=min_date.date(),
                          max_date=max_date.date())

    def debit(self):
        self.run(columns=['Dagsetning', 'Skýring', 'Texti', 'Upphæð'],
                 skiprows=0,
                 type='debit')

    def credit(self):
        self.run(
            columns=['Dagsetning', 'Söluaðili eða skýring', 'Upphæð(ISK)'],
            skiprows=3,
            type='credit')


if __name__ == '__main__':
    # santander_ = Santander(date_column='Date')
    # santander_.main()
    landsbankinn_ = Landsbankinn(date_column='Dagsetning')
    landsbankinn_.debit()
    # landsbankinn_.credit()
