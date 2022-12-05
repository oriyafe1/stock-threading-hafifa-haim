import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas


def get_bitcoin_dates():
    bitcoin_dates_path = os.getenv('BITCOIN_DATES')

    bitcoin_dates_file = open(bitcoin_dates_path, "r")

    bitcoin_dates_file_content = bitcoin_dates_file.read()
    bitcoin_date_strings = bitcoin_dates_file_content[3:].split('\n')[::-1]

    return [datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f') for date_string in
            bitcoin_date_strings]


def percentage_diff(v1, v2):
    return ((v1 - v2) / v1) * 100


def save_bitcoin():
    bitcoin_dates_input = get_bitcoin_dates()
    bitcoin_data = yf.download('BTC-USD', start=bitcoin_dates_input[0],
                               end=bitcoin_dates_input[-1] + timedelta(hours=1),
                               interval="1h")

    bitcoin_hour_dates = [date.strftime('%Y-%m-%d %H:00:00') for date in bitcoin_dates_input]
    relevant_bitcoin_data = bitcoin_data.loc[bitcoin_hour_dates]
    bitcoin_dates_chg_data = []

    for bitcoin_data_row_date, bitcoin_data_row in relevant_bitcoin_data.iterrows():
        row_chg = percentage_diff(bitcoin_data_row['Open'], bitcoin_data_row['Close'])
        bitcoin_dates_chg_data.append(
            pandas.DataFrame(data={"Chg": row_chg},
                             index=pandas.MultiIndex.from_tuples([(bitcoin_data_row_date, 'BTC-USD')],
                                                                 names=["Date", "Stock"])))

    bbb = pandas.concat(bitcoin_dates_chg_data)
    bitcoin_data.to_csv('out.csv')
    bbb.to_csv('chg.csv')


def main():
    save_bitcoin()


if __name__ == '__main__':
    main()
