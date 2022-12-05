import os
import yfinance as yf
import pandas
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor


def get_dates_input(dates_input_file_path):
    dates_file = open(dates_input_file_path, "r")

    dates_file_content = dates_file.read()
    date_strings = dates_file_content[3:].split('\n')[::-1]

    return [pandas.to_datetime(date_string, format='%Y-%m-%d %H:%M:%S.%f').tz_localize('UTC') for date_string in
            date_strings]


def percentage_diff(v1, v2):
    return ((v1 - v2) / v1) * 100


def get_stock_chg_data(stock_name, dates_input_file_path):
    print(f'starting {stock_name}')
    dates_input = get_dates_input(dates_input_file_path)
    ticker = yf.Ticker(stock_name)

    stock_history_data = ticker.history(stock_name, start=dates_input[0] - timedelta(hours=1),
                                        end=dates_input[-1] + timedelta(hours=1),
                                        interval="1h")

    stock_hour_dates = stock_history_data.index[
        stock_history_data.index.get_indexer(dates_input, method='nearest')]
    relevant_stock_data = stock_history_data.loc[stock_hour_dates]
    stock_chg_data_list = []

    for stock_data_row_date, stock_data_row in relevant_stock_data.iterrows():
        row_chg = percentage_diff(stock_data_row['Open'], stock_data_row['Close'])
        stock_chg_data_list.append(
            pandas.DataFrame(data={"Chg": row_chg},
                             index=pandas.MultiIndex.from_tuples([(stock_data_row_date, stock_name)],
                                                                 names=["Date", "Stock"])))

    print(f'ended {stock_name}')

    return pandas.concat(stock_chg_data_list)


def main():
    stocks_and_dates_tuples = [('BTC-USD', os.getenv('BITCOIN_DATES')), ('GOOG', os.getenv('GOOGLE_DATES')),
                               ('AMZN', os.getenv('AMAZON_DATES'))]

    stocks_df_list = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for result in executor.map(lambda p: get_stock_chg_data(*p), stocks_and_dates_tuples):
            stocks_df_list.append(result)

    stocks_df = pandas.concat(stocks_df_list)
    stocks_df.to_csv(os.getenv('DESTINATION_FILE'))


if __name__ == '__main__':
    main()
