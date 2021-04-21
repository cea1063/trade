# coding: utf-8
from DataMngr.DataManager import DataManager
from Util.Date import *
from Util.Constant import *
from DataMngr.SQL_interface import *
import sys

sys.path.append("..")

file_name = __file__.split('/')[-1]


class PriceAnalyzer:
    def __init__(self):
        self.dm = DataManager()

    @time_check
    def find_high_price_stock(self, period, percent, save=False):
        company_df = self.dm.company_df

        # 종목명, 코드, 시작날짜, 마지막날짜, 시가, 종가, 상승액, 상승률
        columns = [NAME, CODE, D_START, D_END, P_START, CLOSE, P_INC, P_PCT]
        result_df = pd.DataFrame(columns=columns)
        for index in company_df.index:
            stock = company_df.loc[index]
            df = get_df_from_table(STOCK_DB, stock[TABLE_NAME])
            df.set_index(keys=[DAY], inplace=True)
            if len(df.index) < period:
                continue
            df = df.tail(period)
            df.reset_index(inplace=True)
            start_price = df.loc[0, CLOSE]
            last_price = df.loc[period-1, CLOSE]
            diff = last_price - start_price
            diff_percent = round(diff * 1.0 / start_price * 100, 2)

            if diff_percent > percent:
                data = [stock[NAME], stock[CODE], df.loc[0, DAY], df.loc[period-1, DAY],
                        df.loc[0, CLOSE], df.loc[period-1, CLOSE], diff, diff_percent]
                result_df = result_df.append(pd.DataFrame([data], columns=columns), ignore_index=True)

        if save:
            result_df.to_csv('{}_{}_{}.csv'.format(self.dm.dot_date, period, percent))

        return result_df

if __name__ == '__main__':
    print(file_name)