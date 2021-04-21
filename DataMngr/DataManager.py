# coding: utf-8
import sys
from .SQL_interface import *
from Util.Constant import *
from Util.Date import *
from .Crawler import Crawler

sys.path.append("..")

file_name = __file__.split('/')[-1]


class DataManager(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        cls = type(self)
        if not hasattr(cls, "_init"):
            self.crawler = Crawler()
            if market_off():
                self.date = get_today()
            else:
                self.date = get_latest_market_day()
            self.dot_date = get_dot_date(self.date)
            self.company_table_name = 'COMPANY_' + self.date.strftime('%Y%m%d')
            self.delete_old_company_info()
            stock_updated = False
            if not check_table_exist(STOCK_DB, self.company_table_name):
                self.insert_today_company_info()
                print('{} company info update'.format(self.company_table_name))
                stock_updated = True
            self.company_df = get_df_from_table(STOCK_DB, self.company_table_name)
            if stock_updated:
                self.update_price_db()
        else:
            print('instance already exist')

    # 'COMPANY_' + date Table을 제외하고 삭제
    def delete_old_company_info(self):
        table_data = get_all_table_name(STOCK_DB)
        table_list = [table[0] for table in table_data if table[0].find('COMPANY') >= 0]
        if self.company_table_name in table_list:
            table_list.remove(self.company_table_name)
        for table in table_list:
            delete_table(STOCK_DB, table)

    # insert today's company info
    @time_check
    def insert_today_company_info(self):
        kospi_dict = self.crawler.get_stock_dict(market='kospi')
        kosdaq_dict = self.crawler.get_stock_dict(market='kosdaq')
        print('Stock Dict OK')
        stock_dict = dict(kospi_dict, **kosdaq_dict)

        dict_df = pd.DataFrame(list(stock_dict.items()), columns=[CODE, NAME])

        kospi_df = self.crawler.make_company_info(0)
        kosdaq_df = self.crawler.make_company_info(1)

        stock_df = pd.concat([kospi_df, kosdaq_df], ignore_index=True)

        dict_df[TABLE_NAME] = ''
        for index in dict_df.index:
            name = dict_df.loc[index, NAME]
            code = dict_df.loc[index, CODE]
            if name[0].isdigit():
                name = 'KOS' + name
            table_name = name.replace(' ', '').replace('&', '').replace('-', '').replace('.', '')[:11] + str(code)
            dict_df.loc[index, TABLE_NAME] = table_name

        df = pd.merge(stock_df, dict_df, on=NAME, how='inner')

        df.sort_values(by=[CODE], axis=0, inplace=True)
        df.set_index(CODE, inplace=True)
        insert_df_to_db(STOCK_DB, self.company_table_name, df)

    # insert all company's price data to db
    def make_price_db(self):
        count = len(self.company_df.index)
        for i, index in enumerate(self.company_df.index):
            code = self.company_df.loc[index][CODE]
            table_name = self.company_df.loc[index][TABLE_NAME]
            if check_table_exist(STOCK_DB, table_name):
                print('{} already exist'.format(table_name))
                continue
            else:
                self.insert_all_price_data(code, table_name)
                print('{}/{} {} complete'.format(i+1, count, self.company_df.loc[index][NAME]))

    # insert price data to db
    def insert_all_price_data(self, code, table_name):
        df = self.crawler.get_all_daily_data(code)
        remove_date = [index for index in df.index if index > self.dot_date]
        df.drop(remove_date, inplace=True)
        df.sort_values(by=[DAY], inplace=True)
        insert_df_to_db(STOCK_DB, table_name, df)

    # update all company's price data by today
    @time_check
    def update_price_db(self):
        count = len(self.company_df.index)
        for i, index in enumerate(self.company_df.index):
            code = self.company_df.loc[index][CODE]
            table_name = self.company_df.loc[index][TABLE_NAME]
            self.update_price_data_by_today(code, table_name)
            print('{} / {} complete'.format(i, count))

    # update price data from last dat to today
    def update_price_data_by_today(self, code, table_name):
        try:
            last_date = get_last_updated_date(STOCK_DB, table_name)
        except Exception as e:
            print(table_name, ' not exist')
            self.insert_all_price_data(code, table_name)
            print('{} insert complete'.format(table_name))
            return

        df = self.crawler.get_stock_info_with_date(code, last_date, self.dot_date)
        if len(df.index) == 0:
            print('{} is already updated'.format(table_name))
        else:
            insert_df_to_db(STOCK_DB, table_name, df, option='append')


if __name__ == '__main__':
    print(file_name)
