# coding: utf-8
from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
from Util.Constant import *
import urllib.parse

file_name = __file__.split('/')[-1]


class Crawler:
    def __init__(self):
        self.fields = []

    def get_stock_info_with_date(self, code, start, end):
        columns = ['날짜', '종가', '전일비', '시가', '고가', '저가', '거래량']
        df = pd.DataFrame(columns=columns)

        # for page in range(1, pageNum + 1):
        done = False
        for page in range(1, 10):
            url = 'https://finance.naver.com/item/sise_day.nhn?code=' + str(code) + '&page=' + str(page)
            html = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
            source = BeautifulSoup(html.text, 'lxml')
            slists = source.find_all('tr', {'onmouseover': 'mouseOver(this)'})
            for tr in slists:
                data = []
                date = tr.find('td', {'align': 'center'}).text
                if len(date) < 5:
                    break

                if date > end:
                    continue
                data.append(date)
                num_list = tr.find_all('td', {'class': 'num'})
                for i, num in enumerate(num_list):
                    try:
                        if i == 1:
                            str_price = num.find('span').text
                            if str_price == '0':
                                data.append(0)
                            else:
                                span_class = num.find('span')['class']
                                price = int(str_price.replace('\t', '').replace('\n', '').replace(',', ''))
                                if 'nv01' in span_class:
                                    price = price * -1
                                data.append(price)
                        else:
                            str_price = num.find('span').text
                            price = int(str_price.replace(',', ''))
                            data.append(price)
                    except Exception as e:
                        print(e)
                if date == start:
                    done = True
                    break
                df = df.append(pd.DataFrame([data], columns=columns), ignore_index=True)
            if done:
                break
        df.sort_values(by=[DAY], inplace=True)
        df.set_index(keys=[DAY], inplace=True)
        return df

    def get_all_daily_data(self, code):
        url = 'https://finance.naver.com/item/sise_day.nhn?code=' + str(code)
        html = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
        bsObj = BeautifulSoup(html.text, 'lxml')
        a_link = bsObj.find('td', {'class': 'pgRR'})

        if a_link is None:
            print('a_link is none')
            pageNum = 1
        else:
            link = a_link.find('a')['href']
            pageNum = int(link.split('=')[2])

        columns = ['날짜', '종가', '전일비', '시가', '고가', '저가', '거래량']
        df = pd.DataFrame(columns=columns)

        for page in range(1, pageNum + 1):
            url = 'https://finance.naver.com/item/sise_day.nhn?code=' + str(code) + '&page=' + str(page)
            html = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
            source = BeautifulSoup(html.text, 'lxml')
            slists = source.find_all('tr', {'onmouseover': 'mouseOver(this)'})
            for tr in slists:
                data = []
                date = tr.find('td', {'align': 'center'}).text
                if len(date) < 5:
                    break
                data.append(date)
                num_list = tr.find_all('td', {'class': 'num'})
                for i, num in enumerate(num_list):
                    try:
                        if i == 1:
                            str_price = num.find('span').text
                            if str_price == '0':
                                data.append(0)
                            else:
                                span_class = num.find('span')['class']
                                price = int(str_price.replace('\t', '').replace('\n', '').replace(',', ''))
                                if 'nv01' in span_class:
                                    price = price * -1
                                data.append(price)
                        else:
                            str_price = num.find('span').text
                            price = int(str_price.replace(',', ''))
                            data.append(price)
                    except Exception as e:
                        print(e)

                df = df.append(pd.DataFrame([data], columns=columns), ignore_index=True)
        df.sort_values(by=[DAY], inplace=True)
        df.set_index(keys=[DAY], inplace=True)
        return df

    def make_company_info(self, code):
        BASE_URL = 'https://finance.naver.com/sise/sise_market_sum.nhn?sosok='
        res = requests.get(BASE_URL + str(code) + "&page=" + str('1'))
        page_soup = BeautifulSoup(res.text, 'lxml')

        # total_page 가져오기
        total_page_num = page_soup.select_one('td.pgRR > a')
        total_page_num = int(total_page_num.get('href').split('=')[-1])

        # 가져올 수 있는 항목명들을 추출
        ipt_html = page_soup.select_one('div.subcnt_sise_item_top')

        # fields라는 변수에 담아 다른 곳에서도 사용할 수 있도록 global 키워드를 붙임

        self.fields = [item.get('value') for item in ipt_html.select('input')]

        # page마다 정보를 긁어오게끔 하여 result에 저장
        result = [self.company(code, str(page)) for page in range(1, total_page_num + 1)]

        # page마다 가져온 정보를 df에 하나로 합침
        df = pd.concat(result, axis=0, ignore_index=True)

        # Naver Finance에서 긁어온 종목들을 바탕으로 유니버스 구성
        # N/A로 값이 없는 필드 0으로 채우기
        mapping = {',': '', 'N/A': '0'}
        df.replace(mapping, regex=True, inplace=True)

        # # 사용할 column들 설정
        # cols = ['거래량', '매출액', '매출액증가율', 'ROE', 'PBR', 'PER']
        #
        # # column들을 숫자타입으로 변환(Naver Finance에서 제공 받은 원래 데이터는 str 형태)
        # df[cols] = df[cols].astype(float)
        out_df = pd.concat([df], axis=0, ignore_index=True)

        return out_df

    def company(self, code, page):
        BASE_URL = 'https://finance.naver.com/sise/sise_market_sum.nhn?sosok='
        # naver finance에 전달할 값들을 만듬, fieldIds에 먼저 가져온 항목명들을 전달하면 이에 대한 응답을 준다.
        data = {'menu': 'market_sum',
                'fieldIds': self.fields,
                'returnUrl': BASE_URL + str(code) + "&page=" + str(page)}

        # requests.get 요청대신 post 요청을 보낸다.
        res = requests.post('https://finance.naver.com/sise/field_submit.nhn', data=data)

        page_soup = BeautifulSoup(res.text, 'lxml')

        # 크롤링할 table html 가져온다. 실질적으로 사용할 부분
        table_html = page_soup.select_one('div.box_type_l')

        # column명을 가공한다.
        header_data = [item.get_text().strip() for item in table_html.select('thead th')][1:-1]

        # 종목명 + 수치 추출 (a.title = 종목명, td.number = 기타 수치)
        inner_data = [item.get_text().strip() for item in table_html.find_all(lambda x:
                                                                              (x.name == 'a' and
                                                                               'tltle' in x.get('class', [])) or
                                                                              (x.name == 'td' and
                                                                               'number' in x.get('class', []))
                                                                              )]

        # page마다 있는 종목의 순번 가져오기
        no_data = [item.get_text().strip() for item in table_html.select('td.no')]
        number_data = np.array(inner_data)

        # 가로x 세로 크기에 맞게 행렬화
        number_data.resize(len(no_data), len(header_data))

        # 한 페이지에서 얻은 정보를 모아 DataFrame로 만들어 리턴
        df = pd.DataFrame(data=number_data, columns=header_data)
        return df

    def get_stock_dict(self, market=None, delisted=False):
        params = {'method': 'download'}
        company_dict = {}

        DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'

        if market.lower() in MARKET_CODE_DICT:
            params['marketType'] = MARKET_CODE_DICT[market]

        if not delisted:
            params['searchType'] = 13

        params_string = urllib.parse.urlencode(params)
        request_url = urllib.parse.urlunsplit(['http', DOWNLOAD_URL, '', params_string, ''])

        df = pd.read_html(request_url, header=0)[0]
        df.종목코드 = df.종목코드.map('{:06d}'.format)
        for index in df.index:
            company_dict[df.loc[index][CODE]] = df.loc[index][COM_NAME]
        return company_dict
