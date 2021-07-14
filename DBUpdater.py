import pandas as pd
import urllib, calendar, time, json
from datetime import datetime
from urllib.request import urlopen
from threading import Timer
import pymysql
from urllib import request as req
from bs4 import BeautifulSoup as bs
import requests

class DBUpdater:
    def __init__(self):
        """생성자 : MariaDB 연결 및 종목코드 딕셔너리 생성 TEST"""
        self.conn = pymysql.connect(host='192.168.0.5', user='kikicom', passwd='3636', db='stock', charset='utf8')
        with self.conn.cursor() as curs:
            sql = """
                CREATE TABLE IF NOT EXISTS TB_COMPANY_INFO(
                    CODE VARCHAR(20),
                    COMPANY    VARCHAR(40),
                    LAST_UPDATE DATE,
                    PRIMARY KEY (CODE)
                )
            """
            curs.execute(sql)
            sql = """
                CREATE TABLE IF NOT EXISTS TB_DAILY_PRICE(
                    CODE VARCHAR(20),
                    DATE DATE,
                    OPEN BIGINT(20),
                    HIGH BIGINT(20),
                    LOW BIGINT(20),
                    CLOSE  BIGINT(20),
                    DIFF BIGINT(20),
                    VOLUME BIGINT(20),
                    PRIMARY KEY (CODE, DATE)
                )
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()
        self.update_comp_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def read_krx_code(self):
        """KRX로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 COMPANY_INFO 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM TB_COMPANY_INFO"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['CODE'].values[idx]]=df['COMPANY'].values[idx]
        with self.conn.cursor() as curs:
            sql = "SELECT max(LAST_UPDATE) FROM TB_COMPANY_INFO"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO TB_COMPANY_INFO (CODE, COMPANY, LAST_UPDATE) VALUES ('{code}','{company}','{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d')

                self.conn.commit()

    def read_naver(self, code, company, pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            '''
            headers=('User-Aqent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppliWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36')
            '''

            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'}
            request = urllib.request.Request(url, headers=hdr)

            with urlopen(request) as doc:
                if doc is None:
                    return None
                html = bs(doc.read(), "html.parser")
                pgrr = html.find('td', class_='pgRR')
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)

            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                pHtml = urllib.request.Request(pg_url, headers=hdr)
                doc = urlopen(pHtml)
                html = bs(doc.read(), "html.parser")
                table = html.find_all('table')[0]
                df = pd.read_html(str(table), header=0)[0]
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading ...'.format(tmnow, company, code, page, pages), end="\r")
            df =df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})
            df['date'] = df['date'].replace('.','-')
            df = df.dropna()
            df[['close','diff','open','high','low','volume','close']] = df[['close','diff','open','high','low','volume','close']].astype(int)
            df = df[['date','open','high','low','close','diff','volume']]
        except Exception as e:
            print('Exception occured : ', str(e))
            return None
        return df

    def replace_into_db(self, df, num, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = "REPLACE INTO TB_DAILY_PRICE VALUES ('{}','{}',{},{},{},{},{},{})".format(code, r.date, r.open, r.high, r.low, r.close, r.diff, r.volume)
                curs.execute(sql)
            self.conn.commit()
            print('[{}] {:04d} {} ({}) : {} rows > REPLACE INTO TB_DAILY_PRICE [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    def execute_daily(self):
        """실행 즉시 }및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.update_comp_info()
        try:
            with open('config.json','r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config ={'pages_to_fetch': 1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, ay=1, hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month+1, day=1, hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0, second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({})... ".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()

if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()