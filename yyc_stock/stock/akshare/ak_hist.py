from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase

class AK_HIST(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def hist_daily_pro(self,codes,sdate):
        df = pd.DataFrame()
        code_list = ','.join(map(lambda x:f"'{x}'",codes)) 
        print("???",sdate)
        sql = f"select * from daily where code in ({code_list}) and date >= '{sdate}'"
        df = self._get_df_source(db_name="daily_pro.db",sql=sql)
        #df = self._get_df_source(ak_func=ak.stock_intraday_em,columns={'时间':'t'})
        return df
    
    def hist_minute_pro(self,codes,sdate):
        df = pd.DataFrame()
        code_list = ','.join(map(lambda x:f"'{x}'",codes)) 
        sql = f"select * from minute where code in ({code_list}) and date >= '{sdate}'"
        df = self._get_df_source(db_name="minute_pro.db",sql=sql)
        #df = self._get_df_source(ak_func=ak.stock_intraday_em,columns={'时间':'t'})
        return df

    def hist_price(self,codes,sdate):
        df = pd.DataFrame()
        code_list = ','.join(codes)
        print(code_list,sdate)
        df = self._get_df_source(db_name="price.db",sql=f"select * from price where code in ({code_list}) and date >= '{sdate}'")
        #df = self._get_df_source(ak_func=ak.stock_intraday_em,columns={'时间':'t'})
        return df
    def hist_zf(self,code,sdate=None,zf=5):
        # 查找股票sdate之前振幅超过5,且(最高价>=当前最高价，或最低价<=当前最低价)的最近日期
        today = datetime.today().strftime('%Y-%m-%d')
        sdate = sdate if sdate else today
        code = self._get_codes('stock',code)[0]
        df = self._get_df_source(db_name="daily_pro.db",sql=f"select * from daily where code = '{code}' and date<='{sdate}'")
        df.date = pd.to_datetime(df.date)
        current = df.loc[df['date'].dt.strftime('%Y-%m-%d')==sdate]
        max_date = df.loc[(df['date']<sdate) & (df['zf'] > 5) & ((df['h']>=current.h.values[0]) | (df['l']<=current.l.values[0])), 'date']
        print(max_date)
        # return max_date
        return df
    def register_router(self):
        @self.router.get("/hist/daily_etf")
        async def _hist_daily_etf(req:Request):
            """获取历史行情数据"""
            try:
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '20230101'
                else:
                    sdate = sdate.replace('-','')
                codes = self._get_request_codes(req)
                dfs = []    
                for code in codes:
                    code_df = ak.fund_etf_hist_em(code,start_date=sdate)
                    #code_df = pd.to_datetime(code_df['日期'])
                    code_df['code'] = code
                    code_df['mc'] = self.etf_codes[code]['mc']
                    code_df = code_df.sort_values('日期',ascending=False)
                    dfs.append(code_df)
                df = pd.concat(dfs,axis=0)
                df['日期'] = pd.to_datetime(df['日期'])
                df = self._prepare_df(df,req)
                formats = req.query_params.get('f')
                if not formats:
                    formats =  'zd:0,0;e:100000000,100000000'
                desc= req.query_params.get('desc')
                content = self._to_html(df,formats=formats,fix_columns=['code','mc'],url=req.url,desc=desc)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hist/minute_etf")
        async def _hist_minute_etf(req:Request):
            """获取历史行情数据"""
            try:
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2023-01-01 09:00:00'
                codes = self._get_request_codes(req)
                dfs = []    
                for code in codes:
                    code_df = ak.fund_etf_hist_min_em(code,start_date=sdate,period=5)
                    #code_df = pd.to_datetime(code_df['日期'])
                    code_df['code'] = code
                    code_df['mc'] = self.etf_codes[code]['mc']
                    code_df = code_df.sort_values('时间',ascending=False)
                    dfs.append(code_df)
                df = pd.concat(dfs,axis=0)
                df['时间'] = pd.to_datetime(df['时间'])
                df = self._prepare_df(df,req)
                formats = req.query_params.get('f')
                if not formats:
                    formats =  'zd:0,0;e:100000000,100000000'
                desc= req.query_params.get('desc')
                content = self._to_html(df,formats=formats,fix_columns=['code','mc'],url=req.url,desc=desc)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hist/daily_pro")
        async def _hist_daily_pro(req:Request):
            """获取历史行情数据"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                df = self.hist_daily_pro(codes,sdate)
                df = self._prepare_df(df,req)
                formats = req.query_params.get('f')
                if not formats:
                    formats =  'zd:0,0;e:100000000,100000000'
                desc= req.query_params.get('desc')
                content = self._to_html(df,formats=formats,fix_columns=['code','mc'],url=req.url,desc=desc)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hist/minute_pro")
        async def _hist_minute_pro(req:Request):
            """获取历史行情数据"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                df = self.hist_minute_pro(codes,sdate)
                df = self._prepare_df(df,req)
                formats = req.query_params.get('f')
                if not formats:
                    formats =  'zd:0,0;e:100000000,100000000'
                content = self._to_html(df,formats=formats,fix_columns=['code','mc']) 
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")


        @self.router.get("/hist/price")
        async def _hist_price(req:Request):
            """获取历史交易统计数据"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                df = self.hist_price(codes,sdate)
                df['date'] = pd.to_datetime(df['date'])
                df = self._prepare_df(df,req)
                content = self._to_html(df,fix_columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hist/zf/{code}/{sdate}/{zf}")
        async def _hist_zf(code:str,sdate:str,zf:int,req:Request):
            try:
                #codes = self._get_request_codes(req)
                #sdate = req.query_params.get('sdate')
                df = self.hist_zf(code,sdate,zf)
                df['date'] = pd.to_datetime(df['date'])
                df = self._prepare_df(df,req)
                content = self._to_html(df,fix_columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
