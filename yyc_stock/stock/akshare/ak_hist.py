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
        code_list = ','.join(codes)
        df = self._get_df_source(db_name="daily_pro.db",sql=f"select * from daily where code in ({code_list}) and d > '{sdate}'")
        #df = self._get_df_source(ak_func=ak.stock_intraday_em,columns={'时间':'t'})
        return df
    
    def register_router(self):
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
                content = self._to_html(df,formats=formats,fix_columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
