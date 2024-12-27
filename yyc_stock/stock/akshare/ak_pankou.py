from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase

class AK_PANKOU(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def bkyd(self):
        df = self._get_df_source(ak_func=ak.stock_board_change_em,columns={'时间':'t'})
        return df
    def gpyd(self,method):
        df = self._get_df_source(ak_func=ak.stock_changes_em,symbol=method,columns={'时间':'t'})
        return df    
    def register_router(self):
        @self.router.get("/pankou/bkyd")
        async def pankou_bkyd(req:Request):
            """获取当前盘口板块异动数据"""
            try:
                df = self.bkyd()
                df = self._prepare_df(df,req)
                desc= req.query_params.get('desc')
                content = self._to_html(df,formats=req.query_params.get('f'),fix_columns=['代码','名称','换手率'],url=req.url,desc=desc)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/pankou/gpyd")
        async def pankou_gpyd(req:Request):
            """当前盘口股票异动数据"""
            try:
                method = req.query_params.get('method')
                if not method:
                    raise Exception("必须指定method参数,method='火箭发射', '快速反弹', '大笔买入', '封涨停板', '打开跌停板', '有大买盘','竞价上涨', '高开5日线', '向上缺口', '60日新高', '60日大幅上涨', '加速下跌', '高台跳水','大笔卖出', '封跌停板', '打开涨停板', '有大卖盘', '竞价下跌', '低开5日线', '向下缺口', '60日新低', '60日大幅下跌'")
                df = self.gpyd(method)
                df = self._prepare_df(df,req)
                desc= req.query_params.get('desc')
                if not desc:
                    desc="""必须指定method参数,method为:<br>
                    '火箭发射', '高台跳水', '快速反弹', '加速下跌','向上缺口','向下缺口'<br>
                    '竞价上涨', '高开5日线', '竞价下跌', '低开5日线', <br>
                    '大笔买入', '有大买盘', '大笔卖出', '有大卖盘', <br> 
                    '封涨停板','封跌停板', '打开涨停板','打开跌停板',<br>
                    '60日新高', '60日大幅上涨','60日新低', '60日大幅下跌'"""

                content = self._to_html(df,formats=req.query_params.get('f'),fix_columns=['代码','名称','换手率'],url=req.url,desc=desc)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
