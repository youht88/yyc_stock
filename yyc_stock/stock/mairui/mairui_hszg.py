from datetime import datetime, timedelta
import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from .mairui_base import MairuiBase

class HSZG(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def get_hszg_list(self):
        """获取沪深两市的指数代码"""
        res = requests.get( 
            f"{self.mairui_api_url}/hszg/list/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hszg_gg(self,code:str):
        '''根据指数、行业、概念板块代码找股票'''
        #http://api.mairui.club/hszg/gg/指数代码/您的licence
        code_info = self._get_bk_code(code)
        code = code_info["code"]
        res = requests.get( 
            f"{self.mairui_api_url}/hszg/gg/{code}/{self.mairui_token}",
        )
        data = res.json() 
        return data
    def get_hszg_zg(self,code:str):
        '''根据股票找相关指数、行业、概念板块'''
        #http://api.mairui.club/hszg/zg/股票代码(如000001)/您的licence
        code_info = self._get_stock_code(code)
        code = code_info["code"]
        res = requests.get( 
            f"{self.mairui_api_url}/hszg/zg/{code}/{self.mairui_token}",
        )
        data = res.json() 
        return data
    
    def register_router(self):
        @self.router.get("/hszg/gg/{code}",response_class=HTMLResponse)
        async def get_hszg_gg(code:str,req:Request):
            '''根据指数、行业、概念板块代码找股票'''
            try:
                codes=code.split(',')
                print("codes:",codes)
                data = self.parallel_execute(func=self.get_hszg_gg,codes=codes)
                dfs = [pd.DataFrame(item) for item in data]
                df = dfs[0]
                for item in dfs[1:]:
                    df = df[df['dm'].isin(item['dm'])]
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/hszg/zg/{code}",response_class=HTMLResponse)
        async def get_hszg_zg(code:str,req:Request):
            '''根据股票找相关指数、行业、概念板块'''
            try:
                codes=code.split(',')
                print("codes:",codes)
                data = self.parallel_execute(func=self.get_hszg_zg,codes=codes)
                dfs = [pd.DataFrame(item) for item in data]
                df = dfs[0]
                for item in dfs[1:]:
                    df = df[df['code'].isin(item['code'])]
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)        
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
