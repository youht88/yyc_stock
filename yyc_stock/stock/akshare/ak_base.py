import contextlib
import io
import json
import sqlite3
from fastapi import HTTPException, Request
from fastapi.datastructures import QueryParams
from fastapi.responses import HTMLResponse
import pandas as pd
import requests
import akshare as ak

from ylz_utils.config import Config
from yyc_stock.stock.base import StockBase
from datetime import datetime,timedelta
from tqdm import tqdm
import numpy as np

class AkshareBase(StockBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def current(self,codes=None):
        df = ak.stock_zh_a_spot_em()
        if codes:
            df = df[df['代码'].isin(codes)]
        return df
    def index_info(self):
        df = ak.index_stock_info()
        return df
    def index_cons(self,name:str):
        df = ak.index_stock_cons(name)
        return df
    def index_daily(self,name:str):
        df = ak.index_zh_a_hist(name)
        return df
    def index_minute(self,name:str):
        df = ak.index_zh_a_hist_min_em(name)
        return df
    def gnbk_info(self):
        df = ak.stock_board_concept_name_em()
        return df
    def gnbk_cons(self,name:str):
        df = ak.stock_board_concept_cons_em(name)
        return df
    def gnbk_daily(self,name:str):
        df = ak.stock_board_concept_hist_em(name)
        return df
    def gnbk_minute(self,name:str):
        df = ak.stock_board_concept_hist_min_em(name)
        return df
    def hybk_info(self):
        df = ak.stock_board_industry_name_em()
        return df
    def hybk_cons(self,name:str):
        df = ak.stock_board_industry_cons_em(name)
        return df
    def hybk_daily(self,name:str):
        df = ak.stock_board_industry_hist_em(name)
        return df
    def hybk_minute(self,name:str):
        df = ak.stock_board_industry_hist_min_em(name)
        return df
    
    def fx1(self,codes=[],sdate=None):
        daily_pro_db = sqlite3.connect("daily_pro.db")
        table = "daily"
        codes = codes[:3]
        #codes_str = ",".join([f"'{code}'" for code in codes])
        #df = pd.read_sql(f"select * from daily where code in ( {codes_str} )",daily_pro_db)
        with tqdm(total=len(codes),desc="进度") as pbar:
            dfs=[]
            if not sdate:
                today=datetime.today()
                sdate = (today + timedelta(days=-100)).strftime("%Y-%m-%d")
            for code in codes:
                try:
                    df = pd.read_sql(f"select * from {table} where code='{code}' and d >= '{sdate}'",daily_pro_db)
                    if df.empty:
                        pbar.update(1)
                        continue
                    cond=pd.Series([True] * len(df))
                    #  4天前涨停
                    cond = cond & (df['zd3'] > 9.9)
                    #  3天前最高价高于4天前的最高价
                    cond = cond & (df['h2'] > df['h3'])
                    # #  1天前最低价高于4天前最低价的50%
                    cond = cond & (df['l'] > df['l3'])
                    # 4天前收盘价低于60日均线
                    cond = cond & (df['c'] < df['ma20c'])
                    #  前3天到前一天连续下跌，且幅度小于%5，且连续缩量 
                    for field in ['v','zd']:
                        for idx in range(3):
                            idx_str = '' if idx==0 else str(idx)
                            # if (idx== 0 or idx==1) and field=='v':
                            #     cond1= (df[f'{field}{idx_str}'] / df[f'{field}{idx+1}']) > 1.3
                            #     cond = cond & cond1
                            # elif (idx== 0 or idx==1) and field=='c':
                            #     cond1= (df[f'{field}{idx_str}'] / df[f'{field}{idx+1}']) > 1.1
                            #     cond = cond & cond1
                            # 股价下跌,且跌幅小于5%
                            if field=='zd':
                                cond1 = df[f'{field}{idx_str}'].ge(-5) & df[f'{field}{idx_str}'].le(0)
                                cond = cond & cond1
                            # 连续缩量
                            if field=='v':
                                #cond1= df[f'{field}{idx_str}'].lt(df[f'{field}{idx+1}'])
                                cond1 = df[f'{field}{idx_str}'] < df['v']
                                cond = cond & cond1
                                    
                    # for idx in range(days):
                    #     idx_str = '' if idx==0 else str(idx)
                    #     cond = cond & df[f'hs{idx_str}'].gt(3) & df[f'hs{idx_str}'].lt(10)
                    df_new = df[cond]
                    dfs.append(df_new)
                    pbar.update(1)
                except Exception as e:
                    print(f"error on {code},{e}")
                    pbar.update(1)
        df_concat = pd.concat(dfs,ignore_index=True)
        print("df_concat:",len(df_concat))
        return df_concat
    def fx2(self,codes=[],sdate=None,kwarg={}):
        daily_pro_db = sqlite3.connect("daily_pro.db")
        codes_str = ",".join([f"'{code}'" for code in codes])
        lxzd = kwarg.get('lxzd')
        lxsf = kwarg.get('lxsf')
        lxzdt = kwarg.get('lxzdt')
        lxzljlr = kwarg.get('lxzljlr')
        cond = []
        if sdate:
            cond.append(f"d >= '{sdate}'")
        if lxzd:
            cond.append(f"lxzd = {lxzd}") 
        if lxzdt:
            cond.append(f"lxzdt = {lxzdt}") 
        if lxsf:
            cond.append(f"lxsf = {lxsf}")
        if lxzljlr:
            cond.append(f"lxzljlr = {lxzljlr}")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from daily where code in ({codes_str}) {cond_str}",daily_pro_db)
        df = df.filter(['d','code','mc','o','c','h','l','v','e','zf','zd','hs','zljlr',
                        'pzd_1','pzd_2','pzd_3','pzd_4','pzd_5',
                        'c_status','v_status','lxzd','lxsf','lxzdt','lxzljlr',
                        'zljlrl','zljlrl1','zljlrl2','zljlrl3','zljlrl4','zljlrl5',
                        'zd1','zd2','zd3','zd4',
                        'kl','kl1','kl2','dl','dl1','dl2','jl','jl1','jl2','dif','dif1','dif2','dea','dea1','dea2','macd','macd1','macd2',
                        'ma5c','ma5c1','ma5c2','ma10c','ma10c1','ma10c2','ma20c','ma20c1','ma20c2',
                        'prod5c','prod10c','prod20c','sum5v','sum10v','sum20v',
                        ])
        return df
    def fx3(self,codes=[],sdate=None,kwarg={}):
        daily_pro_db = sqlite3.connect("daily_pro.db")
        codes_str = ",".join([f"'{code}'" for code in codes])
        cond = []
        if sdate:
            cond.append(f"d > '{sdate}'")
        #cond.append(f"abs(kl - dl) < 1 and kl > kl1 and kl1 > kl2 and dif>0 and dea>0 and macd > macd1 and macd1 < macd2 and macd<0 and macd1<0 and macd2<0") 
        cond.append(f"(abs(macd1)-abs(macd))/abs(macd)>0.5 and abs(macd1) > abs(macd2) and macd<0 and macd1<0 and macd2<0 and c>h1 and v>v1") 
        cond.append(f"hs>3 and hs1>3 and hs2>3 and hs<5 and hs1<5 and hs2<5")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from daily where code in ({codes_str}) {cond_str}",daily_pro_db)
        df = df.filter(['d','code','mc','o','c','h','l','v','e','zf','zd','hs',
                        'pzd_1','pzd_2','pzd_3','pzd_4','pzd_5',
                        'c_status','v_status','lxzd','lxsf',
                        'lxzdt','kl','kl1','kl2','dl','dl1','dl2','dif','dea','macd','macd1','macd2',
                        'ma20c','ma40c',
                        'prod5c','prod10c','prod20c','sum5v','sum10v','sum20v',
                        ])
        return df
    def fx4(self,codes=[],sdate=None,kwarg={}):
        minute_pro_db = sqlite3.connect("minute_pro.db")
        codes_str = ",".join([f"'{code}'" for code in codes])
        cond = []
        if sdate:
            cond.append(f"t >= '{sdate}'")
        #cond.append(f"abs(kl - dl) < 1 and kl > kl1 and kl1 > kl2 and dif>0 and dea>0 and macd > macd1 and macd1 < macd2 and macd<0 and macd1<0 and macd2<0") 
        #cond.append(f"(abs(macd1)-abs(macd))/abs(macd)>0.5 and abs(macd1) > abs(macd2) and macd<0 and macd1<0 and macd2<0 and c>h1 and v>v1") 
        #cond.append(f"hs>3 and hs1>3 and hs2>3 and hs<5 and hs1<5 and hs2<5")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from minute where code in ({codes_str}) {cond_str}",minute_pro_db)
        df = df.filter(['t','code','mc','o','c','h','l','v','e','zf','zd','hs',
                        'pzd_1','pzd_2','pzd_3','pzd_4','pzd_5',
                        'c_status','v_status','lxzd','lxsf',
                        'lxzdt','kl','kl1','kl2','dl','dl1','dl2','dif','dea','macd','macd1','macd2',
                        'ma20c','ma40c',
                        'pct5c','pct10c','pct20c','sum5v','sum10v','sum20v',
                        ])
        return df

    def fx5(self,codes=[],sdate=None,kwarg={}):
        daily_pro_db = sqlite3.connect("daily_pro.db")
        codes_str = ",".join([f"'{code}'" for code in codes])
        cond = []
        if sdate:
            cond.append(f"d >= '{sdate}'")
        #cond.append(f"abs(kl - dl) < 1 and kl > kl1 and kl1 > kl2 and dif>0 and dea>0 and macd > macd1 and macd1 < macd2 and macd<0 and macd1<0 and macd2<0") 
        #cond.append(f"(abs(macd1)-abs(macd))/abs(macd)>0.5 and abs(macd1) > abs(macd2) and macd<0 and macd1<0 and macd2<0 and c>h1 and v>v1") 
        cond.append(f"zljlr>0 and zljlr1>0 and zljlr2>0 and zljlr3>0 and zljlr4>0 and zljlr5>0")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from daily where code in ({codes_str}) {cond_str}",daily_pro_db)
        df = df.filter(['d','code','mc','o','c','h','l','v','e','zf','zd','hs',
                        'pzd_1','pzd_2','pzd_3','pzd_4','pzd_5',
                        'c_status','v_status','lxzd','lxsf','lxzdt',
                        'zljlrl','cddjlrl','ddjlrl','zdjlrl','xdjlrl',
                        'zljlr','zljlr1','zljlr2','zljlr3','zljlr4','zljlr5','zljlr6','zljlr7','zljlr8',
                        'pct5c','pct10c','pct20c',
                        ])
        return df
    def fx6(self,codes=[],sdate=None,kwarg:QueryParams=None):
        price_db = sqlite3.connect("price.db")
        codes_str = ",".join([f"'{code}'" for code in codes])
        cond = []
        if sdate:
            cond.append(f"date >= '{sdate}'")
        # if kwarg:
        #     f = kwarg.get("f")
        #     print("f=",f)
        #cond.append(f"abs(kl - dl) < 1 and kl > kl1 and kl1 > kl2 and dif>0 and dea>0 and macd > macd1 and macd1 < macd2 and macd<0 and macd1<0 and macd2<0") 
        #cond.append(f"(abs(macd1)-abs(macd))/abs(macd)>0.5 and abs(macd1) > abs(macd2) and macd<0 and macd1<0 and macd2<0 and c>h1 and v>v1") 
        #cond.append(f"zljlr>0 and zljlr1>0 and zljlr2>0 and zljlr3>0 and zljlr4>0 and zljlr5>0")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from price where code in ({codes_str}) {cond_str}",price_db)
        return df
    def get_help(self,func)->str:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            help(func)
        docstring =buffer.getvalue()
        html_content = "<html><body>"
        html_content += f"<h1>函数{func.__name__}帮助文档</h1>"
        html_content += "<pre>" + docstring.replace('\n', '<br>') + "</pre>"
        html_content += "</body></html>"
        return html_content
    def register_router(self):
        @self.router.get("/")
        async def ak_df(req:Request):
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                func = req.query_params.get("func")
                if func:
                    df = eval(f"ak.{func}")
                    if callable(df):
                        content = self.get_help(df)
                    else:
                        df = self._prepare_df(df,req)
                        content = self._to_html(df,formats=req.query_params.get('f'),url=req.url)
                    return HTMLResponse(content=content)
                else:
                    raise Exception("必须指定func参数，例如func=stock_margin_detail_sse('20241226'),如果要获得函数的帮助文档则func=stock_margin_detail_sse")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")            
            
        @self.router.get("/fx1")
        async def fx1(req:Request):
            """fx1"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes = [item for item in code.split(',') if item]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]
                sdate=req.query_params.get('sdate')
                df = self.fx1(codes,sdate)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/fx2")
        async def fx2(req:Request):
            """fx2"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes_info = [self._get_stock_code(item) for item in code.split(',') if item]
                    codes = [item['code'] for item in codes_info]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                lxzd = req.query_params.get('lxzd')
                if lxzd:
                    lxzd=int(lxzd)
                else:
                    lxzd=None
                lxsf = req.query_params.get('lxsf')
                if lxsf:
                    lxsf=int(lxsf)
                else:
                    lxsf=None
                lxzdt = req.query_params.get('lxzdt')
                if lxzdt:
                    lxzdt=int(lxzdt)
                else:
                    lxzdt=None
                lxzljlr = req.query_params.get('lxzljlr')
                if lxzljlr:
                    lxzljlr=int(lxzljlr)
                else:
                    lxzljlr=None
                if not lxzd and not lxzdt and not lxsf and not lxzljlr:
                    raise Exception("至少须指定lxzd,lxsf,lxzdt,lxzljlr中的一个参数")
                df = self.fx2(codes,sdate,{'lxzd':lxzd,'lxsf':lxsf,'lxzdt':lxzdt,'lxzljlr':lxzljlr})
                df = self._prepare_df(df,req)
                content = self._to_html(df,fix_columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/fx3")
        async def fx3(req:Request):
            """fx3"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes_info = [self._get_stock_code(item) for item in code.split(',') if item]
                    codes = [item['code'] for item in codes_info]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                lxzd = req.query_params.get('lxzd')
                if lxzd:
                    lxzd=int(lxzd)
                else:
                    lxzd=None
                df = self.fx3(codes,sdate,{'lxzd':lxzd})
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/fx4")
        async def fx4(req:Request):
            """fx4"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01 00:00:00'
                df = self.fx4(codes,sdate)
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/fx5")
        async def fx5(req:Request):
            """fx5"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                df = self.fx5(codes,sdate)
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/fx6")
        async def fx6(req:Request):
            """fx6"""
            try:
                codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2024-01-01'
                df = self.fx6(codes,sdate,req.query_params)
                df = self._prepare_df(df,req)
                formats = req.query_params.get("f")
                content = self._to_html(df,columns=['code','mc'],formats=formats)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/index/{method}")
        async def _index(method:str,req:Request):
            """获取当前行情数据"""
            try:
                name = req.query_params.get('name')
                if not name and method!='info':
                    raise Exception(f"必须指定name参数")
                if method=='info':
                    df = self.index_info()
                elif method=='cons':
                    df = self.index_cons(name)
                    df['name'] = name
                elif method=='daily':
                    df = self.index_daily(name)
                    df['name'] = name
                elif method=='minute':
                    df = self.index_minute(name)
                    df['name'] = name
                else:
                    raise Exception(f'错误的method:{method}')               
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
            
        @self.router.get("/gnbk/{method}")
        async def _gnbk(method:str,req:Request):
            """获取当前行情数据"""
            name = req.query_params.get('name')
            if not name and method!='info':
                raise Exception(f"必须指定name参数")
            try:
                if method=='info':
                    df = self.gnbk_info()
                elif method=='cons':
                    df = self.gnbk_cons(name)
                    df['name'] = name
                elif method=='daily':
                    df = self.gnbk_daily(name)
                    df['name'] = name
                elif method=='minute':
                    df = self.gnbk_minute(name)
                    df['name'] = name
                else:
                    raise Exception(f'错误的method:{method}')               
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/hybk/{method}")
        async def _hybk(method:str,req:Request):
            """获取当前行情数据"""
            name = req.query_params.get('name')
            if not name and method!='info':
                raise Exception(f"必须指定name参数")
            try:
                if method=='info':
                    df = self.hybk_info()
                elif method=='cons':
                    df = self.hybk_cons(name)
                    df['name'] = name
                elif method=='daily':
                    df = self.hybk_daily(name)
                    df['name'] = name
                elif method=='minute':
                    df = self.hybk_minute(name)
                    df['name'] = name
                else:
                    raise Exception(f'错误的method:{method}')               
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/lxslxd")
        async def lxslxd(req:Request):
            """分析连续缩量下跌信息"""
            try:
                daily_pro_db=sqlite3.connect("daily_pro.db")
                table = "daily"
                codes_info = self._get_bk_codes("hs_a")
                #codes_info = codes_info[:100]
                print("total codes:",len(codes_info))
                error_code_info=[]
                error_msg=""
                
                days = req.query_params.get('days')
                if days:
                    days=int(days)
                else:
                    days=5
                with tqdm(total=len(codes_info),desc="进度") as pbar:
                    dfs=[]
                    for code_info in codes_info:
                        code = code_info['dm']
                        df = pd.read_sql(f"select * from {table} where code='{code}'",daily_pro_db)
                        cond=pd.Series([True] * len(df))
                        for field in ['c','v']:
                            for idx in range(days):
                                idx_str = '' if idx==0 else str(idx)
                                # if (idx== 0 or idx==1) and field=='v':
                                #     cond1= (df[f'{field}{idx_str}'] / df[f'{field}{idx+1}']) > 1.3
                                #     cond = cond & cond1
                                # elif (idx== 0 or idx==1) and field=='c':
                                #     cond1= (df[f'{field}{idx_str}'] / df[f'{field}{idx+1}']) > 1.1
                                #     cond = cond & cond1
                                if field=='c':
                                    cond1= df[f'{field}{idx_str}'].lt(df[f'{field}{idx+1}'])
                                    if field=='c':
                                        cond2=df[f'{field}{idx_str}'].lt(df[f'o{idx+1}'])
                                        cond = cond & (cond1 & cond2)
                                    else:    
                                        cond = cond & cond1
                                if field=='v':
                                    cond1= df[f'{field}{idx_str}'].gt(df[f'{field}{idx+1}'])
                                    cond = cond & cond1
                                        
                        for idx in range(days):
                            idx_str = '' if idx==0 else str(idx)
                            cond = cond & df[f'hs{idx_str}'].gt(3) & df[f'hs{idx_str}'].lt(10)
                        df_new = df[cond]
                        dfs.append(df_new)
                        pbar.update(1)
                df_concat = pd.concat(dfs,ignore_index=True)
                print("df_concat:",len(df_concat))
                df = self._prepare_df(df_concat,req)
                content = self._to_html(df,columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/lxxd")
        async def lxxd(req:Request):
            """分析连续下跌信息"""
            try:
                daily_db=sqlite3.connect("daily.db")
                table = "daily"
                codes_info = self._get_bk_codes("hs_a")
                #codes_info = codes_info[:100]
                print("total codes:",len(codes_info))
                error_code_info=[]
                error_msg=""
                
                days = req.query_params.get('days')
                if days:
                    days=int(days)
                else:
                    days=5
                sdate = req.query_params.get('sdate')
                if not sdate:
                    sdate = '2022-01-01'
                with tqdm(total=len(codes_info),desc="进度") as pbar:
                    dfs=[]
                    for code_info in codes_info:
                        code = code_info['dm']
                        origin_df = pd.read_sql(f"select * from {table} where code='{code}' and d >= '{sdate}'",daily_db)
                        df = origin_df.reset_index() 
                        for idx in range(days):
                            t=idx+1
                            df[f'pzd_{t}']=df['zd'].shift(-t)
                        for idx in range(days):
                            t=idx+1
                            df[f'o{t}']=df['o'].shift(t)
                            df[f'c{t}']=df['c'].shift(t)
                            df[f'v{t}']=df['v'].shift(t)
                            df[f'e{t}']=df['e'].shift(t)
                            df[f'po{t}']=df['o'].shift(-t)
                            df[f'pc{t}']=df['c'].shift(-t)
                        cond=pd.Series([True] * len(df))
                        for field in ['c']:
                            for idx in range(days):
                                if idx==0:
                                    cond1 = df[f'{field}'] < df[f'{field}{idx+1}']
                                    if field=='c':
                                        cond2=df[f'{field}'] < df[f'o{idx+1}']
                                        cond = cond & (cond1 & cond2)
                                    else:    
                                        cond = cond & cond1
                                else:
                                    cond1= df[f'{field}{idx}'].lt(df[f'{field}{idx+1}'])
                                    if field=='c':
                                        cond2=df[f'{field}{idx}'].lt(df[f'o{idx+1}'])
                                        cond = cond & (cond1 & cond2)
                                    else:    
                                        cond = cond & cond1
                        df_new = df[cond]
                        dfs.append(df_new)
                        pbar.update(1)
                df_concat = pd.concat(dfs,ignore_index=True)
                print("df_concat:",len(df_concat))
                df = self._prepare_df(df_concat,req)
                content = self._to_html(df,columns=['code','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/test")
        async def _test(req:Request):
            """分析连续下跌信息"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                df = self._get_df_source(ak_func=ak.stock_intraday_em,columns={'时间':'t'})
                codes = self._get_request_codes(req)
                df = self.current(codes)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=req.query_params.get('f'))
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
    
