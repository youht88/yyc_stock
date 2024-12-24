from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase

class AK_LHB(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def hot_rank(self,codes):
        df = pd.DataFrame()
        date = datetime.today().strftime("%Y-%m-%d")
        today_total_df = ak.stock_zh_a_spot_em()
        today_total_df = today_total_df.rename(columns={'序号':'index', '代码':'code', '名称':'mc', '最新价':'c', '涨跌幅':'zd', '涨跌额':'zde', '成交量':'v', '成交额':'e',
                                                    '振幅':'zf', '最高':'h', '最低':'l','今开':'o', '昨收':'zc', '量比':'lb', '换手率':'hs', '市盈率-动态':'pe', 
                                                    '市净率':'sjl', '总市值':'zsz', '流通市值':'ltsz', '涨速':'zs', 
                                                    '5分钟涨跌':'min5zd','60日涨跌幅':'day60zd', '年初至今涨跌幅':'year1zd'})
        with tqdm(total=len(codes),desc="进度") as pbar:
            dfs=[]
            for code in codes:
                code_info = self._get_stock_code(code)
                code = code_info['code']
                mc = code_info['name']
                try:
                    df_today = today_total_df[today_total_df['code']==code]
                    df=ak.stock_intraday_em(code)
                    #df=ak.stock_zh_a_tick_tx_js(code)
                    if not df.empty:
                        df=df.rename(columns={'时间':'t','成交价':'p','手数':'v','买卖盘性质':'xz'})
                        df['tt']=df['t'].apply(lambda x:0 if x<'09:25:00' else 1 if x<'10:30:00' else 2 if x<'11:30:00' else 3 if x<'14:00:00' else 4)
                        df_jhjj = df[df.tt==0].copy()
                        df_jhjj['tt'] = df_jhjj['t'].apply(lambda x:0 if x<'09:20:00' else 1 )
                        df_jhjj['v0']=df_jhjj.groupby(['tt','p']).v.diff().fillna(df_jhjj.v)
                        df_jhjj['e']=df_jhjj.p*df_jhjj.v0*100
                        df_jhjj['vt'] = df_jhjj['e'].apply(lambda x:'cdd' if abs(x)>=1000000 else 'dd' if abs(x)>=200000 else 'zd' if abs(x)>=40000 else 'xd')
                        df_jhjj['cnt']=1
                        df_jhjj = df_jhjj.groupby(['tt','p','vt'])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df_jhjj['code']=code
                        df_jhjj['date']=date
                        df_jhjj['mc']=mc
                        
                        df_price = df[df.t>='09:25:00'].copy()
                        df_price['e']=df_price.p*df_price.v*100
                        df_price['vt'] = df_price['e'].apply(lambda x:'cdd' if x>=1000000 else 'dd' if x>=200000 else 'zd' if x>=40000 else 'xd')
                        df_price['cnt']=1
                        df_price = df_price.groupby(by=["xz","vt","p"])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df_price_s = df_price[df_price['xz']=='卖盘']
                        df_price_b = df_price[df_price['xz']=='买盘']
                        df_price = pd.merge(df_price_s,df_price_b,on=['p','vt'],how='outer',suffixes=['_s','_b'])
                        df_price = df_price.drop(columns=['xz_s','xz_b']).fillna(0)
                        df_xd=df_price[df_price['vt']=='xd']
                        df_zd=df_price[df_price['vt']=='zd']
                        df_dd=df_price[df_price['vt']=='dd']
                        df_cdd=df_price[df_price['vt']=='cdd']
                        df_xd_zd = pd.merge(df_xd,df_zd,on=['p'],how='outer',suffixes=['_xd','_zd'])
                        df_xd_zd = df_xd_zd.drop(columns=['vt_xd','vt_zd']).fillna(0)
                        df_dd_cdd = pd.merge(df_dd,df_cdd,on=['p'],how='outer',suffixes=['_dd','_cdd'])
                        df_dd_cdd = df_dd_cdd.drop(columns=['vt_dd','vt_cdd']).fillna(0)
                        df_price = pd.merge(df_xd_zd,df_dd_cdd,on=['p'],how='outer')
                        df_price = df_price.fillna(0)
                        
                        df_price['v_b']=df_price['v_b_xd']+df_price['v_b_zd']+df_price['v_b_dd']+df_price['v_b_cdd']
                        df_price['v_s']=df_price['v_s_xd']+df_price['v_s_zd']+df_price['v_s_dd']+df_price['v_s_cdd']
                        df_price['e_b']=df_price['e_b_xd']+df_price['e_b_zd']+df_price['e_b_dd']+df_price['e_b_cdd']
                        df_price['e_s']=df_price['e_s_xd']+df_price['e_s_zd']+df_price['e_s_dd']+df_price['e_s_cdd']
                        df_price['cnt_b']=df_price['cnt_b_xd']+df_price['cnt_b_zd']+df_price['cnt_b_dd']+df_price['cnt_b_cdd']
                        df_price['cnt_s']=df_price['cnt_s_xd']+df_price['cnt_s_zd']+df_price['cnt_s_dd']+df_price['cnt_s_cdd']
                        df_price['v_zljlr'] = (df_price['v_b_dd']+df_price['v_b_cdd']) - (df_price['v_s_dd']+df_price['v_s_cdd'])
                        df_price['e_zljlr'] = (df_price['e_b_dd']+df_price['e_b_cdd']) - (df_price['e_s_dd']+df_price['e_s_cdd'])
                        df_price['v_jlr']=df_price['v_b']-df_price['v_s']                        
                        df_price['e_jlr']=df_price['e_b']-df_price['e_s']

                        df_price.insert(0,'code',code)
                        df_price.insert(0,'mc',mc)
                        df_price.insert(0,'date',date)
                        
                        df_merged=pd.merge(df_price,df_today,on=['code'],how='left',suffixes=['','_today'])
                        df_price['status'] = df_merged.apply(lambda x: ('U' if x['p'] > x['o'] else 'D' if x['p'] < x['c'] else 'M') if x['o']>x['c'] else
                                                             ('D' if x['p'] < x['o'] else 'U' if x['p'] > x['c'] else 'M'),axis=1)
                        df_price['v'] = df_price['v_b']+df_price['v_s']
                        df_price['e'] = df_price['e_b']+df_price['e_s']
                        df_price['cnt'] = df_price['cnt_b']+df_price['cnt_s']
                    else:
                        print(f"no data on {code}-{mc}") 
                except Exception as e:
                    print(f"error on {code}-{mc}",e)               
        return pd.concat([df_jhjj,df_price])
        #return df_price
    
    def register_router(self):
        @self.router.get("/lhb/detail")
        async def lhb_detail(req:Request):
            """获取龙虎榜详情"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                #codes = self._get_request_codes(req)
                df = self._get_df_source(ak_func=ak.stock_lhb_stock_statistic_em,columns={})
                df.最近上榜日=pd.to_datetime(df.最近上榜日)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=req.query_params.get('f'),fix_columns=['代码','名称','换手率'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/lhb/hyyyb")
        async def lhb_detail(req:Request):
            """获取龙虎榜活跃营业部"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                #codes = self._get_request_codes(req)
                sdate = req.query_params.get('sdate')
                edate = req.query_params.get('edate')
                if not sdate and not edate:
                    raise Exception('必须指定sdate或edate参数，格式为YYYY-MM-DD')
                if not sdate:
                    sdate = edate
                if not edate:
                    edate = sdate
                formats = req.query_params.get('f')
                if not formats:
                    formats = "买入总金额:0,0;卖出总金额:0,0;总买卖净额:0,0"
                df = self._get_df_source(ak_func=ak.stock_lhb_hyyyb_em,start_date=sdate.replace('-',''),end_date=edate.replace('-',''),columns={})
                df.上榜日=pd.to_datetime(df.上榜日)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=formats,fix_columns=['营业部名称'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/lhb/yybph")
        async def lhb_yybph(req:Request):
            """获取龙虎榜营业部排行"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                #codes = self._get_request_codes(req)
                method = req.query_params.get('method')
                if not method:
                    method = '近一月'
                print(f'method = {method}: choice of ["近一月", "近三月", "近六月", "近一年"]')
                formats = req.query_params.get('f')
                if not formats:
                    formats = "买入总金额:0,0;卖出总金额:0,0;总买卖净额:0,0"
                df = self._get_df_source(ak_func=ak.stock_lhb_yybph_em,symbol=method,columns={})
                #df.上榜日=pd.to_datetime(df.上榜日)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=formats,fix_columns=['营业部名称'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/lhb/jgtj")
        async def lhb_jgtj(req:Request):
            """获取龙虎榜机构统计"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                #codes = self._get_request_codes(req)
                method = req.query_params.get('method')
                if not method:
                    method = '近一月'
                print(f'method = {method}: choice of ["近一月", "近三月", "近六月", "近一年"]')
                formats = req.query_params.get('f')
                if not formats:
                    formats = "龙虎榜成交金额:0,0;机构买入额:0,0;机构卖出额:0,0;机构净买额:0,0"
                df = self._get_df_source(ak_func=ak.stock_lhb_jgstatistic_em,symbol=method,columns={})
                #df.上榜日=pd.to_datetime(df.上榜日)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=formats,fix_columns=['营业部名称'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/lhb/yybtj")
        async def lhb_yybtj(req:Request):
            """获取龙虎榜营业部统计"""
            try:
                #df = self._get_df_source(db_name="price_30.db",sql=f"select * from price where code='{code}'")
                #codes = self._get_request_codes(req)
                method = req.query_params.get('method')
                if not method:
                    method = '近一月'
                print(f'method = {method}: choice of ["近一月", "近三月", "近六月", "近一年"]')
                formats = req.query_params.get('f')
                if not formats:
                    formats = "龙虎榜成交金额:0,0;买入额:0,0;卖出额:0,0"
                df = self._get_df_source(ak_func=ak.stock_lhb_traderstatistic_em,symbol=method,columns={})
                #df.上榜日=pd.to_datetime(df.上榜日)
                df = self._prepare_df(df,req)
                content = self._to_html(df,formats=formats,fix_columns=['营业部名称'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
