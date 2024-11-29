import json
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import pandas as pd
import requests
import akshare as ak

from ylz_utils.config import Config
from ylz_utils.stock.base import StockBase
from datetime import datetime,timedelta
from tqdm import tqdm
import numpy as np

class AkshareStock(StockBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def current(self):
        df = ak.stock_zh_a_spot_em()
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
    def bk_refresh(self):
        # 清除代码库
        con=sqlite3.connect(self.stock_db_name)
        try:
            con.execute("drop table akbk_codes")
        except:
            pass
        try:
            con.execute("drop table akbkdm")
        except:
            pass
        #获取数据
        df_res1 = ak.stock_board_industry_name_em()
        df_res2 = ak.stock_board_concept_name_em()
        df_hybk = df_res1[['板块名称','板块代码']].rename(columns={'板块名称':'name','板块代码':'code'})
        df_hybk['type']='hy'
        df_gnbk = df_res2[['板块名称','板块代码']].rename(columns={'板块名称':'name','板块代码':'code'})
        df_gnbk['type']='gn'
        df = pd.concat([df_hybk,df_gnbk]).reset_index()                   
        self.akbkdm = df.to_dict(orient='records')
        df.to_sql("akbkdm",index=False,if_exists="replace",con=con) 
        self.akbk_codes={}
        with tqdm(total=len(self.akbkdm)) as pbar:
            for bk_info in self.akbkdm:
                code = bk_info["code"]
                print(bk_info)
                self._get_akbk_codes(code)                
                pbar.update(1) 
        return {"message":"ok"}
    
    def daily_refresh(self,sdate='20220101'):
        daily_db=sqlite3.connect("daily.db")
        codes_info = self._get_bk_codes("hs_a")
        print("total codes:",len(codes_info))
        error_code_info=[]
        error_msg=""
        
        with tqdm(total=len(codes_info)) as pbar:
            try:
                for code_info in codes_info:
                    code = code_info['dm']
                    mc = code_info['mc']
                    today = datetime.today().strftime("%Y%m%d")
                    new_sdate = sdate
                    try:
                        try:
                            max_date=daily_db.execute(f"select max(d) from daily where code='{code}'").fetchall()[0][0]
                            if max_date:
                                max_date = datetime.strptime(max_date,"%Y-%m-%d %H:%M:%S") 
                                start_date = datetime.strptime(sdate,"%Y%m%d")
                                if max_date >= start_date:
                                    new_sdate = datetime.strftime(max_date + timedelta(days=1),"%Y%m%d")
                        except Exception as e:
                            print(code,mc,e)
                            error_msg="no_such_table"
                        finally:
                            if new_sdate<=today:
                                df=ak.stock_zh_a_hist(code,start_date=new_sdate)
                                if not df.empty:
                                    df=df.rename(columns={'日期':'d','股票代码':'code','开盘':'o','最高':'h','最低':'l','收盘':'c','成交量':'v','成交额':'e','振幅':'zf','涨跌幅':'zd','涨跌额':'zde','换手率':'hs'})
                                    df['mc']=mc
                                    df['d'] = pd.to_datetime(df['d'],format='%Y-%m-%d')
                                    if error_msg=="no_such_table":
                                        error_msg=""
                                        df.to_sql("daily",if_exists="replace",index=False,con=daily_db)
                                        daily_db.execute("create unique index daily_index on daily(code,d)")
                                    else:
                                        df.to_sql("daily",if_exists="append",index=False,con=daily_db)
                                else:
                                    print(f"no data begin of  {new_sdate} on {code}-{mc}")
                            else:
                                print(f"no need fetch {new_sdate} of {code}-{mc}")
                    except Exception as e:
                        error_code_info.append(f"{code}-{mc},{e}")
                    pbar.update(1)
            except KeyboardInterrupt:
                pbar.close()
                raise Exception("任务中断!!")
        return error_code_info
    def price(self):
        price_db=sqlite3.connect("price.db")
        table = "price"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:10]
        #codes_info=[{'dm':'300159','mc':'新研股份','jys':'sz'}]
        print("total codes:",len(codes_info))
        error_msg=""
        try:
            price_db.execute(f"select * from {table} limit 1")
        except Exception as e:
            error_msg="no_such_table"
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            error_code_info=[]
            date = datetime.today().strftime("%Y-%m-%d")
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                try:
                    df=ak.stock_intraday_em(code)
                    #df=ak.stock_zh_a_tick_tx_js(code)
                    if not df.empty:
                        df=df.rename(columns={'时间':'t','成交价':'p','手数':'v','买卖盘性质':'xz'})
                        df['e']=df.p*df.v*100
                        df['tt']=df['t'].apply(lambda x:'0' if x<'09:25:00' else '1' if x<'10:30:00' else '2' if x<'11:30:00' else '3' if x<'14:00:00' else '4')
                        df['vt'] = df['e'].apply(lambda x:'cdd' if x>=1000000 else 'dd' if x>=200000 else 'zd' if x>=40000 else 'xd')
                        df['cnt']=1
                        df1 = df.groupby(['xz','tt','p','vt'])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df1['code']=code
                        df1['date']=date
                        df1['mc']=mc
                        if error_msg=="no_such_table":
                            error_msg=""
                            print("no such table [price]")
                            df1.to_sql("price",if_exists="replace",index=False,con=price_db)
                            price_db.execute("create unique index index_price on price(code,date,xz,tt,p,vt)")
                        else:
                            df1.to_sql("price",if_exists="append",index=False,con=price_db)
                    else:
                        print(f"no data on {code}-{mc}")
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)
        return error_code_info
    def minute_pro(self):
        minute_pro_db=sqlite3.connect("minute_pro.db")
        table = "minute"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        print("total codes:",len(codes_info))
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            error_code_info=[]
            error_msg=""
            stime = '2022-01-01 00:00:00'
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                new_stime = stime
                max_time=None
                try:
                    max_time=minute_pro_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_time:
                        max_time = max_time[0][0]
                        d_max_time = datetime.strptime(max_time,"%Y-%m-%d %H:%M:%S") 
                        d_start_time = datetime.strptime(s_time,"%Y-%m-%d %H:%M:%S")
                        if d_max_time >= d_start_time:
                            new_stime = datetime.strftime(d_max_time + timedelta(minutes=-150),"%Y-%m-%d %H:%M:%S")
                    else:
                        print("no_info_record-->",code,mc,"max_time=",max_time,"stime=",stime)
                        error_msg = "no_info_table"
                except Exception as e:
                    print("no_minute_table-->",code,mc,"max_time=",max_time,"stime=",stime,"error=",e)
                    error_msg="no_minute_table"
                try:
                    origin_df = pd.read_sql(f"select * from {table} where code='{code}' and d >= '{new_sdate}'",daily_db)
                except:
                    pass    
    def daily_pro(self):
        daily_db=sqlite3.connect("daily.db")
        daily_pro_db=sqlite3.connect("daily_pro.db")
        table = "daily"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        print("total codes:",len(codes_info))
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            days=15
            error_code_info=[]
            error_msg=""
            sdate = '2022-01-01'
            try:
                total_max_date = daily_db.execute(f"select max(d) from {table}").fetchall()[0][0][:10]
                print(f"total_max_date on daily.db is {total_max_date}")
            except:
                raise Exception('没有找到daily.db，请先访问/ak/daily/refresh重新生成!')
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                new_sdate = sdate
                max_date=None
                try:
                    max_date=daily_pro_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_date:
                        max_date = max_date[0][0][:10]
                        if max_date>=total_max_date:
                            pbar.update(1)
                            continue   
                        d_max_date = datetime.strptime(max_date,"%Y-%m-%d") 
                        d_start_date = datetime.strptime(sdate,"%Y-%m-%d")
                        if d_max_date >= d_start_date:
                            new_sdate = datetime.strftime(d_max_date + timedelta(days=-150),"%Y-%m-%d")
                    else:
                        print("no_info_record-->",code,mc,"max_date=",max_date,"sdate=",sdate)
                        error_msg = "no_info_table"
                except Exception as e:
                    print("no_daily_table-->",code,mc,"max_date=",max_date,"sdate=",sdate,"error=",e)
                    error_msg="no_daily_table"
                try:
                    origin_df = pd.read_sql(f"select * from {table} where code='{code}' and d >= '{new_sdate}'",daily_db)
                    if origin_df.empty:
                        print(f"{code}-{mc} > new_sdate={new_sdate},没有找到该数据!")
                        pbar.update(1)
                        continue 
                    if max_date and max_date >= origin_df['d'].iloc[-1][:10]:
                        print(f"{code}-{mc},no deed because of data exists!")
                        pbar.update(1)
                        continue 
                    df = origin_df.reset_index() 
                    df_add_cols={}
                    # 后15天涨跌幅
                    for col in ['zd']:
                        for idx in range(days):
                            t=idx+1
                            df_add_cols[f'p{col}_{t}']=df[col].shift(-t)
                    # kdj指标
                    _kdj_df = self._kdj(df.c,df.l,df.h,9,3,3)
                    df_add_cols['kl'] = _kdj_df.kl
                    df_add_cols['dl'] = _kdj_df.dl
                    df_add_cols['jl'] = _kdj_df.jl
                    # macd指标
                    _macd_df = self._macd(df.c,12,26,9)
                    df_add_cols['dif'] = _macd_df.dif
                    df_add_cols['dea'] = _macd_df.dea
                    df_add_cols['macd'] = _macd_df.macd
                    # 当日股价高低位置
                    status_df= pd.Series(['N']*len(df))
                    high=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.9))
                    low=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.1))
                    status_df.loc[df['c'] > high] = 'H'
                    status_df.loc[df['c'] < low] = 'L'
                    df_add_cols['c_status'] = status_df
                    # 当日交易量缩放情况
                    status_df= pd.Series(['N']*len(df))
                    high=df['v'].rolling(window=120).apply(lambda x:x.mean() + x.std()*2)
                    low=df['v'].rolling(window=120).apply(lambda x:x.mean() - x.std()*2)
                    status_df.loc[df['v'] > high] = 'U'
                    status_df.loc[df['v'] < low] = 'D'
                    df_add_cols['v_status'] = status_df
                                        
                    # 近5,10,20,60,120交易日平均关键指标                    
                    for col in ['c','v','e','hs','zf']:
                        for idx in [5,10,20,40,60,120]:
                            df_add_cols[f'ma{idx}{col}']=df[col].rolling(window=idx).apply(lambda x:x.mean())        
                    
                    # 近5,10,20,60,120交易日累计交易量、交易额
                    for col in ['v','e']:
                        for idx in [5,10,20,40,60,120]:
                            df_add_cols[f'sum{idx}{col}']=df[col].rolling(window=idx).apply(lambda x:x.sum())        

                    # 近5,10,20,60,120交易日期间涨幅
                    #for col in ['zd']:
                    #    for idx in [4,9,19,59]:
                    #    df_add_cols[f'prod{idx+1}{col}']=df[col].rolling(window=idx).apply(lambda x:((1+x/100).prod()-1)*100)        
                    for col in ['c']:    
                        for idx in [4,9,19,39,59,119]:
                            #df_add_cols[f'prod{idx+1}{col}'] = (df[col] - df.shift(idx)[col]) / df.shift(idx)[col] * 100   
                            df_add_cols[f'pct{idx+1}{col}'] = df[col].pct_change(idx) * 100 
                    
                    # 前15天关键指标          
                    for col in ['o','c','h','l','zd','v','e','hs','zf']:
                        for idx in range(days):
                            t=idx+1
                            df_add_cols[f'{col}{t}']=df[col].shift(t)
                    #
                    df_cols = pd.concat(list(df_add_cols.values()), axis=1, keys=list(df_add_cols.keys()))
                    df = pd.concat([origin_df,df_cols],axis=1)
                    # 之前的kdj、macd
                    fields={'kl','dl','jl','dif','dea','macd'}
                    for key in fields:
                        df[f"{key}1"]=df[f"{key}"].shift(1)
                        df[f"{key}2"]=df[f"{key}"].shift(2)
                    # 连续上涨、下跌天数,正负数表示
                    # 连续缩放量天数,正负数表示
                    # 连续涨跌停天数,正负数表示
                    fields={'lxzd':'c','lxsf':'v','lxzdt':'zd'}
                    for key in fields:
                        df[key] = 0
                        for i in range(len(df)):
                            count = 0
                            for j in range(days-1):
                                j_str = '' if j==0 else str(j)
                                if key=='lxzdt':
                                    if df.loc[i, f"{fields[key]}{j_str}"] > 9.9:
                                        count += 1
                                    else:
                                        break
                                else:
                                    if df.loc[i, f"{fields[key]}{j_str}"] > df.loc[i, f"{fields[key]}{j+1}"]:
                                        count += 1
                                    else:
                                        break
                            if count==0:
                                for j in range(days-1):
                                    j_str = '' if j==0 else str(j)
                                    if key=='lxzdt':
                                        if df.loc[i, f"{fields[key]}{j_str}"] < -9.9:
                                            count += 1
                                        else:
                                            break
                                    else:
                                        if df.loc[i, f"{fields[key]}{j_str}"] <= df.loc[i, f"{fields[key]}{j+1}"]:
                                            count += 1
                                        else:
                                            break
                                count = count*-1 
                            df.at[i, key] = count
                
                    if max_date:
                        new_sdate = datetime.strftime(d_max_date + timedelta(days=1),"%Y-%m-%d")
                        df = df[df['d']>=new_sdate]
                    
                    info_max_date = df.iloc[-1]['d'][:10]
                    if error_msg=="no_daily_table":
                        error_msg=""
                        print("create daily & info table ->",code,info_max_date)
                        daily_pro_db.execute("create table info(code TEXT,max_d TEXT)")
                        daily_pro_db.execute("create unique index info_index on info(code)")
                        daily_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        df.to_sql("daily",if_exists="replace",index=False,con=daily_pro_db)
                        daily_pro_db.execute("create unique index daily_index on daily(code,d)")
                    elif error_msg=="no_info_table":
                        error_msg=""
                        print("insert info ->",code,info_max_date)
                        daily_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        daily_pro_db.commit()
                        df.to_sql("daily",if_exists="append",index=False,con=daily_pro_db)                        
                    else:
                        print("update info ->",code,info_max_date)
                        daily_pro_db.execute(f"update info set max_d = '{info_max_date}' where code = '{code}'")
                        daily_pro_db.commit()
                        df.to_sql("daily",if_exists="append",index=False,con=daily_pro_db)
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)
        return error_code_info
    
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
        cond = []
        if sdate:
            cond.append(f"d > '{sdate}'")
        if lxzd:
            cond.append(f"lxzd = {lxzd}") 
        if lxzdt:
            cond.append(f"lxzdt = {lxzdt}") 
        if lxsf:
            cond.append(f"lxsf = {lxsf}")
        cond_str = ' and '.join(cond)
        if cond_str:
            cond_str = ' and ' + cond_str
        print(cond_str)     
        df = pd.read_sql(f"select * from daily where code in ({codes_str}) {cond_str}",daily_pro_db)
        df = df.filter(['d','code','mc','o','c','h','l','v','e','zf','zd','hs',
                        'pzd_1','pzd_2','pzd_3','pzd_4','pzd_5',
                        'c_status','v_status','lxzd','lxsf',
                        'lxzdt','zd1','zd2','zd3','zd4','kl','dl','jl','dif','dea','macd','ma20c','ma40c',
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

    def register_router(self):
        @self.router.get("/daily/refresh")
        async def daily_refresh(req:Request):
            """更新日线数据"""
            sdate = req.query_params.get('sdate')
            error_code_info=[]
            if sdate:
                sdate=sdate.replace('-','')
                error_code_info=self.daily_refresh(sdate)
            else:
                error_code_info=self.daily_refresh()
            return {"message":f"daily refresh已完成,error_code_info={error_code_info}"}
        @self.router.get("/daily/pro")
        async def daily_pro(req:Request):
            """更新日线增强数据"""
            error_code_info=self.daily_pro()
            return {"message":f"daily_pro已完成,error_code_info={error_code_info}"}
        @self.router.get("/price")
        async def price(req:Request):
            """更新价格数据"""
            error_code_info=self.price()
            return {"message":f"price已完成,error_code_info={error_code_info}"}
       
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
                if not lxzd and not lxzdt and not lxsf:
                    raise Exception("至少须指定lxzd,lxsf,lxzdt中的一个参数")
                df = self.fx2(codes,sdate,{'lxzd':lxzd,'lxsf':lxsf,'lxzdt':lxzdt})
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['code','mc'])
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
        
        @self.router.get("/current")
        async def current(req:Request):
            """获取当前行情数据"""
            try:
                df = self.current()
                df = self._prepare_df(df,req)
                content = self._to_html(df)
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
        @self.router.get("/bk/refresh")
        async def _bk_refresh(req:Request):
            """分析连续下跌信息"""
            try:
                #return self._get_akbk_code_info('BK0695')
                #return self._get_akbk_codes('BK1027')
                #return self._get_bk_codes('chgn_700129')
                return self.bk_refresh()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
    
