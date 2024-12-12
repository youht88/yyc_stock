from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase

class AK_REFRESH(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()
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

    def daily_pro(self):
        daily_db=sqlite3.connect("daily.db")
        daily_pro_db=sqlite3.connect("daily_pro.db")
        table = "daily"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        print("total codes:",len(codes_info))
        flow_cols={'日期': 'd', '主力净流入-净额': 'zljlr', '主力净流入-净占比': 'zljlrl', 
                              '超大单净流入-净额': 'cddjlr', '超大单净流入-净占比': 'cddjlrl', '大单净流入-净额': 'ddjlr', '大单净流入-净占比': 'ddjlrl',
                              '中单净流入-净额': 'zdjlr', '中单净流入-净占比': 'zdjlrl', '小单净流入-净额': 'xdjlr', '小单净流入-净占比': 'xdjlrl'}
                    
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            days=15
            error_code_info=[]
            error_msg=""
            sdate = '2024-01-01'
            try:
                total_max_date = daily_db.execute(f"select max(d) from {table}").fetchall()[0][0][:10]
                print(f"total_max_date on daily.db is {total_max_date}")
            except:
                raise Exception('没有找到daily.db，请先访问/ak/daily/refresh重新生成!')
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                jys = code_info['jys']
                new_sdate = sdate
                max_date=None
                try:
                    max_date=daily_pro_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_date:
                        max_date = max_date[0][0]
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
                    if max_date and max_date >= origin_df['d'].iloc[-1]:
                        print(f"{code}-{mc},no deed because of data exists!")
                        pbar.update(1)
                        continue 
                    df = origin_df.reset_index() 
                    df.d = pd.to_datetime(df.d)
                    
                    df_flow = ak.stock_individual_fund_flow(code, market=jys)
                    df_flow = df_flow.rename(columns=flow_cols).drop(columns=["收盘价","涨跌幅"])
                    df_flow.d = pd.to_datetime(df_flow.d)
                    
                    df = pd.merge(df,df_flow,on='d',how="left")
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
                    for col in ['o','c','h','l','zd','v','e','hs','zf','zljlr','zljlrl','cddjlrl','ddjlrl','zdjlrl','xdjlrl']:
                        for idx in range(days):
                            t=idx+1
                            df_add_cols[f'{col}{t}']=df[col].shift(t)
                    #
                    df_cols = pd.concat(list(df_add_cols.values()), axis=1, keys=list(df_add_cols.keys()))
                    df = pd.concat([df,df_cols],axis=1)
                    # 之前的kdj、macd、ma
                    fields={'kl','dl','jl','dif','dea','macd','ma5c','ma10c','ma20c'}
                    for key in fields:
                        df[f"{key}1"]=df[f"{key}"].shift(1)
                        df[f"{key}2"]=df[f"{key}"].shift(2)
                    # 连续上涨、下跌天数,正负数表示
                    # 连续缩放量天数,正负数表示
                    # 连续涨跌停天数,正负数表示
                    fields={'lxzd':'c','lxsf':'v','lxzdt':'zd','lxzljlr':'zljlr'}
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
                                elif key=='lxzljlr':
                                    if df.loc[i, f"{fields[key]}{j_str}"] > 0:
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
                                    elif key=='lxzljlr':
                                        if df.loc[i, f"{fields[key]}{j_str}"] < 0:
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
                    info_max_date = df.iloc[-1]['d'].strftime("%Y-%m-%d")
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

    def minute_pro(self):
        minute_pro_db=sqlite3.connect("minute_pro.db")
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        print("total codes:",len(codes_info))
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            error_code_info=[]
            error_msg=""
            sdate = '2024-01-01'
            period=15
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                new_sdate = sdate
                max_date=None
                try:
                    max_date=minute_pro_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_date:
                        max_date = max_date[0][0]
                        d_max_date = datetime.strptime(max_date,"%Y-%m-%d") 
                        d_start_date = datetime.strptime(sdate,"%Y-%m-%d")
                        if d_max_date >= d_start_date:
                            new_sdate = datetime.strftime(d_max_date + timedelta(days=-30),"%Y-%m-%d")
                    else:
                        print("no_info_record-->",code,mc,"max_date=",max_date,"sdate=",sdate)
                        error_msg = "no_info_table"
                except Exception as e:
                    print("no_minute_table-->",code,mc,"max_date=",max_date,"sdate=",sdate,"error=",e)
                    error_msg="no_minute_table"
                try:
                    if max_date and max_date >= datetime.today().strftime("%Y-%m-%d"):
                        print(f"{code}-{mc},no deed because of data exists!")
                        pbar.update(1)
                        continue 
                    origin_df = ak.stock_zh_a_hist_min_em(code,start_date=new_sdate,period=15)
                    if origin_df.empty:
                        print(f"{code}-{mc} > new_sdate={new_sdate},没有找到该数据!")
                        pbar.update(1)
                        continue
                    origin_df['code']=code
                    origin_df['mc']=mc 
                    origin_df = origin_df.rename(columns={"时间":"t","开盘":"o","收盘":"c","最高":"h","最低":"l",
                                                          "涨跌幅":"zd","涨跌额":"zde","成交量":"v","成交额":"e","振幅":"zf","换手率":"hs"})
                    df = origin_df.copy().reset_index() 
                    df_add_cols={}
                    # 后15个周期涨跌幅
                    for col in ['zd']:
                        for idx in range(period):
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
                        for idx in range(period):
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
                            for j in range(period-1):
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
                                for j in range(period-1):
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
                        df = df[df['t']>=new_sdate]
                    
                    info_max_date = df.iloc[-1]['t'][:10]
                    if error_msg=="no_minute_table":
                        error_msg=""
                        print("create minute & info table ->",code,info_max_date)
                        minute_pro_db.execute("create table info(code TEXT,max_d TEXT)")
                        minute_pro_db.execute("create unique index info_index on info(code)")
                        minute_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        df.to_sql("minute",if_exists="replace",index=False,con=minute_pro_db)
                        minute_pro_db.execute("create unique index minute_index on minute(code,t)")
                    elif error_msg=="no_info_table":
                        error_msg=""
                        print("insert info ->",code,info_max_date)
                        minute_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        minute_pro_db.commit()
                        df.to_sql("minute",if_exists="append",index=False,con=minute_pro_db)                        
                    else:
                        print("update info ->",code,info_max_date)
                        minute_pro_db.execute(f"update info set max_d = '{info_max_date}' where code = '{code}'")
                        minute_pro_db.commit()
                        df.to_sql("minute",if_exists="append",index=False,con=minute_pro_db)
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)         
        return error_code_info   
    
    def price_hist(self,delta=30):
        price_db=sqlite3.connect(f"price_{delta}.db")
        jhjj_db=sqlite3.connect("jhjj.db")
        table = "price"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:10]
        #codes_info=[{'dm':'300159','mc':'新研股份','jys':'sz'}]
        print("total codes:",len(codes_info))
        price_error_msg=""
        jhjj_error_msg=""
        if int(datetime.today().strftime("%H"))<16:
            print("today=",datetime.today())
            raise Exception("请在当天交易结束后,16点后执行")
        try:
            price_db.execute(f"select * from {table} limit 1")
        except Exception as e:
            price_error_msg="no_price_db"
        try:
            jhjj_db.execute(f"select * from {table} limit 1")
        except Exception as e:
            jhjj_error_msg="no_jhjj_db"

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
                        df_jhjj = df[df.t<'09:25:00'].copy()
                        df_jhjj['tt'] = 0
                        df_jhjj['v0']=df_jhjj.groupby(['tt','p']).v.diff().fillna(df_jhjj.v)
                        df_jhjj['e']=df_jhjj.p*df_jhjj.v0*100
                        df_jhjj['vt'] = df_jhjj['e'].apply(lambda x:'cdd' if abs(x)>=1000000 else 'dd' if abs(x)>=200000 else 'zd' if abs(x)>=40000 else 'xd')
                        df_jhjj['cnt']=1
                        df_jhjj = df_jhjj.groupby(['tt','p','vt'])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df_jhjj['code']=code
                        df_jhjj['date']=date
                        df_jhjj['mc']=mc
                        if jhjj_error_msg=="no_jhjj_db":
                            jhjj_error_msg=""
                            print("no such table [jhjj]")
                            df_jhjj.to_sql("price",if_exists="replace",index=False,con=jhjj_db)
                            jhjj_db.execute("create unique index index_price on price(code,date,tt,p,vt)")
                        else:
                            try:
                                df_jhjj.to_sql("price",if_exists="append",index=False,con=jhjj_db)
                            except:
                                pass
                        df_price = df[df.t>='09:25:00'].copy()
                        df_price['t'] = pd.to_datetime(df_price['t'],format='%H:%M:%S')
                        df_price['tx'] = (df_price.t.dt.hour*3600+df_price.t.dt.minute*60+df_price.t.dt.second)//delta*delta
                        df_price['tx'] = pd.to_datetime(df_price['tx'], unit='s').dt.strftime('%H:%M:%S')
                        df_price['e']=df_price.p*df_price.v*100
                        df_price['cnt']=1
                        df_price = df_price.groupby(by=["xz","tx","p"])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df_price['tt']=df_price['tx'].apply(lambda x:0 if x<'09:25:00' else 1 if x<'10:30:00' else 2 if x<'11:30:00' else 3 if x<'14:00:00' else 4)
                        df_price['vt'] = df_price['e'].apply(lambda x:'cdd' if x>=1000000 else 'dd' if x>=200000 else 'zd' if x>=40000 else 'xd')
                        df_price = df_price.groupby(['xz','tt','p','vt'])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
                        df_price_s = df_price[df_price['xz']=='卖盘']
                        df_price_b = df_price[df_price['xz']=='买盘']
                        df_price = pd.merge(df_price_s,df_price_b,on=['p','tt','vt'],how='outer',suffixes=['_s','_b'])
                        df_price = df_price.drop(columns=['xz_s','xz_b']).fillna(0)
                        df_price['v_jlr']=df_price['v_b']-df_price['v_s']
                        df_price['e_jlr']=df_price['e_b']-df_price['e_s']
                        df_price['code']=code
                        df_price['date']=date
                        df_price['mc']=mc
                        if price_error_msg=="no_price_db":
                            price_error_msg=""
                            print("no such table [price]")
                            df_price.to_sql("price",if_exists="replace",index=False,con=price_db)
                            price_db.execute("create unique index index_price on price(code,date,tt,p,vt)")
                        else:
                            df_price.to_sql("price",if_exists="append",index=False,con=price_db)
                    else:
                        print(f"no data on {code}-{mc}")
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)
        return error_code_info

    def register_router(self):
        @self.router.get("/refresh/bk")
        async def _refresh_bk(req:Request):
            """分析连续下跌信息"""
            try:
                #return self._get_akbk_code_info('BK0695')
                #return self._get_akbk_codes('BK1027')
                #return self._get_bk_codes('chgn_700129')
                return self.bk_refresh()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/refresh/daily")
        async def _refresh_refresh(req:Request):
            """更新日线数据"""
            sdate = req.query_params.get('sdate')
            error_code_info=[]
            if sdate:
                sdate=sdate.replace('-','')
                error_code_info=self.daily_refresh(sdate)
            else:
                error_code_info=self.daily_refresh()
            return {"message":f"daily refresh已完成,error_code_info={error_code_info}"}
        @self.router.get("/refresh/daily_pro")
        async def _refresh_daily_pro(req:Request):
            """更新日线增强数据"""
            error_code_info=self.daily_pro()
            return {"message":f"daily_pro已完成,error_code_info={error_code_info}"}
        @self.router.get("/refresh/minute_pro")
        async def _refresh_minute_pro(req:Request):
            """更新15分钟增强数据"""
            error_code_info=self.minute_pro()
            return {"message":f"minute_pro已完成,error_code_info={error_code_info}"}
        @self.router.get("/refresh/price/{delta}")
        async def _refresh_price(delta:int,req:Request):
            """更新价格数据"""
            try:
                error_code_info=self.price_hist(delta)
                return {"message":f"price已完成,error_code_info={error_code_info}"}
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")       