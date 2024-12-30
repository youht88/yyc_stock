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
    def akbk_refresh(self):
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
    def update_daily_pro(self):
        daily_pro_db=sqlite3.connect("daily_pro.db")
        table = "daily"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            days=15
            error_code_info=[]
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                try:
                    df = pd.read_sql(f"select code,date,zd,fzd from {table} where code='{code}' and fzd like 'SOME:%'",daily_pro_db)
                    if df.empty:
                        pbar.update(1)
                        continue
                    df.date = pd.to_datetime(df.date)
                    start_date = datetime.strftime(df.date.min(),'%Y-%m-%d')
                    end_date = datetime.strftime(df.date.max() + timedelta(days=15),"%Y-%m-%d") 
                    daily_df = ak.stock_zh_a_hist(code,start_date=start_date.replace('-',''),end_date=end_date.replace('-',''))
                    daily_df.日期=pd.to_datetime(daily_df.日期)
                    for idx in range(days):
                        t=idx+1
                        daily_df[f'fzd_{t}']=daily_df['涨跌幅'].shift(-t)
                    for date in df.date:
                        date_str = datetime.strftime(date,'%Y-%m-%d')
                        daily_date_df = daily_df[daily_df.日期==date_str]
                        data = daily_date_df.filter(like='fzd').to_dict(orient='records')[0]
                        fzd = str({int(key.split('_')[1]): value for key, value in data.items()})
                        if not all(str(item)!='nan' for item in data.values()):
                            fzd = 'SOME:'+fzd
                        daily_pro_db.execute(f"update {table} set fzd='{fzd}' where code='{code}' and date='{date_str}'")
                    daily_pro_db.commit()
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")    
                pbar.update(1)
        return error_code_info
    
    def daily_pro(self):
        daily_pro_db=sqlite3.connect("daily_pro.db")
        table = "daily"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:5]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        print("total codes:",len(codes_info))
        flow_cols={'日期': 'date', '主力净流入-净额': 'zljlr', '主力净流入-净占比': 'zljlrl', 
                              '超大单净流入-净额': 'cddjlr', '超大单净流入-净占比': 'cddjlrl', '大单净流入-净额': 'ddjlr', '大单净流入-净占比': 'ddjlrl',
                              '中单净流入-净额': 'zdjlr', '中单净流入-净占比': 'zdjlrl', '小单净流入-净额': 'xdjlr', '小单净流入-净占比': 'xdjlrl'}
        cmf_cols={'日期':'date','获利比例':'hlbl','平均成本':'pjcb','90成本-低':'cb90d','90成本-高':'cb90g','90集中度':'jzd90',
                  '70成本-低':'cb70d','70成本-高':'cb70g','70集中度':'jzd70'}
        szindex_cols={'日期':'date','开盘':'szo','收盘':'szc','最高':'szh','最低':'szl','成交量':'szv','成交额':'sze','振幅':'szzf','涨跌幅':'szzd','涨跌额':'szzde','换手率':'szhs'}            

        df_szindex = ak.index_zh_a_hist('000001')
        df_szindex = df_szindex.rename(columns=szindex_cols)
        df_szindex.date = pd.to_datetime(df_szindex.date)            
        
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            days=15
            error_code_info=[]
            error_msg=""
            sdate = '2024-01-01'
            today = datetime.today().strftime('%Y-%m-%d')
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                jys = code_info['jys']
                jysc = self._get_stock_jysc(code)
                new_sdate = sdate
                max_date=None
                try:
                    max_date=daily_pro_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_date:
                        max_date = max_date[0][0]
                        if max_date>=today:
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
                    #origin_df = pd.read_sql(f"select * from {table} where code='{code}' and d >= '{new_sdate}'",daily_db)
                    origin_df = ak.stock_zh_a_hist(code,start_date=new_sdate.replace('-',''))
                    if origin_df.empty:
                        print(f"{code}-{mc} > new_sdate={new_sdate},没有找到该数据!")
                        pbar.update(1)
                        continue 
                    
                    origin_df=origin_df.rename(columns={'日期':'date','股票代码':'code','开盘':'o','最高':'h','最低':'l','收盘':'c','成交量':'v','成交额':'e','振幅':'zf','涨跌幅':'zd','涨跌额':'zde','换手率':'hs'})
                    origin_df['code']=code
                    origin_df['mc'] = mc
                    origin_df['jys'] = jys
                    origin_df['jysc'] = jysc
                    origin_df.date = pd.to_datetime(origin_df.date)
                    
                    if max_date and d_max_date >= origin_df['date'].iloc[-1]:
                        print(f"{code}-{mc},no deed because of data exists!")
                        pbar.update(1)
                        continue 
                    df = origin_df.reset_index() 
                    
                    df_flow = ak.stock_individual_fund_flow(code, market=jys)
                    df_flow = df_flow.rename(columns=flow_cols).drop(columns=["收盘价","涨跌幅"])
                    df_flow.date = pd.to_datetime(df_flow.date)            
                    df = pd.merge(df,df_flow,on='date',how="left")

                    df_cmf = ak.stock_cyq_em(code)
                    df_cmf = df_cmf.rename(columns=cmf_cols)
                    df_cmf.date = pd.to_datetime(df_cmf.date)            
                    df = pd.merge(df,df_cmf,on='date',how="left")
                    
                    df = pd.merge(df,df_szindex,on='date',how="left")
                    
                    df_add_cols={}
                    # 后15天涨跌幅
                    # 改为初始设置为空，由update daily_pro来更新
                    df_add_cols['fzd']=pd.Series(['SOME:']*len(df))
                    # for col in ['zd']:
                    #     for idx in range(days):
                    #         t=idx+1
                    #         df_add_cols[f'p{col}_{t}']=df[col].shift(-t)
                    # kdj指标
                    _kdj_df = self._kdj(df.c,df.l,df.h,9,3,3)
                    df_add_cols['kl'] = _kdj_df.kl
                    df_add_cols['dl'] = _kdj_df.dl
                    df_add_cols['jl'] = _kdj_df.jl
                    _sz_kdj_df = self._kdj(df.szc,df.szl,df.szh,9,3,3)
                    df_add_cols['szkl'] = _sz_kdj_df.kl
                    df_add_cols['szdl'] = _sz_kdj_df.dl
                    df_add_cols['szjl'] = _sz_kdj_df.jl
                    # macd指标
                    _macd_df = self._macd(df.c,12,26,9)
                    df_add_cols['dif'] = _macd_df.dif
                    df_add_cols['dea'] = _macd_df.dea
                    df_add_cols['macd'] = _macd_df.macd
                    _sz_macd_df = self._macd(df.szc,12,26,9)
                    df_add_cols['szdif'] = _sz_macd_df.dif
                    df_add_cols['szdea'] = _sz_macd_df.dea
                    df_add_cols['szmacd'] = _sz_macd_df.macd
                    # 当日股价高低位置
                    status_df= pd.Series(['M']*len(df))
                    high=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.9))
                    low=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.1))
                    status_df.loc[df['c'] > high] = 'U'
                    status_df.loc[df['c'] < low] = 'D'
                    df_add_cols['c_status'] = status_df
                    # 当日交易量缩放情况
                    status_df= pd.Series(['M']*len(df))
                    high=df['v'].rolling(window=120).apply(lambda x:x.mean() + x.std()*2)
                    low=df['v'].rolling(window=120).apply(lambda x:x.mean() - x.std()*2)
                    status_df.loc[df['v'] > high] = 'U'
                    status_df.loc[df['v'] < low] = 'D'
                    df_add_cols['v_status'] = status_df
                                        
                    # 近5,10,20,60,120交易日平均关键指标                    
                    for col in ['c','v','e','hs','zf','szc']:
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
                    for col in ['o','c','h','l','zd','v','e','hs','zf','zljlr','zljlrl','hlbl','pjcb','jzd90','jzd70','szc','szzd','szv']:
                        for idx in range(days):
                            t=idx+1
                            df_add_cols[f'{col}{t}']=df[col].shift(t)
                    #
                    df_cols = pd.concat(list(df_add_cols.values()), axis=1, keys=list(df_add_cols.keys()))
                    df = pd.concat([df,df_cols],axis=1)
                    # 之前的kdj、macd、ma
                    fields={'kl','dl','jl','dif','dea','macd','ma5c','ma10c','ma20c',
                            'szkl','szdl','szjl','szdif','szdea','szmacd','ma5szc','ma10szc','ma20szc'}
                    for key in fields:
                        df[f"{key}1"]=df[f"{key}"].shift(1)
                        df[f"{key}2"]=df[f"{key}"].shift(2)
                    # 连续上涨、下跌天数,正负数表示
                    # 连续缩放量天数,正负数表示
                    # 连续涨跌停天数,正负数表示
                    fields={'lxzd':'c','lxsf':'v','lxzdt':'zd','lxzljlr':'zljlr','lxszzd':'szzd','lxszsf':'szv'}
                    zdtbz=9.9
                    if jysc=='cyb' or jysc=='kcb':
                        zdtbz=19.9
                    elif jysc=='bj':
                        zdtbz=29.9
                    for key in fields:
                        df[key] = 0
                        for i in range(len(df)):
                            count = 0
                            for j in range(days-1):
                                j_str = '' if j==0 else str(j)
                                if key=='lxzdt':
                                    if df.loc[i, f"{fields[key]}{j_str}"] > zdtbz:
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
                                        if df.loc[i, f"{fields[key]}{j_str}"] < -zdtbz:
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
                        df = df[df['date']>=new_sdate]
                    info_max_date = df.iloc[-1]['date'].strftime("%Y-%m-%d")
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    if error_msg=="no_daily_table":
                        error_msg=""
                        print("create daily & info table ->",code,info_max_date)
                        daily_pro_db.execute("create table info(code TEXT,max_d TEXT)")
                        daily_pro_db.execute("create unique index info_index on info(code)")
                        daily_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        df.to_sql("daily",if_exists="replace",index=False,con=daily_pro_db)
                        daily_pro_db.execute("create unique index daily_index on daily(code,date)")
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

    def update_minute_pro(self):
        minute_pro_db=sqlite3.connect("minute_pro.db")
        table = "minute"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:100]
        #codes_info=[{'dm':'300159','mc':'新研股份'}]
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            period=15
            error_code_info=[]
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                try:
                    df = pd.read_sql(f"select code,date,time,zd,fzd from {table} where code='{code}' and fzd like 'SOME:%'",minute_pro_db)
                    if df.empty:
                        pbar.update(1)
                        continue
                    df['datetime'] = pd.to_datetime(df.date+' '+df.time)
                    start_datetime = datetime.strftime(df.datetime.min(),'%Y-%m-%d %H:%M:%S')
                    end_datetime = datetime.strftime(df.datetime.max() + timedelta(minutes=300),"%Y-%m-%d %H:%M:%S") 
                    minute_df = ak.stock_zh_a_hist_min_em(code,start_date=start_datetime,end_date=end_datetime,period=15)
                    minute_df['时间'] = pd.to_datetime(minute_df.时间)
                    minute_df['date'] = minute_df.时间.dt.strftime('%Y-%m-%d')
                    minute_df['time'] = minute_df.时间.dt.strftime('%H:%M:%S')
                    for idx in range(period):
                        t=idx+1
                        minute_df[f'fzd_{t}']=minute_df['涨跌幅'].shift(-t)
                    for row in df.itertuples(index=True, name='Pandas'):
                        date_str = datetime.strftime(row.datetime,'%Y-%m-%d')
                        time_str = datetime.strftime(row.datetime,'%H:%M:%S')
                        minute_pice_df = minute_df[(minute_df.date==date_str) & (minute_df.time==time_str)]
                        data = minute_pice_df.filter(like='fzd').to_dict(orient='records')[0]
                        fzd = str({int(key.split('_')[1]): value for key, value in data.items()})
                        if not all(str(item)!='nan' for item in data.values()):
                            fzd = 'SOME:'+fzd
                        minute_pro_db.execute(f"update {table} set fzd='{fzd}' where code='{code}' and date='{date_str}' and time='{time_str}'")
                    minute_pro_db.commit()
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

        szindex_cols={'时间':'datetime','开盘':'szo','收盘':'szc','最高':'szh','最低':'szl','成交量':'szv','成交额':'sze','振幅':'szzf','涨跌幅':'szzd','涨跌额':'szzde','换手率':'szhs'}            

        df_szindex = ak.index_zh_a_hist_min_em('000001',period=15)
        df_szindex = df_szindex.rename(columns=szindex_cols)
        df_szindex['datetime'] = pd.to_datetime(df_szindex.datetime)
        df_szindex['date'] = df_szindex.datetime.dt.strftime('%Y-%m-%d')
        df_szindex['time'] = df_szindex.datetime.dt.strftime('%H:%M:%S')
        df_szindex.drop(columns='datetime',inplace=True)
        with tqdm(total=len(codes_info),desc="进度") as pbar:
            error_code_info=[]
            error_msg=""
            sdate = '2024-01-01'
            period=15
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                jys = code_info['jys']
                jysc = self._get_stock_jysc(code)
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
                    origin_df['jys']=jys
                    origin_df['jysc']=jysc
                    origin_df = origin_df.rename(columns={"时间":"datetime","开盘":"o","收盘":"c","最高":"h","最低":"l",
                                                          "涨跌幅":"zd","涨跌额":"zde","成交量":"v","成交额":"e","振幅":"zf","换手率":"hs"})
                    origin_df['datetime'] = pd.to_datetime(origin_df.datetime)
                    origin_df['date'] = origin_df.datetime.dt.strftime('%Y-%m-%d')
                    origin_df['time'] = origin_df.datetime.dt.strftime('%H:%M:%S')
                    origin_df.drop(columns='datetime',inplace=True)
                    df = origin_df.copy().reset_index() 

                    df = pd.merge(df,df_szindex,on=['date','time'],how="left")
                    df_add_cols={}
                    # 后15个周期涨跌幅
                    # 初始设置为空，由update minute_pro来更新
                    df_add_cols['fzd']=pd.Series(['SOME:']*len(df))
                    # for col in ['zd']:
                    #     for idx in range(period):
                    #         t=idx+1
                    #         df_add_cols[f'p{col}_{t}']=df[col].shift(-t)
                    # kdj指标
                    _kdj_df = self._kdj(df.c,df.l,df.h,9,3,3)
                    df_add_cols['kl'] = _kdj_df.kl
                    df_add_cols['dl'] = _kdj_df.dl
                    df_add_cols['jl'] = _kdj_df.jl
                    _sz_kdj_df = self._kdj(df.szc,df.szl,df.szh,9,3,3)
                    df_add_cols['szkl'] = _sz_kdj_df.kl
                    df_add_cols['szdl'] = _sz_kdj_df.dl
                    df_add_cols['szjl'] = _sz_kdj_df.jl
                    # macd指标
                    _macd_df = self._macd(df.c,12,26,9)
                    df_add_cols['dif'] = _macd_df.dif
                    df_add_cols['dea'] = _macd_df.dea
                    df_add_cols['macd'] = _macd_df.macd
                    _sz_macd_df = self._macd(df.szc,12,26,9)
                    df_add_cols['szdif'] = _sz_macd_df.dif
                    df_add_cols['szdea'] = _sz_macd_df.dea
                    df_add_cols['szmacd'] = _sz_macd_df.macd
                    # 当日股价高低位置
                    status_df= pd.Series(['M']*len(df))
                    high=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.9))
                    low=df['c'].rolling(window=120).apply(lambda x:x.quantile(0.1))
                    status_df.loc[df['c'] > high] = 'U'
                    status_df.loc[df['c'] < low] = 'D'
                    df_add_cols['c_status'] = status_df
                    # 当日交易量缩放情况
                    status_df= pd.Series(['M']*len(df))
                    high=df['v'].rolling(window=120).apply(lambda x:x.mean() + x.std()*2)
                    low=df['v'].rolling(window=120).apply(lambda x:x.mean() - x.std()*2)
                    status_df.loc[df['v'] > high] = 'U'
                    status_df.loc[df['v'] < low] = 'D'
                    df_add_cols['v_status'] = status_df
                                        
                    # 近5,10,20,60,120交易日平均关键指标                    
                    for col in ['c','v','e','hs','zf','szc']:
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
                    for col in ['o','c','h','l','zd','v','e','hs','zf','szc','szzd','szv']:
                        for idx in range(period):
                            t=idx+1
                            df_add_cols[f'{col}{t}']=df[col].shift(t)
                    #
                    df_cols = pd.concat(list(df_add_cols.values()), axis=1, keys=list(df_add_cols.keys()))
                    df = pd.concat([df,df_cols],axis=1)
                    # 之前的kdj、macd、ma
                    fields={'kl','dl','jl','dif','dea','macd','ma5c','ma10c','ma20c',
                            'szkl','szdl','szjl','szdif','szdea','szmacd','ma5szc','ma10szc','ma20szc'}
                    for key in fields:
                        df[f"{key}1"]=df[f"{key}"].shift(1)
                        df[f"{key}2"]=df[f"{key}"].shift(2)
                    # 连续上涨、下跌天数,正负数表示
                    # 连续缩放量天数,正负数表示
                    # 连续涨跌停天数,正负数表示
                    fields={'lxzd':'c','lxsf':'v','lxzdt':'zd','lxszzd':'szzd','lxszsf':'szv'}
                    zdtbz=9.9
                    if jysc=='cyb' or jysc=='kcb':
                        zdtbz=19.9
                    elif jysc=='bj':
                        zdtbz=29.9
                    for key in fields:
                        df[key] = 0
                        for i in range(len(df)):
                            count = 0
                            for j in range(period-1):
                                j_str = '' if j==0 else str(j)
                                if key=='lxzdt':
                                    if df.loc[i, f"{fields[key]}{j_str}"] > zdtbz:
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
                                        if df.loc[i, f"{fields[key]}{j_str}"] < -zdtbz:
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
                        df = df[df['date']>=new_sdate]
                    
                    info_max_date = df.iloc[-1]['date']
                    if error_msg=="no_minute_table":
                        error_msg=""
                        print("create minute & info table ->",code,info_max_date)
                        minute_pro_db.execute("create table info(code TEXT,max_d TEXT)")
                        minute_pro_db.execute("create unique index info_index on info(code)")
                        minute_pro_db.execute(f"insert into info values('{code}','{info_max_date}')")
                        df.to_sql("minute",if_exists="replace",index=False,con=minute_pro_db)
                        minute_pro_db.execute("create unique index minute_index on minute(code,date,time)")
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
    
    def price_hist_old(self,delta=30):
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
    
    def price_refresh(self):
        price_db=sqlite3.connect(f"price.db")
        jhjj_db=sqlite3.connect("jhjj.db")
        table = "price"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:10]
        #codes_info=[{'dm':'300096','mc':'ST易联众','jys':'sz'}]
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
            today_total_df = ak.stock_zh_a_spot_em()
            today_total_df = today_total_df.rename(columns={'序号':'index', '代码':'code', '名称':'mc', '最新价':'c', '涨跌幅':'zd', '涨跌额':'zde', '成交量':'v', '成交额':'e',
                                                     '振幅':'zf', '最高':'h', '最低':'l','今开':'o', '昨收':'zc', '量比':'lb', '换手率':'hs', '市盈率-动态':'pe', 
                                                     '市净率':'sjl', '总市值':'zsz', '流通市值':'ltsz', '涨速':'zs', 
                                                     '5分钟涨跌':'min5zd','60日涨跌幅':'day60zd', '年初至今涨跌幅':'year1zd'})
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                try:
                    df_today = today_total_df[today_total_df['code']==code]
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
                        p_mean = df_price.p.mean()
                        p_std = df_price.p.std()
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
                        df_price['p_mean'] = p_mean
                        df_price['p_std'] = p_std
                        df_price['p_zscore'] = (df_price['p'] - p_mean)/p_std
                        if price_error_msg=="no_price_db":
                            price_error_msg=""
                            print("no such table [price]")
                            df_price.to_sql("price",if_exists="replace",index=False,con=price_db)
                            price_db.execute("create unique index index_price on price(code,date,p)")
                        else:
                            df_price.to_sql("price",if_exists="append",index=False,con=price_db)
                    else:
                        print(f"no data on {code}-{mc}")
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)
        return error_code_info

    def cmf_refresh(self):
        cmf_db=sqlite3.connect(f"cmf.db")
        table = "cmf"
        codes_info = self._get_bk_codes("hs_a")
        #codes_info = codes_info[:10]
        #codes_info=[{'dm':'300159','mc':'新研股份','jys':'sz'}]
        print("total codes:",len(codes_info))
        cmf_error_msg=""
        today=datetime.today().strftime("%Y-%m-%d")
        if int(datetime.today().strftime("%H"))<16:
            print("today=",datetime.today())
            raise Exception("请在当天交易结束后,16点后执行")
        try:
            cmf_db.execute(f"select * from {table} limit 1")
        except Exception as e:
            cmf_error_msg="no_cmf_db"

        with tqdm(total=len(codes_info),desc="进度") as pbar:
            error_code_info=[]                
            for code_info in codes_info:
                code = code_info['dm']
                mc = code_info['mc']
                try:
                    max_info_date=cmf_db.execute(f"select max_d from info where code='{code}'").fetchall()
                    if max_info_date:
                        max_info_date = max_info_date[0][0]
                        info_error_msg=""
                        if max_info_date >= today:
                            pbar.update(1)
                            continue
                    else:
                        max_info_date = None
                        print("no_info_record-->",code,mc,"max_date=",max_info_date)
                        info_error_msg = "no_info_record"
                except:
                    info_error_msg = "no_info_table"
                    max_info_date = None
                try:
                    df=ak.stock_cyq_em(code)
                    if not df.empty:
                        df=df.rename(columns={'日期':'date','获利比例':'hlbl','平均成本':'pjcb','90成本-低':'cb90d','90成本-高':'cb90g','90集中度':'jzd90','70成本-低':'cb70d','70成本-高':'cb70g','70集中度':'jzd70'})
                        df['code']=code
                        df['mc']=mc
                        df['date'] = pd.to_datetime(df['date'])
                        max_cmf_date = df['date'].max().strftime('%Y-%m-%d')
                        if max_info_date:
                            df = df[df['date'] > max_info_date]
                        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                        if not df.empty:
                            if cmf_error_msg=="no_cmf_db":
                                cmf_error_msg=""
                                print("no such table [cmf]")
                                df.to_sql(table,if_exists="replace",index=False,con=cmf_db)
                                cmf_db.execute("create unique index index_cmf on cmf(code,date)")
                                cmf_db.execute("create table info(code TEXT,max_d TEXT)")
                                cmf_db.execute("create unique index info_index on info(code)")
                                cmf_db.execute(f"insert into info values('{code}','{max_cmf_date}')")
                            elif info_error_msg=="no_info_record":
                                info_error_msg=""
                                cmf_db.execute(f"insert into info values('{code}','{max_cmf_date}')")
                                cmf_db.commit()
                                df.to_sql(table,if_exists="append",index=False,con=cmf_db)
                            else:
                                cmf_db.execute(f"update into info values('{code}','{max_cmf_date}')")
                                cmf_db.commit()
                                df.to_sql(table,if_exists="append",index=False,con=cmf_db)
                    else:
                        print(f"no data on {code}-{mc}")
                except Exception as e:
                    error_code_info.append(f"{code}-{mc},{e}")
                pbar.update(1)
        return error_code_info
    def refresh_se(self):
        pass
    def register_router(self):
        @self.router.get("/refresh/akbk")
        async def _refresh_akbk(req:Request):
            """分析连续下跌信息"""
            try:
                #return self._get_akbk_code_info('BK0695')
                #return self._get_akbk_codes('BK1027')
                #return self._get_bk_codes('chgn_700129')
                return self.akbk_refresh()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/refresh/se")
        async def _refresh_se(req:Request):
            """更新融资融券信息"""
            try:
                return self.refresh_se()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/refresh/cmf")
        async def _refresh_cmf(req:Request):
            """更新筹码峰信息"""
            try:
                return self.cmf_refresh()
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/refresh/daily_pro")
        async def _refresh_daily_pro(req:Request):
            """更新日线增强数据"""
            error_code_info=self.daily_pro()
            return {"message":f"daily_pro已完成,error_code_info={error_code_info}"}

        @self.router.get("/update/daily_pro")
        async def _update_daily_pro(req:Request):
            """更新日线增强数据"""
            error_code_info=self.update_daily_pro()
            return {"message":f"update_daily_pro已完成,error_code_info={error_code_info}"}
        
        @self.router.get("/refresh/minute_pro")
        async def _refresh_minute_pro(req:Request):
            """更新15分钟增强数据"""
            error_code_info=self.minute_pro()
            return {"message":f"minute_pro已完成,error_code_info={error_code_info}"}
        @self.router.get("/refresh/price")
        async def _refresh_price(req:Request):
            """更新价格数据"""
            try:
                error_code_info=self.price_refresh()
                return {"message":f"price已完成,error_code_info={error_code_info}"}
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")       
