from datetime import datetime, timedelta
import pandas as pd
import requests
from .mairui_base import MairuiBase

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

class HIGG(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def get_higg_jlr(self,sync_es=False):
        """获取所有股票的资金净流入"""
        #数据更新：每天15:30（约10分钟更新完成）
        #请求频率：1分钟20次

        # 字段名称	数据类型	字段说明
        # t	string	服务器更新时间yyyy-MM-dd HH:mm:ss
        # mc	string	名称
        # dm	string	代码
        # zxj	number	最新价（元）
        # zdf	number	涨跌幅（%）
        # hsl	number	换手率（%）
        # cje	number	成交额（元）
        # lczj	number	流出资金（元）
        # lrzj	number	流入资金（元）
        # jlr	string	净流入（元）
        # jlrl	string	净流入率（%）
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"higg_jlr"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"higg/jlr",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def get_higg_zljlr(self,sync_es=False):
        """获取所有股票的主力资金净流入"""
        #数据更新：每天15:30（约10分钟更新完成）
        #请求频率：1分钟20次

        # 字段名称	数据类型	字段说明
        # t	string	服务器更新时间yyyy-MM-dd HH:mm:ss
        # mc	string	名称
        # dm	string	代码
        # zxj	number	最新价（元）
        # zdf	number	涨跌幅（%）
        # hsl	number	换手率（%）
        # cje	number	成交额（元）
        # zllczj	number	主力流出资金（元）
        # zllrzj	number	主力流入资金（元）
        # zljlr	string	主力净流入（元）
        # zljlrl	string	主力净流入率（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"higg_zljlr"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"higg/zljlr",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_higg_shjlr(self,sync_es=False):
        """获取所有股票的散户资金净流入"""
        #数据更新：每天15:30（约10分钟更新完成）
        #请求频率：1分钟20次

        # 字段名称	数据类型	字段说明
        # t	string	服务器更新时间yyyy-MM-dd HH:mm:ss
        # mc	string	名称
        # dm	string	代码
        # zxj	number	最新价（元）
        # zdf	number	涨跌幅（%）
        # hsl	number	换手率（%）
        # cje	number	成交额（元）
        # shlczj	number	散户流出资金（元）
        # shlrzj	number	散户流入资金（元）
        # shjlr	string	散户净流入（元）
        # shjlrl	string	散户净流入率（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"higg_shjlr"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"higg/shzlr",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def register_router(self):
        @self.router.get("/higg/jlr",response_class=HTMLResponse)
        async def get_higg_jlr(req:Request):
            """获取所有股票的资金净流入"""
            try:
                df = self.get_higg_jlr()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/higg/zljlr",response_class=HTMLResponse)
        async def get_higg_zljlr(req:Request):
            """获取所有股票的主力资金净流入"""
            try:
                df = self.get_higg_zljlr()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/higg/shjlr",response_class=HTMLResponse)
        async def get_higg_shjlr(req:Request):
            """获取所有股票的散户资金净流入"""
            try:
                df = self.get_higg_shjlr()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
