from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests
from .mairui_base import MairuiBase

class HIJG(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def get_hijg_jgcghz(self,year:int,quarter:int,sync_es=False):
        """机构持股汇总，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据"""
        # 数据更新：每周六上午9点（约5个小时完成更新）
        # 请求频率：1分钟20次
        
        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # oc	number	机构数
        # occ	number	机构变化数
        # gbl	number	持股比例(%)
        # gblz	number	持股比例增幅(%)
        # lbl	number	占流通股比例(%)
        # ltz	number	占流通股比例增幅(%)
        # dt	array(OrgDetail)	机构明细，OrgDetail对象属性详见下方
        # y	number	报告年份，如2021
        # q	number	报告季度，1:一季报，2：中报，3：三季报，4：年报
        # yq	string	报告年份季度含义，如“2021年一季报”
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['ud']
        keys=[] #['dm','y','q','ud']
        wind='dt'
        name = f"hijg_jgcghz"
        #sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and y={year} and q={quarter}"
        sql = f"delete from {name} where y={year} and q={quarter}"
        df = self.load_data(name,f"hijg/jgcghz/{year}/{quarter}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            wind=wind,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','y','q','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hijg_jj(self,year:int,quarter:int,sync_es=False):
        """基金重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
        # 数据更新：每周六上午9点（约5个小时完成更新）
        # 请求频率：1分钟20次
        
        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # ed	string	截止日期
        # js	number	机构数
        # bq	number	本期持股数(万股)
        # ltb	number	持股占已流通A股比例(%)
        # szj	number	同上期增减(万股)
        # cbl	number	持股比例(%)
        # sjs	number	上期机构数
        # dt	array(OrgJjzcDetail)	机构明细，OrgJjzcDetail对象属性详见下方
        # y	number	报告年份，如2021
        # q	number	报告季度，1:一季报，2：中报，3：三季报，4：年报
        # yq	string	报告年份季度含义，如“2021年一季报”
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['ud']
        keys=[] #['dm','y','q','ud']
        wind='dt'
        name = f"hijg_jj"
        #sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and y={year} and q={quarter}"
        sql = f"delete from {name} where y={year} and q={quarter}"
        df = self.load_data(name,f"hijg/jj/{year}/{quarter}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            wind=wind,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','y','q','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hijg_sb(self,year:int,quarter:int,sync_es=False):
        """社保重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
        # 数据更新：每周六上午9点（约5个小时完成更新）
        # 请求频率：1分钟20次
        
        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # ed	string	截止日期
        # js	number	机构数
        # bq	number	本期持股数(万股)
        # ltb	number	持股占已流通A股比例(%)
        # szj	number	同上期增减(万股)
        # cbl	number	持股比例(%)
        # sjs	number	上期机构数
        # dt	array(OrgJjzcDetail)	机构明细，OrgJjzcDetail对象属性详见下方
        # y	number	报告年份，如2021
        # q	number	报告季度，1:一季报，2：中报，3：三季报，4：年报
        # yq	string	报告年份季度含义，如“2021年一季报”
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['ud']
        keys=[] #['dm','y','q','ud']
        wind='dt'
        name = f"hijg_sb"
        #sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and y={year} and q={quarter}"
        sql = f"delete from {name} where y={year} and q={quarter}"
        df = self.load_data(name,f"hijg/sb/{year}/{quarter}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            wind=wind,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','y','q','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hijg_qf(self,year:int,quarter:int,sync_es=False):
        """QFII重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
        # 数据更新：每周六上午9点（约5个小时完成更新）
        # 请求频率：1分钟20次
        
        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # ed	string	截止日期
        # js	number	机构数
        # bq	number	本期持股数(万股)
        # ltb	number	持股占已流通A股比例(%)
        # szj	number	同上期增减(万股)
        # cbl	number	持股比例(%)
        # sjs	number	上期机构数
        # dt	array(OrgJjzcDetail)	机构明细，OrgJjzcDetail对象属性详见下方
        # y	number	报告年份，如2021
        # q	number	报告季度，1:一季报，2：中报，3：三季报，4：年报
        # yq	string	报告年份季度含义，如“2021年一季报”
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['ud']
        keys=[] #['dm','y','q','ud']
        wind='dt'
        name = f"hijg_qf"
        #sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and y={year} and q={quarter}"
        sql = f"delete from {name} where y={year} and q={quarter}"
        df = self.load_data(name,f"hijg/qf/{year}/{quarter}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            wind=wind,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','y','q','ud'])
            print(f"errors:{es_result["errors"]}")
        return df


    def register_router(self):
        @self.router.get("/hijg/jgcghz/{year}/{quarter}")
        async def get_hijg_jgcghz(year:int,quarter:int,req:Request):
            """机构持股汇总，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据"""
            try:
                df = self.get_hijg_jgcghz(year,quarter)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hijg/jj/{year}/{quarter}")
        async def get_hijg_jj(year:int,quarter:int,req:Request):
            """基金重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
            try:
                df = self.get_hijg_jj(year,quarter)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hijg/sb/{year}/{quarter}")
        async def get_hijg_sb(year:int,quarter:int,req:Request):
            """社保重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
            try:
                df = self.get_hijg_sb(year,quarter)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hijg/qf/{year}/{quarter}")
        async def get_hijg_qf(year:int,quarter:int,req:Request):
            """QFII重仓，支持“年份_季度”查询，年份可选（1989~当前年份），季度可选（1:一季报，2：中报，3：三季报，4：年报），如“2021_1”，表示查询2021年一季度数据。"""
            try:
                df = self.get_hijg_qf(year,quarter)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
