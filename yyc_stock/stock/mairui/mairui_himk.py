from datetime import datetime, timedelta
import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from .mairui_base import MairuiBase

class HIMK(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def get_himk_jdzgzd(self,sync_es:bool=False):   
        """市场中所有股票与指数的阶段性（近5日、10日、20日、60日）的最高最低价以及涨跌幅。"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # g5	number	近5日最高价
        # d5	number	近5日最低价
        # zd5	number	近5日涨跌幅（%）
        # iscq5	number	近5日是否除权除息（0：否，1：是）
        # g10	number	近10日最高价
        # d10	number	近10日最低价
        # zd10	number	近10日涨跌幅（%）
        # iscq10	number	近10日是否除权除息（0：否，1：是）
        # g20	number	近20日最高价
        # d20	number	近20日最低价
        # zd20	number	近20日涨跌幅（%）
        # iscq20	number	近20日是否除权除息（0：否，1：是）
        # g60	number	近60日最高价
        # d60	number	近60日最低价
        # zd60	number	近60日涨跌幅（%）
        # iscq60	number	近60日是否除权除息（0：否，1：是）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_jdzgzd"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/jdzgzd",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_himk_pzxg(self,sync_es:bool=False):   
        """沪深A股今日创最近30个交易日内价格新高的股票。"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # h	number	最高价
        # l	number	最低价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # hs	number	换手率（%）
        # zdf5	number	5日涨跌幅（%）
        # iscq5	number	近5日是否除权除息（0：否，1：是）
        # zdf10	number	10日涨跌幅（%）
        # iscq10	number	近10日是否除权除息（0：否，1：是）
        # zdf20	number	20日涨跌幅（%）
        # iscq20	number	近20日是否除权除息（0：否，1：是）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_pzxg"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/pzxg",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_himk_pzxd(self,sync_es:bool=False):   
        """沪深A股今日创最近30个交易日内价格新低的股票。"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # h	number	最高价
        # l	number	最低价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # hs	number	换手率（%）
        # zdf5	number	5日涨跌幅（%）
        # iscq5	number	近5日是否除权除息（0：否，1：是）
        # zdf10	number	10日涨跌幅（%）
        # iscq10	number	近10日是否除权除息（0：否，1：是）
        # zdf20	number	20日涨跌幅（%）
        # iscq20	number	近20日是否除权除息（0：否，1：是）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_pzxd"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/pzxd",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def get_himk_cjzz(self,sync_es:bool=False):   
        """沪深A股中今日成交量较上一交易日成交量大幅增加的股票。"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # pv	number	前一交易日成交量（股）
        # zjl	number	增减量（股）
        # zjf	number	增减幅（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_cjzz"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/cjzz",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_cjzj(self,sync_es:bool=False):   
        """沪深A股中今日成交量较上一交易日成交量大幅减少的股票。"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # pv	number	前一交易日成交量（股）
        # zjl	number	增减量（股）
        # zjf	number	增减幅（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_cjzj"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/cjzj",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_lxfl(self,sync_es:bool=False):   
        """沪深A股中成交量连续放大的股票"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # pv	number	前一交易日成交量（股）
        # flday	number	放量天数
        # pzdf	number	阶段涨跌幅（%）
        # ispcq	number	阶段是否除权除息（0：否，1：是）
        # phs	number	阶段换手率（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_lxfl"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/lxfl",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_lxsl(self,sync_es:bool=False):   
        """沪深A股中成交量连续缩小的股票"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # pv	number	前一交易日成交量（股）
        # flday	number	放量天数
        # pzdf	number	阶段涨跌幅（%）
        # ispcq	number	阶段是否除权除息（0：否，1：是）
        # phs	number	阶段换手率（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_lxsl"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/lxsl",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_lxsz(self,sync_es:bool=False):   
        """沪深A股中连续上涨的股票"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # hs	number	换手（%）
        # szday	number	上涨天数
        # pzdf	number	阶段涨跌幅（%）
        # ispcq	number	阶段是否除权除息（0：否，1：是）
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_lxsz"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/lxsz",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_lxxd(self,sync_es:bool=False):   
        """沪深A股中连续下跌的股票"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # iscq	number	当天是否除权除息（0：否，1：是）
        # v	number	成交量（股）
        # hs	number	换手（%）
        # szday	number	上涨天数
        # pzdf	number	阶段涨跌幅（%）
        # ispcq	number	阶段是否除权除息（0：否，1：是）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_lxxd"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/lxxd",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_himk_ltszph(self,sync_es:bool=False):   
        """获取沪深A股流通市值排名"""
        #数据更新：每天20:00（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dm	string	代码
        # mc	string	名称
        # c	number	收盘价
        # zdf	number	涨跌幅（%）
        # v	number	成交量（股）
        # hs	number	换手率（%）
        # ltsz	number	流通市值（万元）
        # zsz	number	总市值（万元）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>20:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']        
        keys=['dm','ud']
        name = f"himk_ltszph"
        sql = f"select * from himk_ltszph where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"himk/ltszph",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def register_router(self):
        @self.router.get("/himk/jdzgzd",response_class=HTMLResponse)
        async def get_himk_jdzgzd(req:Request):
            """市场中所有股票与指数的阶段性（近5日、10日、20日、60日）的最高最低价以及涨跌幅"""
            try:
                df = self.get_himk_jdzgzd()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/himk/pzxg",response_class=HTMLResponse)
        async def get_himk_pzxg(req:Request):
            """沪深A股今日创最近30个交易日内价格新高的股票。"""
            try:
                df = self.get_himk_pzxg()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/himk/pzxd",response_class=HTMLResponse)
        async def get_himk_pzxd(req:Request):
            """沪深A股今日创最近30个交易日内价格新低的股票。"""
            try:
                df = self.get_himk_cjzz()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/himk/cjzz",response_class=HTMLResponse)
        async def get_himk_cjzz(req:Request):
            """沪深A股中今日成交量较上一交易日成交量大幅增加的股票。"""
            try:
                df = self.get_himk_pzxd()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/himk/cjzj",response_class=HTMLResponse)
        async def get_himk_cjzj(req:Request):
            """沪深A股中今日成交量较上一交易日成交量大幅减少的股票。"""
            try:
                df = self.get_himk_cjzj()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/himk/lxfl",response_class=HTMLResponse)
        async def get_himk_lxfl(req:Request):
            """沪深A股中成交量连续放大的股票"""
            try:
                df = self.get_himk_lxfl()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/himk/lxsl",response_class=HTMLResponse)
        async def get_himk_lxsl(req:Request):
            """沪深A股中成交量连续缩小的股票"""
            try:
                df = self.get_himk_lxsl()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/himk/lxsz",response_class=HTMLResponse)
        async def get_himk_lxsz(req:Request):
            """沪深A股中连续上涨的股票"""
            try:
                df = self.get_himk_lxsz()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/himk/lxxd",response_class=HTMLResponse)
        async def get_himk_lxxd(req:Request):
            """沪深A股中连续下跌的股票"""
            try:
                df = self.get_himk_lxxd()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
            
        @self.router.get("/himk/ltszph",response_class=HTMLResponse)
        async def get_himk_ltszph(req:Request):
            """获取沪深A股流通市值排名"""
            try:
                df = self.get_himk_ltszph()
                magic = req.query_params.get('magic')
                if magic!=None:
                    df = df[df['c'].apply(self.is_magic)] 
                    df = df.reset_index(drop=True)   
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")