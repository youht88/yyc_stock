from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase

class AK_FLOW(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def register_router(self):
        @self.router.get("/flow/rank/gg")
        async def flow_rank_gg0(req:Request):
            """获取当前资金流数据"""
            try:
                #sector_type: choice of {"行业资金流", "概念资金流", "地域资金流"}
                periods={"0":"今日","3":"3日","5":"5日","10":"10日"}
                merged_df=pd.DataFrame()
                for period in periods:
                    period_str = periods[period]
                    df = ak.stock_individual_fund_flow_rank(indicator=period_str)
                    df = df.rename(columns={"代码":"dm","名称":"mc","最新价":"zxj",f"{period_str}涨跌幅":f"zd{period}",
                                            f"{period_str}主力净流入-净额":f"zljlr{period}",f"{period_str}主力净流入-净占比":f"zljlrl{period}",
                                            f"{period_str}超大单净流入-净额":f"cddjlr{period}",f"{period_str}超大单净流入-净占比":f"cddjlrl{period}",
                                            f"{period_str}大单净流入-净额":f"ddjlr{period}",f"{period_str}大单净流入-净占比":f"ddjlrl{period}",
                                            f"{period_str}中单净流入-净额":f"zdjlr{period}",f"{period_str}中单净流入-净占比":f"zdjlrl{period}",
                                            f"{period_str}小单净流入-净额":f"xdjlr{period}",f"{period_str}小单净流入-净占比":f"xdjlrl{period}",
                                            })
                    df = df[df[f"zljlrl{period}"]!='-']
                    df = df[df[f"zljlrl{period}"]>0]
                    df.pop("序号")
                    if merged_df.empty:
                        merged_df = df.copy()
                    else:
                        df.pop("mc")
                        df.pop("zxj")
                        merged_df = pd.merge(merged_df,df,on="dm")
                merged_df["jlr0"]=merged_df.eval("cddjlr0+ddjlr0+zdjlr0+xdjlr0")                        
                merged_df["jlr3"]=merged_df.eval("cddjlr3+ddjlr3+zdjlr3+xdjlr3")                        
                merged_df["jlr5"]=merged_df.eval("cddjlr5+ddjlr5+zdjlr5+xdjlr5")                        
                merged_df["jlr10"]=merged_df.eval("cddjlr10+ddjlr10+zdjlr10+xdjlr10")                        
                # merged_df["__order"]=merged_df.eval("zljlrl3*zljlrl5*zljlrl10")
                # df = merged_df.sort_values(by="__order",ascending=False).reset_index()
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/flow/rank/{type}/{period}")
        async def flow_rank_gg(type:str,period:str,req:Request):
            """获取当前资金流数据"""
            try:
                #sector_type: choice of {"行业资金流", "概念资金流", "地域资金流"}
                types={"gg":"individual","hy":"行业资金流","gn":"概念资金流","dy":"地域资金流"}
                periods={"0":"今日","3":"3日","5":"5日","10":"10日"}
                func=None
                period_str=periods.get(period)
                if not period_str and type=='gg':
                    raise Exception("period must be 0,3,5,10")
                if period_str=='3日' and type!='gg':
                    raise Exception("period must be 0,5,10")
                
                type_str = types.get(type)
                print(type_str,period_str)
                if not type_str:
                    raise Exception("type must be 'gg' or 'hy' or 'gn' or 'dy'")
                if type=="gg":
                    df = ak.stock_individual_fund_flow_rank(indicator=period_str)
                else:
                    df = ak.stock_sector_fund_flow_rank(indicator=period_str,sector_type=type_str)
                df = df.rename(columns={"代码":"dm","名称":"mc","最新价":"zxj",f"{period_str}涨跌幅":f"zd{period}",
                                        f"{period_str}主力净流入-净额":f"zljlr{period}",f"{period_str}主力净流入-净占比":f"zljlrl{period}",
                                        f"{period_str}超大单净流入-净额":f"cddjlr{period}",f"{period_str}超大单净流入-净占比":f"cddjlrl{period}",
                                        f"{period_str}大单净流入-净额":f"ddjlr{period}",f"{period_str}大单净流入-净占比":f"ddjlrl{period}",
                                        f"{period_str}中单净流入-净额":f"zdjlr{period}",f"{period_str}中单净流入-净占比":f"zdjlrl{period}",
                                        f"{period_str}小单净流入-净额":f"xdjlr{period}",f"{period_str}小单净流入-净占比":f"xdjlrl{period}",
                                        })
                df = df[df[f"zd{period}"]!='-']
                df.pop("序号")
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
       