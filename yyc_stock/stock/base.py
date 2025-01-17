import sqlite3
import time
from fastapi.responses import HTMLResponse
import pandas as pd
import requests
from datetime import datetime,timedelta
import logging
import re
import requests
from rich import print
from typing import Literal
from ylz_utils.config import Config 
from ylz_utils.database.elasticsearch import ESLib

import concurrent.futures
from urllib.parse import unquote

from fastapi import FastAPI, HTTPException,Request,Response,APIRouter
import akshare as ak

def gen_content_html(data_table,state_table,fix_col_index:list[int]=[],url=None,desc=None):
    if fix_col_index:
        fix_columns = ','.join([f'th:nth-child({idx}), td:nth-child({idx})' for idx in fix_col_index])
        fix_columns_css = fix_columns + ''' {
                position: sticky;
                left: 0;
                background-color: #f9f9f9; /* 固定列的背景颜色 */
                z-index: 2; /* 确保固定列在其他内容上方 */
            }'''
    else:
        fix_columns_css = ''
    if url:
        url = unquote(str(url))
    #th.代码, td.代码, th.名称,td.名称     
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>DataFrame Table</title>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
                white-space: nowrap;
            }}
            {fix_columns_css} 

            th {{
                position: sticky; /* 使表头固定 */
                top: 0; /* 固定在顶部 */
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
                cursor: pointer;
                background-color: #f2f2f2;
                z-index: 40;
            }}
            th:hover {{
                background-color: #ddd;
            }}
            
            tr:nth-child(even) {{
                background-color: #f2f2f2; /* 偶数行背景色 */
            }}
            tr:nth-child(odd) {{
                background-color: #ffffff; /* 奇数行背景色 */
            }}
            .copy-button {{
                margin: 5px auto;
                display: block;
                cursor: pointer;
                padding: 5px 10px;
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 5px;
                text-align: center;
            }}
            .export-button {{
                margin: 5px auto;
                cursor: pointer;
                padding: 5px 10px;
                background-color: #1E90FF;
                color: white;
                border: none;
                border-radius: 5px;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div>
        <h2>说明:</h2> <p>{desc}</p>
        </div>
        <div>
        <h2>URL: </h2> <p>{url}</p>
        </div>
        <h2>统计 <button id="export_state_btn" class="export-button">导出为 CSV</button> </h2>
        {state_table}
        <h2>数据 <button id="export_data_btn" class="export-button">导出为 CSV</button> </h2>
        {data_table}

        <script>
            function export_data(table_id) {{
                // 获取表格数据
                let table = document.getElementById(table_id);
                let csv = [];
                let row_count = 0;
                for (let row of table.rows) {{
                    let rowData = [];
                    row_count ++;
                    for (let cell of row.cells) {{
                        let text = cell.innerText;
                        let value = ''
                        if (row_count==1){{
                            value=""
                            text = text.replace('Copy','').replace('\\n','')                     
                        }}else{{
                            value = parseFloat(text.replace(/,/g, '').replace(/E[+-]?\\d+/, 'e$&'));
                        }}
                        if (value){{
                            rowData.push(value);
                        }}else{{
                            rowData.push(text);
                        }}
                    }}
                    csv.push(rowData.join(','));
                }}
                let csvContent = 'data:text/csv;charset=utf-8,' + csv.join('\\n');
                let encodedUri = encodeURI(csvContent);
                let link = document.createElement('a');
                link.setAttribute('href', encodedUri);
                link.setAttribute('download', 'data.csv');
                document.body.appendChild(link); // Required for FF
                link.click();
                document.body.removeChild(link); // Remove the link after downloading
            }}
            document.getElementById('export_state_btn').addEventListener('click', ()=>{{export_data('state_table')}});
            document.getElementById('export_data_btn').addEventListener('click', ()=>{{export_data('data_table')}});
            document.querySelectorAll('#data_table thead th').forEach((th,index) => {{
                if (index>=0){{
                    th.addEventListener('click', () => {{
                        const table = th.parentElement.parentElement.parentElement;
                        const tbody = table.querySelector('tbody');
                        const rows = Array.from(tbody.querySelectorAll('tr'));
                        const index = Array.from(th.parentNode.children).indexOf(th);
                        const isAscending = th.classList.toggle('asc');

                        rows.sort((a, b) => {{
                            const aText = a.children[index].innerText;
                            const bText = b.children[index].innerText;
                            // 处理格式化的数字
                            const aValue = parseFloat(aText.replace(/,/g, '').replace(/E[+-]?\\d+/, 'e$&'));
                            const bValue = parseFloat(bText.replace(/,/g, '').replace(/E[+-]?\\d+/, 'e$&'));
                            if (aValue && bValue) {{ 
                                // 如果是数字，则进行比较，否则按字符串比较
                                return isAscending ? aValue - bValue : bValue - aValue;
                            }}else{{
                                return isAscending ? aText.localeCompare(bText) : bText.localeCompare(aText);
                            }}}});

                        while (tbody.firstChild) {{
                            tbody.removeChild(tbody.firstChild);
                        }}
                        tbody.append(...rows);
                    }});
                    // 在表头添加拷贝按钮
                    const copyButton = document.createElement('button');
                    copyButton.innerText = 'Copy';
                    copyButton.className = 'copy-button';
                    copyButton.onclick = (e) => {{
                        e.stopPropagation();  // 防止触发排序
                        copyColumn(index);
                    }};
                    const buttonContainer = document.createElement('div');
                    buttonContainer.className = 'button-container';
                    buttonContainer.appendChild(copyButton);
                    th.appendChild(buttonContainer);
                }}
            }});
            // 复制功能
            function copyColumn(index) {{
                const table = document.querySelector('#data_table');
                const tbody = table.querySelector('tbody');
                const rows = Array.from(tbody.querySelectorAll('tr'));
                const columnData = rows.map(row => row.children[index]?.innerText).join(',');

                navigator.clipboard.writeText(columnData).then(() => {{
                    console.log('Column copied to clipboard!');
                }}).catch(err => {{
                    console.error('Failed to copy: ', err);
                }});
            }}
        </script>
    </body>
    </html>
    """
    return html_content

class StockBase:
    stock:list = []
    def __init__(self):
        self.esLib = ESLib(using='es')
        # 获取当前模块的目录
        self.gpdm = None
        self.bkdm = None
        self.bk_codes = {}
        self.akbkdm=None
        self.akbk_codes = {}
        self.mairui_token = Config.get('STOCK.MAIRUI.TOKEN')
        self.mairui_api_url = "http://api.mairui.club" 
        self.router = APIRouter()
        self.stock_db_name = Config().get('STOCK.DB_NAME','stock.db')
        self.sqlite = sqlite3.connect(self.stock_db_name)
    # 定义一个执行函数的方法k
    def parallel_execute(self,**kwargs):
        func = kwargs.pop("func")
        codes = kwargs.get("codes")
        sync_sqlite = kwargs.get("sync_sqlite")
        if func:
            logging.info(f"run {func.__name__} as {datetime.now()}")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                if sync_sqlite:
                    kwargs.pop("sync_sqlite")
                if codes:
                    kwargs.pop("codes")
                    futures = [executor.submit(func,code,**kwargs) for code in codes]
                else:
                    futures = [executor.submit(func,**kwargs)]
                results = [future.result() for future in futures]
                if isinstance(results[0],list):
                    #如果results是的每一项是数组，则需要展开
                    results = [item for items in results for item in items]
                print("results count:",len(results))
                if sync_sqlite:
                    db_name = sync_sqlite.get('db_name')
                    table_name = sync_sqlite.get('table_name')
                    columns = sync_sqlite.get('columns')
                    if db_name and table_name:
                        df = pd.DataFrame(results)
                        if columns:
                            df = df.filter(columns)
                        df.to_sql(table_name,index=False,if_exists="append",con=sqlite3.connect(db_name))
                return results
    def _get_codes(self,type:Literal['stock','zx','bk','akbk'],name)->list[str]:
        codes = []
        if type=='stock':
            codes_info = [self._get_stock_code(item) for item in name.split(',') if item]
            codes = [item['code'] for item in codes_info]
        elif type=='zx':
            codes_info = self._get_zx_codes(name)
            codes = [item['code'] for item in codes_info]
        elif type=='bk':
            codes_info = self._get_bk_codes(name)
            codes = [item['dm'] for item in codes_info]
        elif type=='akbk':
            codes_info = self._get_akbk_codes(name)
            codes = [item['dm'] for item in codes_info]
        return codes        
    def _get_request_codes(self,req:Request)->list[str]:
        stock = req.query_params.get('code')
        zx = req.query_params.get('zx')
        bk = req.query_params.get('bk')
        akbk = req.query_params.get('akbk')
        codes = []
        if not stock and not zx and not bk and not akbk:
            raise Exception('必须指定code、zx、bk、akbk参数中的一个')
        if stock:
            codes = self._get_codes('stock',stock)
        elif zx:
            codes = self._get_codes('zx',zx)
        elif bk:
            codes = self._get_codes('bk',bk)
        elif akbk:
            codes = self._get_codes('akbk',akbk)
        return codes
    def _get_zx_codes(self,key:str)->list[dict]:
        try:
            df = pd.read_sql(f"select * from zx where key='{key}'",self.sqlite)
            if df.empty:
                raise Exception(f"自选{key}不存在")
            return df.to_dict(orient="records")
        except Exception as e:
            raise Exception(f"没有找到自选{key},{e}")
    def _get_bk_codes(self,bk:str,intersect=True,jys=['sz','sh'])->list[dict]:    
        codes = [item for item in bk.split(',') if item]
        try:
            codes = [self._get_bk_code(code)['code'] for code in codes]
        except Exception as e:
            raise Exception(f"转换板块code时出错,{e}")        
        df_bk=[]
        con=sqlite3.connect(self.stock_db_name)
        for code in codes:
            if not self.bk_codes.get(code):
                try:
                    df = pd.read_sql(f"select * from bk_codes where bk='{code}'",con=con)
                    if df.empty:
                        raise Exception("need retrieve from network")
                    self.bk_codes[code] = df.to_dict(orient="records")
                    data = [ item for item in self.bk_codes[code] if item['jys'] in jys]
                    df_bk.append(pd.DataFrame(data))    
                except Exception as e:
                    res = requests.get( 
                        f"{self.mairui_api_url}/hszg/gg/{code}/{self.mairui_token}",
                    )
                    data = res.json()
                    df = pd.DataFrame(data)
                    df['bk'] = code
                    try:
                        df.to_sql("bk_codes",index=False,if_exists="append",con=con)
                        con.execute("create unique index bk_codes_index on bk_codes(bk,dm)")
                    except:
                        pass
                    self.bk_codes[code]=data
                    data = [ item for item in data if item['jys'] in jys]
                    df_bk.append(pd.DataFrame(data))
            else:
                df_bk.append(pd.DataFrame(self.bk_codes[code]))
        if intersect:
            df = df_bk[0]
            if len(df_bk)>1:
                for item in df_bk[1:]:
                    df = df.merge(item)
        else:
            df = pd.concat(df_bk).drop_duplicates()
        return df.to_dict(orient='records')

    def _get_akbk_codes(self,bk:str,intersect=True,jys=['sz','sh'])->list[dict]:    
        codes = [item for item in bk.split(',') if item]
        try:
            codes_info = [self._get_akbk_code_info(code) for code in codes]
        except Exception as e:
            raise Exception(f"转换板块code时出错,{e}")        
        df_bk=[]
        con=sqlite3.connect(self.stock_db_name)
        for code_info in codes_info:
            code = code_info['code']
            name = code_info['name']
            type = code_info['type']
            if not self.akbk_codes.get(code):
                try:
                    df = pd.read_sql(f"select * from akbk_codes where bk='{code}'",con=con)
                    if df.empty:
                        raise Exception("need retrieve from network")
                    self.akbk_codes[code] = df.to_dict(orient="records")
                    data = [ item for item in self.akbk_codes[code] if item['jys'] in jys]
                    df_bk.append(pd.DataFrame(data)) 
                except Exception as e:
                    if type=='hy':
                        df = ak.stock_board_industry_cons_em(name)
                    else:
                        df = ak.stock_board_concept_cons_em(name)                    
                    df = df[['代码','名称']].rename(columns={'代码':'dm','名称':'mc'})
                    df['jys']= df['dm'].apply(lambda x:'sh' if x.startswith('6') else 
                                                       'sz' if x.startswith('0') or x.startswith('3') else 
                                                       'bj' if x.startswith('8') else '' )
                    df['type'] = type
                    df['bkmc'] = name
                    data = df.to_dict(orient='records')
                    df['bk'] = code
                    try:
                        df.to_sql("akbk_codes",index=False,if_exists="append",con=con)
                        con.execute("create unique index IF NOT EXISTS akbk_codes_index on akbk_codes(bk,dm)")
                    except:
                        pass
                    self.bk_codes[code]=data
                    data = [ item for item in data if item['jys'] in jys]
                    df_bk.append(pd.DataFrame(data))
            else:
                df_bk.append(pd.DataFrame(self.akbk_codes[code]))
        if intersect:
            df = df_bk[0]
            if len(df_bk)>1:
                for item in df_bk[1:]:
                    df = df.merge(item)
        else:
            df = pd.concat(df_bk).drop_duplicates()
        return df.to_dict(orient='records')

    def _get_akbk_code_info(self,bk_name:str,force=False)->dict:
        if not self.akbkdm:
            try:
                con=sqlite3.connect(self.stock_db_name)
                try:
                    if force:
                        raise Exception("force to retrieve from network!")
                    df = pd.read_sql("select * from akbkdm",con=con)
                    self.akbkdm = df.to_dict(orient="records")
                except Exception as e:
                    df_res1 = ak.stock_board_industry_name_em()
                    df_res2 = ak.stock_board_concept_name_em()
                    df_hybk = df_res1[['板块名称','板块代码']].rename(columns={'板块名称':'name','板块代码':'code'})
                    df_hybk['type']='hy'
                    df_gnbk = df_res2[['板块名称','板块代码']].rename(columns={'板块名称':'name','板块代码':'code'})
                    df_gnbk['type']='gn'
                    df = pd.concat([df_hybk,df_gnbk]).reset_index()                   
                    self.akbkdm = df.to_dict(orient='records')
                    df.to_sql("akbkdm",index=False,if_exists="replace",con=con)
            except Exception as e:
                raise Exception(f"获取{bk_name}代码错误,{e}") 
        code_info = list(filter(lambda item:item['code']==bk_name,self.akbkdm))
        if code_info:
            if len(code_info)>1:
                code_str = '|'.join([f"name:{info['name']},code:{info['code']}" for info in code_info])
                raise Exception(f'板块代码[{bk_name}]不唯一,[{code_str}],请重新配置!')
            return code_info[0]
        else:
            code_info = list(filter(lambda item:item['name'].find(bk_name)>=0,self.akbkdm)) 
            if code_info:
                if len(code_info)>1:
                    code_str = '|'.join([f"name:{info['name']},code:{info['code']}" for info in code_info])
                    raise Exception(f'板块代码[{bk_name}]不唯一,[{code_str}],请重新配置!')
                else:
                    return code_info[0]
            else:
                raise Exception(f'没有找到[{bk_name}]相关板块!')

    def _get_bk_code(self,bk_name:str,force=False)->dict:
        if not self.bkdm:
            try:
                con=sqlite3.connect(self.stock_db_name)
                try:
                    if force:
                        raise Exception("force to retrieve from network!")
                    df = pd.read_sql("select * from bkdm",con=con)
                    self.bkdm = df.to_dict(orient="records")
                except Exception as e:
                    res = requests.get(f"{self.mairui_api_url}/hszg/list/{self.mairui_token}")
                    self.bkdm = res.json()
                    df = pd.DataFrame(self.bkdm)
                    df.to_sql("bkdm",index=False,if_exists="replace",con=con)
            except Exception as e:
                raise Exception(f"获取{bk_name}代码错误,{e}") 
        code_info = list(filter(lambda item:item['code']==bk_name,self.bkdm))
        if code_info:
            if len(code_info)>1:
                code_str = '|'.join([f"name:{info['name']},code:{info['code']}" for info in code_info])
                raise Exception(f'板块代码[{bk_name}]不唯一,[{code_str}],请重新配置!')
            return code_info[0]
        else:
            code_info = list(filter(lambda item:item['name'].find(bk_name)>=0,self.bkdm)) 
            if code_info:
                if len(code_info)>1:
                    code_str = '|'.join([f"name:{info['name']},code:{info['code']}" for info in code_info])
                    raise Exception(f'板块代码[{bk_name}]不唯一,[{code_str}],请重新配置!')
                else:
                    return code_info[0]
            else:
                raise Exception(f'没有找到[{bk_name}]相关板块!')

    def _get_stock_jysc(self,code:str)->str:
        '''获取股票的交易市场，主板zb、创业板cyb、科创板kcb、北交所bj'''
        if code.startswith('300'):
            return 'cyb'
        elif code.startswith('688'):
            return 'kcb'
        elif code.startswith('60') or code.startswith('00'):
            return 'zb'
        elif code.startswith('8') or code.startswith('9'):
            return 'bj'
        else:
            return 'na'
    def _get_stock_code(self,stock_name:str,force=False)->dict:
        """根据股票或指数名称获取股票/指数代码"""
        if not self.gpdm:
            try:
                con=sqlite3.connect(self.stock_db_name)
                try:
                    if force:
                        raise Exception("force to retrieve from network!")
                    df = pd.read_sql("select * from gpdm",con=con)
                    self.gpdm = df.to_dict(orient="records")
                except Exception as e:
                    res = requests.get(f"{self.mairui_api_url}/hslt/list/{self.mairui_token}")
                    stock_dm = res.json()
                    #合并沪深两市指数代码
                    res = requests.get(f"{self.mairui_api_url}/zs/sh/{self.mairui_token}")
                    sh_dm = [{**item , "dm":item['dm'].replace('sh','')} for item in res.json()]
                    res = requests.get(f"{self.mairui_api_url}/zs/sz/{self.mairui_token}")
                    sz_dm = [{**item , "dm":item['dm'].replace('sz','')} for item in res.json()]
                    all_dm = stock_dm + sh_dm + sz_dm
                    self.gpdm = [
                        {"ts_code":f"{item['dm']}.{item['jys'].upper()}","symbol":f"{item['dm']}","name":item['mc']} for item in all_dm
                    ]
                    df = pd.DataFrame(self.gpdm)
                    df.to_sql("gpdm",index=False,if_exists="replace",con=con)
            except Exception as e:
                raise Exception(f"获取{stock_name}代码错误,{e}")
        if stock_name.startswith('sh') or stock_name.startswith('sz') or stock_name.startswith('bj'):
            # mairui code
            jys = stock_name[:2]
            code = stock_name[2:]
            ts_code = f"{code}.{jys.upper()}"
            mr_code = stock_name
            ball_code = f"{jys.upper()}{code}"
            stock_info = list(filter(lambda item:item["symbol"]==code,self.gpdm))
            if stock_info:
                name = stock_info[0]['name']
                return {"code":code,"mr_code":mr_code,"ts_code":ts_code,"name":name,"jys":jys,"ball_code":ball_code,"jysc":self._get_stock_jysc(code)}
            else:
                raise Exception(f"没有找到{stock_name}代码信息")
        elif stock_name.endswith('.SH') or stock_name.endswith('.SZ') or stock_name.endswith('.BJ'):
            #tu-share code
            jys = stock_name[-2:].lower()
            code = stock_name[:-3]
            ts_code = stock_name
            mr_code = f"{jys}{code}"
            ball_code = f"{jys.upper()}{code}"
            stock_info = list(filter(lambda item:item["symbol"]==code,self.gpdm))
            if stock_info:
                name = stock_info[0]['name']
                return {"code":code,"mr_code":mr_code,"ts_code":ts_code,"name":name,"jys":jys,"ball_code":ball_code,"jysc":self._get_stock_jysc(code)}
            else:
                raise Exception(f"没有找到{stock_name}代码信息")
        elif stock_name.startswith('SH') or stock_name.startswith('SZ') or stock_name.startswith('BJ'):
            #雪球code
            jys = stock_name[:2].lower()
            code = stock_name[2:]
            ball_code = stock_name
            ts_code = f"{code}.{jys.upper()}"
            mr_code = f"{jys}{code}"
            stock_info = list(filter(lambda item:item["symbol"]==code,self.gpdm))
            if stock_info:
                name = stock_info[0]['name']
                return {"code":code,"mr_code":mr_code,"ts_code":ts_code,"name":name,"jys":jys,"ball_code":ball_code,"jysc":self._get_stock_jysc(code)}
            else:
                raise Exception(f"没有找到{stock_name}代码信息")
        elif stock_name.isnumeric():
            code = stock_name
            stock_info = list(filter(lambda item:item["symbol"]==code,self.gpdm))
            if stock_info:
                ts_code = stock_info[0]['ts_code']
                jys = ts_code[-2:].lower()
                mr_code = f"{jys}{code}"
                name = stock_info[0]['name']
                ball_code = f"{jys.upper()}{code}"
                return {"code":code,"mr_code":mr_code,"ts_code":ts_code,"name":name,"jys":jys,"ball_code":ball_code,"jysc":self._get_stock_jysc(code)}
            else:
                raise Exception(f"没有找到{stock_name}代码信息")
        else:
            try:
                stock_info = list(filter(lambda item:item["name"]==stock_name.upper(),self.gpdm))
                if stock_info:
                    ts_code = stock_info[0]['ts_code']
                    jys = ts_code[-2:].lower()
                    code = stock_info[0]['symbol']
                    name = stock_info[0]['name']
                    mr_code = f"{jys}{code}"
                    ball_code = f"{jys.upper()}{code}"
                    return {"code":code,"mr_code":mr_code,"ts_code":ts_code,"name":name,"jys":jys,"ball_code":ball_code,"jysc":self._get_stock_jysc(code)}
                else:
                    raise Exception(f"没有找到{stock_name}代码信息")
            except Exception as e:
                raise Exception(f"查找{stock_name}代码信息出错,{e}")

    def _prepare_df(self,df:pd.DataFrame,req:Request):
        where = [(item[0][1:],item[1]) for item in req.query_params.items() if item[0].startswith('@')]
        having = [(item[0][1:],item[1]) for item in req.query_params.items() if item[0].startswith('$')]
        order = req.query_params.get('o')
        limit = int(req.query_params.get('n',0))
        add_cols = req.query_params.get('a')
        groupby = req.query_params.get('g')
        column = req.query_params.get('c')
        column_regex = req.query_params.get('c.regex')
        if add_cols:
            print("add cols=",add_cols)
            df = self._parse_add_express(df,add_cols) 
        if where:
            print("where=",where)
            df = self._parse_condition_express(df,where)
        if groupby:
            print("groupby=",groupby)
            df = self._parse_groupby_express(df,groupby) 
        if having:
            print("having=",having)
            df = self._parse_condition_express(df,having) 
        if order:
            print("order=",order)
            df = self._parse_order_express(df,order) 
        if limit:
            print("limit=",limit)
            df = df.head(limit)
        if column:
            print("column=",column)
            df = self._parse_column_express(df,column)
        elif column_regex:
            print("column_regex=",column_regex)
            df = self._parse_column_express(df,column_regex,regex=True)

        return df
    def _parse_column_express(self,df:pd.DataFrame,column:str)->pd.DataFrame:
        columns = column.split(",")
        df_filters = [df.filter(regex=item) for item in columns]
        df = pd.concat(df_filters, axis=1)        
        df = df.loc[:, ~df.columns.duplicated()]
        return df
    def _parse_condition_express(self,df:pd.DataFrame,condition:list[tuple[str,str]])->pd.DataFrame:
        try:
            number_pattern = r'^-?\d*\.?\d+$'
            for item in condition:
                key = item[0]
                value = item[1]
                if key and value:
                    keys = key.split('.')
                    key = keys[0]
                    key_func = keys[1] if len(keys)>1 else None 
                    if pd.api.types.is_datetime64_any_dtype(df[key]):
                        values = value.replace('[','').replace(']','').split(',')[:2]
                        if key_func=='date':
                            if values[0]!='' and values[1]!='':
                                print(f"{key} date between {values[0]} and {values[1]}")
                                start = pd.to_datetime(values[0]).date()
                                end = pd.to_datetime(values[1]).date()
                                df = df[(df[key].dt.date>=start) & (df[key].dt.date<=end)]
                            elif values[0]!='' and values[1]=='':
                                print(f"{key} date >= {values[0]}")
                                start = pd.to_datetime(values[0]).date()
                                df = df[(df[key].dt.date>=start)]
                            elif values[0]=='' and values[1]!='':
                                print(f"{key} date <= {values[1]}")
                                end = pd.to_datetime(values[1]).date()
                                df = df[(df[key].dt.date<=end)]
                            else:
                                print(f"{key} date == {values[0]}")
                                start = pd.to_datetime(values[0]).date()
                                df = df[(df[key].dt.date==start)]
                        elif key_func=='time':
                            if values[0]!='' and values[1]!='':
                                print(f"{key} time between {values[0]} and {values[1]}")
                                start = pd.to_datetime(values[0]).time()
                                end = pd.to_datetime(values[1]).time()
                                df = df[(df[key].dt.time>=start) & (df[key].dt.time<=end)]
                            elif values[0]!='' and values[1]=='':
                                print(f"{key} time >= {values[0]}")
                                start = pd.to_datetime(values[0]).time()
                                df = df[(df[key].dt.time>=start)]
                            elif values[0]=='' and values[1]!='':
                                print(f"{key} time <= {values[1]}")
                                end = pd.to_datetime(values[1]).time()
                                df = df[(df[key].dt.time<=end)]
                            else:
                                print(f"{key} time == {values[0]}")
                                start = pd.to_datetime(values[0]).time()
                                df = df[(df[key].dt.time==start)]
                        else:
                            if values[0]!='' and values[1]!='':
                                print(f"{key} datetime between {values[0]} and {values[1]}")
                                start = pd.to_datetime(values[0])
                                end = pd.to_datetime(values[1])
                                df = df[(df[key]>=start) & (df[key]<=end)]                      
                            elif values[0]!='' and values[1]=='':
                                print(f"{key} datetime >= {values[0]}")
                                start = pd.to_datetime(values[0])
                                df = df[(df[key]>=start)]
                            elif values[0]=='' and values[1]!='':
                                print(f"{key} datetime <= {values[1]}")
                                end = pd.to_datetime(values[1])
                                df = df[(df[key]<=end)]
                            else:
                                print(f"{key} datetime == {values[0]}")
                                start = pd.to_datetime(values[0])
                                df = df[(df[key].dt.time==start)]
                    elif pd.api.types.is_numeric_dtype(df[key]):
                        values = value.replace('[','').replace(']','').split(',')[:2]
                        if re.match(number_pattern, values[0]) and re.match(number_pattern, values[1]):
                            print(f"{key} between {float(values[0])} and {float(values[1])}")
                            df = df[(df[key]>=float(values[0])) & (df[key]<=float(values[1]))]
                        elif re.match(number_pattern, values[0]) and values[1]=='':
                            print(f"{key} >= {float(values[0])}")
                            df = df[df[key] >= float(values[0])]
                        elif values[0]=='' and re.match(number_pattern, values[1]):
                            print(f"{key} <= {float(values[1])}")
                            df = df[df[key] <= float(values[1])]
                        elif re.match(number_pattern, values[0]):
                            print(f"{key} = {float(values[0])}")
                            df = df[df[key]==float(values[0])]
                        else:
                            print(f"{key} = {values[0]}")
                            df = df[df[key]==values[0]]
                    else:
                        values = value.split(',')
                        if key_func == 'regex':
                            filter = pd.Series([False] * len(df))
                            for value in values:
                                print(f"{key} match {value}")
                                filter = filter | df[key].str.contains(value,regex=True)
                            df = df[filter]
                        else:
                            # 包含字符串或不包含字符串
                            filter = pd.Series([False] * len(df))
                            for value in values:
                                if value.startswith("!"):
                                    print(f"{key} not contains {value[1:]}")
                                    filter = filter | ~df[key].str.contains(value[1:])
                                else:
                                    print(f"{key} contains {value}")
                                    filter = filter | df[key].str.contains(value)
                            df = df[filter]
            return df
        except Exception as e:
            raise Exception(f"condition express is error:{e}") 

    def _parse_groupby_express(self,df:pd.DataFrame,groupby:str)->pd.DataFrame:
        # g=code,c;p:sum,z:avg
        #df.groupby(['tt','p','vt'])[['v','e','cnt']].agg({'v':'sum','e':'sum','cnt':'count'}).reset_index()
        try:
            temp = groupby.split(';')
            groupby_list = temp[0].split(',')
            kvs = temp[1].split(',')
            #keys = [ item.split(':')[0] for item in kvs]
            aggs = { item.split(':')[0]:item.split(':')[1] for item in kvs}
            if groupby_list and aggs:        
                df = df.groupby(by=groupby_list).agg(aggs).reset_index()
            return df
        except Exception as e:
            raise Exception(f"groupby express example `code,mc;price:sum,zf:mean,...` ,but `{groupby}` is error:{e}")
    def _parse_add_express(self,df:pd.DataFrame,add_cols:str)->pd.DataFrame:
        # a=x:a.shift(-5)*b;y:(a.mean()+b.abs())/2;z:lambda row:'U' if row.a>row.b else 'D'...
        try:
            add_col_parts = [item for item in add_cols.split(';')]
            for add_col in add_col_parts:
                item = add_col.split(':')
                key = item[0]
                value=item[1]
                if value.startswith('lambda'):
                    value = f'{value}:{item[2]}'
                    func=eval(value)
                    df[key] = df.apply(func,axis=1)
                else:
                    df[key] = df.eval(value)         
            return df
        except Exception as e:
            raise Exception(f"add column express example `x:a.shift(-5)*b;y:(a.mean()+b.abs())/2;z:lambda row:'U' if row.a>row.b else 'D'...` ,but `{add_cols}` is error:{e}")
    def _parse_order_express(self,df:pd.DataFrame,order:str)->pd.DataFrame:
        try:
            express = re.findall(r'(add|sub|mul|div|avg)\((.*)\)',order)
            if not express:
                express=[order]
            else:
                express = [express[0][0]] + express[0][1].split(',')
            number_pattern = r'^-?\d*\.?\d+$'
            if express[0]=='add' and len(express)>1:
                order = "_order"
                args = [float(arg) if re.match(number_pattern,arg) else df[arg] for arg in express[1:]]
                df["_order"] = args[0]
                for item in args[1:]:
                    df["_order"] = df["_order"] + item
            elif express[0]=='sub' and len(express)==3:
                order = "_order"
                args = [float(arg) if re.match(number_pattern,arg) else df[arg] for arg in express[1:]]
                df["_order"] = args[0] - args[1]
            elif express[0]=='mul' and len(express)>1:
                order = "_order"
                args = [float(arg) if re.match(number_pattern,arg) else df[arg] for arg in express[1:]]
                df["_order"] = args[0]
                for item in args[1:]:
                    df["_order"] = df["_order"] * item
            elif express[0]=='div' and len(express)==3:
                order = "_order"
                args = [float(arg) if re.match(number_pattern,arg) else df[arg] for arg in express[1:]]
                df["_order"] = args[0] / args[1]
            elif express[0]=='avg' and len(express)>1:
                order = "_order"
                args = [float(arg) if re.match(number_pattern,arg) else df[arg] for arg in express[1:]]
                df["_order"] = args[0]
                for item in args[1:]:
                    df["_order"] = df["_order"] + item
                df["_order"] = df["_order"]/len(args)
            elif len(express)==1:
                order="_order"
                df['_order'] = df.eval(express[0])            
            else:
                raise Exception(f"{order}表达式不正确")
            df = df.sort_values(by=order,ascending=False)
            return df
        except Exception as e:
            raise Exception(f"order express error:{e}")

    def _to_html(self,df:pd.DataFrame,columns:list[str]=None,formats:str=None,fix_columns:list[str]=[],url=None,desc=None):
        datetime_cols = df.select_dtypes(include=['datetime']).columns.to_list()
        number_cols= df.select_dtypes(include=['int','float']).columns.to_list()
        str_cols= df.select_dtypes(include=['object']).columns.to_list()
        df_state = df.describe()
        df_state.loc['sum']=df[df.select_dtypes(include=['int', 'float']).columns].sum()
        df_state = df_state.T
        state_table = df_state.to_html(table_id='state_table')
        if formats:
            #example f=e_s:10000000,50000000;e_jlr:0,0;v_jlr:0,0
            formats = [item.split(":") for item in formats.split(";")]
            formats = [[item[0]]+[float(x) for x in item[1].split(",")] for item in formats]
            print("formats=",formats)
            formaters = {item[0]: lambda y, fmt='{:,.2f}',min=item[1],max=item[2]: '<span style="color: {}">{}</span>'.format('red' if y>max else 'green' if y<min else 'blac\
    k',fmt.format(y)) for item in formats}
        else:
            formaters = None
        data_table = df.to_html(table_id='data_table',index=False,escape=False,columns=columns,formatters=formaters)
        columns = df.columns.to_list()
        fix_col_index = list(filter(lambda x:x!=-1,[ columns.index(col)+1 if col in columns else -1 for col in fix_columns]))
        datetime_col_index = list(filter(lambda x:x!=-1,[ columns.index(col)+1 if col in columns else -1 for col in datetime_cols]))
        number_col_index = list(filter(lambda x:x!=-1,[ columns.index(col)+1 if col in columns else -1 for col in number_cols]))
        str_col_index = list(filter(lambda x:x!=-1,[ columns.index(col)+1 if col in columns else -1 for col in str_cols]))
        # print("datetime_cols:",datetime_col_index)
        # print("number_cols:",number_col_index)
        # print("str_cols:",str_col_index)
        content_html = gen_content_html(data_table,state_table,fix_col_index=fix_col_index,url=url,desc=desc)        
        return content_html
    
    def _kdj(self,prices, low_prices, high_prices, n=9, k=3, d=3):
        """
        计算KDJ指标。
        
        参数:
        prices -- 收盘价序列（Pandas Series）
        low_prices -- 最低价序列（Pandas Series）
        high_prices -- 最高价序列（Pandas Series）
        n -- RSV的计算周期，默认为9
        k -- K线的平滑移动平均周期，默认为3
        d -- D线的平滑移动平均周期，默认为3
        
        返回:
        kdj -- KDJ值（Pandas DataFrame）
        """
        # 计算最高价的最高值和最低价的最低值
        high_9 = high_prices.rolling(window=n).max()
        low_9 = low_prices.rolling(window=n).min()
        
        # 计算未成熟随机值RSV
        rsv = (prices - low_9) / (high_9 - low_9) * 100
        
        # 计算K线
        k_line = rsv.ewm(alpha=1/k, adjust=False).mean()
        
        # 计算D线
        d_line = k_line.ewm(alpha=1/d, adjust=False).mean()
        
        # 计算J线
        j_line = 3 * k_line - 2 * d_line
        
        # 将结果存入DataFrame
        kdj = pd.DataFrame({
            'kl': k_line,
            'dl': d_line,
            'jl': j_line
        })
    
        return kdj
    def _macd(self,prices, short_period=12, long_period=26, signal_period=9):
        """
        计算MACD值。
        
        参数:
        prices -- 股价序列（Pandas Series）
        short_period -- 快速EMA周期，默认为12
        long_period -- 慢速EMA周期，默认为26
        signal_period -- 信号线周期，默认为9
        
        返回:
        macd -- MACD值（Pandas DataFrame）
        """
        # 计算指数移动平均线
        ema_short = prices.ewm(span=short_period, adjust=True).mean()
        ema_long = prices.ewm(span=long_period, adjust=True).mean()
        
        # 计算MACD线
        macd_line = ema_short - ema_long
        
        # 计算信号线
        signal_line = macd_line.ewm(span=signal_period, adjust=True).mean()
        
        # 计算MACD柱状图
        histogram = (macd_line - signal_line)*2
        
        # 将结果存入DataFrame
        macd = pd.DataFrame({
            'dif': macd_line,
            'dea': signal_line,
            'macd': histogram
        })
        
        return macd

    def _fetch_rx(self,code:str,*,sdate:str,edate:str,fsjb:str="dn",add_fields={}):
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        mc = code_info['name']
        add_fields = {"mr_code":mr_code,"fsjb":fsjb,"mc":mc}
        try:
            max_date = sqlite3.connect("rx_2024.db").execute(f"select max(d) from rx where mr_code='{mr_code}'").fetchall()[0][0]
            if max_date>=sdate:
                sdate=datetime.strptime(max_date,'%Y-%m-%d')+timedelta(days=1)
        except Exception as e:
            pass
        print(f"start fetch {mc} kline data begin at {sdate}...")
        #res = requests.get(f"{self.mairui_api_url}/hszbc/fsjy/{code}/{fsjb}/{sdate}/{edate}/{self.mairui_token}")
        df = ak.stock_zh_a_hist(mr_code,start_date=sdate,end_date=edate)
        data = df.to_dict(orient="records")
        if data:
            #data = res.json()
            data = [{**item,**add_fields} for item in data]
            print(f"fetch {len(data)} for {mc}")
            return data
        #elif res.status_code==429:
        #    print(f"wait 30s for fetch {code}")
        #    time.sleep(30)
        #    return self._fetch_rx(code,sdate=sdate,edate=edate,fsjb=fsjb,add_fields=add_fields)
        else:
            raise Exception(f"network error on fetch {mc} data with status code {res.status_code}")
    
    def _get_df_source(self,db_name=None,sql=None,columns={},ak_func=None,**ak_kwargs):
        df = pd.DataFrame()
        if not (db_name and sql) and not ak_func:
            raise Exception('必须指定db_name和sql，或者指定ak_func')
        if db_name and sql:
            conn = sqlite3.connect(db_name)
            df = pd.read_sql(sql,conn)
        elif ak_func:
            if ak_kwargs:
                df = ak_func(**ak_kwargs)
            else:
                df = ak_func()
        df = df.rename(columns = columns)
        return df
    def register_router(self):
        from .mairui.mairui_hszg import HSZG
        from .snowball import SnowballStock
        hszg = HSZG()          
        snowball = SnowballStock()
        @self.router.get("/test")
        async def test(req:Request):
            return self._get_rx("600111",sdate="2024-01-01",edate="2024-12-31")

        @self.router.get("/zx/add/{key}")
        async def add_zx(key:str,req:Request):
            '''增加自选股票列表'''
            try:
                codes = [item for item in req.query_params.get('code','').split(',') if not item=='']
                if len(codes)>0:
                    try:
                        df = pd.read_sql(f"select * from zx where key='{key}'",self.sqlite)
                        self.sqlite.execute(f"delete from zx where key='{key}'")
                    except:
                        df = pd.DataFrame([])
                    code_info = [{**self._get_stock_code(code),'key':key} for code in codes]
                    code_info_df = pd.DataFrame(code_info)
                    if df.empty:
                        df = code_info_df
                    else:
                        df = pd.concat([df,code_info_df]).drop_duplicates()
                    df.to_sql("zx",con=self.sqlite,index=False,if_exists="append")
                    return {"message":f"已增加{key}"}
                else:
                    raise Exception("必须指定code参数")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")  
        @self.router.get("/zx/remove/{key}")
        async def remove_zx(key:str,req:Request):
            '''删除自选股票列表的项目,可以指定code或name。如果不指定则删除所以key的项目'''
            try:
                messages=[]
                code = req.query_params.get("code")
                name = req.query_params.get("name")
                if code or name:
                    if code:
                        codes_str = ','.join([f"'{item}'" for item in code.split(',') if item])
                        self.sqlite.execute(f"delete from zx where key='{key}' and code in ({codes_str})")
                        messages.append(f"已删除[{key}]中的code:[{codes_str}]")
                    if name:
                        names_str = ','.join([f"'{item}'" for item in name.split(',') if item])
                        self.sqlite.execute(f"delete from zx where key='{key}' and name in ({names_str})")
                        messages.append(f"已删除[{key}]中的name:[{names_str}]")
                else:
                    self.sqlite.execute(f"delete from zx where key='{key}'")
                    messages.append(f"已删除[{key}]")
                self.sqlite.commit()
                return {"message":",".join(messages)}
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")  
        @self.router.get("/zx/get/{key}")
        async def get_zx(key:str,req:Request):
            '''获取自选股票列表'''
            try:
                return self._get_zx_codes(key)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")  
        @self.router.get("/zx/get_all")
        async def get_all_zx(req:Request):
            '''获取自选股票列表'''
            try:
                df = pd.read_sql("select * from zx order by key",self.sqlite)
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['mr_code','name'])
                return HTMLResponse(content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}") 
             
        @self.router.get("/code_info")
        async def get_code_info(req:Request):
            '''获取code信息列表'''
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes = [item for item in code.split(',') if item]
                    codes_info=[ self._get_stock_code(code) for code in codes]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                df = pd.DataFrame(codes_info)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content) 
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")  
            
        @self.router.get("/es/sql",response_class=HTMLResponse)
        async def es_sql(req:Request):
            try:
                sql = req.query_params.get("q")
                if not sql:
                    raise HTTPException(status_code=400, detail="must give argument [q]!")
                data = self.esLib.sql(sql)
                df = pd.DataFrame(data['rows'],columns=[item['name'] for item in data['columns']])
                if "json" in req.query_params.keys():
                        return df.to_dict(orient='records')
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")  
        @self.router.get("/es/drop/{table}")
        async def es_drop_table(table:str):
            res = self.esLib.drop(table)
            return res
        
        @self.router.get("/sqlite/sql")
        async def sqlite_sql(req:Request):
            try:
                sql = req.query_params.get("q")
                if not sql:
                    raise HTTPException(status_code=400, detail="must give argument [q]!")
                df = pd.read_sql(sql,con=self.sqlite)
                if "json" in req.query_params.keys():
                    return df.to_dict(orient='records')
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")        
        @self.router.get("/sqlite/drop/{table}")
        async def sqlite_drop_table(table:str):
            res = self.esLib.drop(table)
            return res
