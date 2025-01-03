from datetime import datetime, timedelta
import sqlite3
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests

import akshare as ak
import pandas as pd
from tqdm import tqdm

from .ak_base import AkshareBase
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np

class AK_TORCH(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def register_router(self):
        @self.router.get("/torch2")
        async def test_torch2(req:Request):
            '''测试torch2,根据前input_days天的数据预测后output_days的股价'''
            try:
                df = self._get_df_source("daily_pro.db","select * from daily where code='000001'")
                daily = df[['c','h','l']]
                daily.reset_index(drop=True,inplace=True)
                daily_numpy = daily.values
                #daily_numpy = np.random.rand(200)*100
                input_days = 20
                output_days = 3
                data_len = len(daily_numpy) - input_days - output_days +1
                src_data = torch.tensor([daily_numpy[i:i+input_days] for i in range(data_len)]).float()
                tgt_data = torch.tensor([daily_numpy[i+input_days:i+input_days+output_days] for i in range(data_len)]).float()
                print(src_data.shape,tgt_data.shape)
                #return 0
                d_model = 64
                nhead = 4
                num_layers = 6
                dropout = 0.1
                feature=3
                class StockTransformer(nn.Module):
                    def __init__(self,d_model,nhead,num_layers,dropout):
                        super(StockTransformer,self).__init__()
                        self.input_linear = nn.Linear(feature,d_model)
                        self.transformer = nn.Transformer(d_model,nhead,num_layers,dropout=dropout)
                        self.output_linear = nn.Linear(d_model,1)
                    def forward(self,src,tgt):
                        src = self.input_linear(src)
                        tgt = self.input_linear(tgt)
                        output = self.transformer(src,tgt)
                        output = self.output_linear(output)
                        return output
                model = StockTransformer(d_model,nhead,num_layers,dropout)
                epochs = 10
                lr = 0.001
                batch_size = 16

                criterion = nn.MSELoss()
                optimizer = optim.Adam(model.parameters(),lr=lr)
                for epoch in range(epochs):
                    for i in range(0,data_len,batch_size):
                        src_batch = src_data[i:i+batch_size].transpose(0,1)
                        tgt_batch = tgt_data[i:i+batch_size].transpose(0,1)
                        #print(epoch,src_batch.size(),tgt_batch.size(),tgt_batch[:-1].shape,tgt_batch[1:].shape)
                        optimizer.zero_grad()
                        output = model(src_batch,tgt_batch[:-1])
                        loss = criterion(output,tgt_batch[1:])
                        loss.backward()
                        optimizer.step()
                    print(f"Epoch {epoch+1}/{epochs},Loss: {loss.item()}")
                
                src = torch.tensor(daily_numpy[-input_days:]).unsqueeze(-1).float()
                tgt = torch.zeros(output_days,1,1)
                print(src.shape,tgt.shape)
                with torch.no_grad():
                    for i in range(output_days):
                        pred = model(src,tgt[:i+1])
                        tgt[i] = pred[-1]
                output = tgt.squeeze().tolist()
                print("Next {} days of stock prices:",output)
                return output
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

        @self.router.get("/torch1")
        async def test_torch1(req:Request):
            """获取当前资金流数据"""
            try:
                df = self._get_df_source("daily_pro.db","select * from daily where code='000001'")
                df = df[['o','h','l','c']]
                df.reset_index(inplace=True)

                #1. 数据转为pytorch张量
                data_tensor = torch.tensor(df['c'].values,dtype=torch.float32).view(-1,1)
                #2. 数据归一化
                min_val = data_tensor.min()
                max_val = data_tensor.max()
                data_normalized_tensor = (data_tensor - min_val) / (max_val - min_val)
                #3.创建序列
                def create_sequences(data, seq_length):
                    sequences = []
                    targets = []
                    for i in range(len(data) - seq_length):
                        seq = data[i:i + seq_length]
                        target = data[i + seq_length]
                        sequences.append(seq)
                        targets.append(target)
                    return torch.stack(sequences), torch.stack(targets)
                seq_length = 30 #通过过去30天的数据来预测下一天
                x,y = create_sequences(data_normalized_tensor,seq_length)
                #4.划分训练集和测试集
                train_size = int(len(x)*0.8)
                x_train,x_test = x[:train_size],x[train_size:]
                y_train,y_test = y[:train_size],y[train_size:]
                #5. 创建数据加载器
                import torch.nn as nn
                import torch.optim as optim
                train_dataset = TensorDataset(x_train, y_train)
                test_dataset = TensorDataset(x_test, y_test)

                train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
                test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)
                #6. 定义Transformer模型
                class TransformerModel(nn.Module):
                    def __init__(self, input_dim, model_dim, n_heads, n_layers):
                        super(TransformerModel, self).__init__()
                        self.embedding = nn.Linear(input_dim, model_dim)
                        self.transformer = nn.Transformer(d_model=model_dim, nhead=n_heads, num_encoder_layers=n_layers)
                        self.fc = nn.Linear(model_dim, 1)

                    def forward(self, src, tgt):
                        src = self.embedding(src)
                        tgt = self.embedding(tgt)
                        output = self.transformer(src, tgt)
                        output = self.fc(output[-1, :, :])  # 取最后一个时间步的输出
                        return output                #7. 舒适化模型、损失函数和优化器
                model = TransformerModel(input_dim=1, model_dim=64, n_heads=4, n_layers=2)
                criterion = nn.MSELoss()
                optimizer = optim.Adam(model.parameters(), lr=0.001)
                #8. 训练模型
                num_epochs = 100
                best_loss = float('inf')
                for epoch in range(num_epochs):
                    model.train()
                    for batch_x, batch_y in train_loader:
                        optimizer.zero_grad()
                        # 使用前一时间步的输入作为目标序列
                        tgt = torch.zeros(batch_x.size(0), seq_length, 1)
                        tgt[:, :-1, :] = batch_x[:, :-1, :]
                        output = model(batch_x.permute(1, 0, 2), tgt.permute(1, 0, 2))
                        loss = criterion(output, batch_y)
                        loss.backward()
                        optimizer.step()
                    
                    if (epoch + 1) % 10 == 0:
                        print(f'Epoch [{epoch + 1}/{num_epochs}], Train Loss: {loss.item():.4f}')
                    #9. 验证模型
                    model.eval()
                    total_loss = 0.0
                    with torch.no_grad():
                        for batch_x, batch_y in test_loader:
                            tgt = torch.zeros(batch_x.size(0), seq_length, 1)
                            tgt[:, :-1, :] = batch_x[:, :-1, :]
                            output = model(batch_x.permute(1, 0, 2), tgt.permute(1, 0, 2))
                            loss = criterion(output, batch_y)
                            total_loss += loss.item()
                    if (epoch + 1) % 10 == 0:
                        print(f'Epoch [{epoch + 1}/{num_epochs}], Val Loss: {total_loss / len(test_loader)}')
                    if total_loss < best_loss:
                        best_loss = total_loss
                        best_model = model
                        torch.save(best_model.state_dict,"transformer.pth")
                # #11. 反归一化
                # predictions = predictions * (max_val - min_val) + min_val
                # actuals = actuals * (max_val - min_val) + min_val
                

                return "ok"
                # df = self._prepare_df(df,req)
                # content = self._to_html(df)
                # return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

