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
from torch.utils.data import DataLoader, TensorDataset

class AK_TORCH(AkshareBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def register_router(self):
        @self.router.get("/torch")
        async def test_torch(req:Request):
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
                        print(f'Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}')
                #9. 验证模型
                model.eval()
                predictions = []
                actuals = []

                with torch.no_grad():
                    for batch_x, batch_y in test_loader:
                        tgt = torch.zeros(batch_x.size(0), seq_length, 1)
                        tgt[:, :-1, :] = batch_x[:, :-1, :]
                        output = model(batch_x.permute(1, 0, 2), tgt.permute(1, 0, 2))
                        print(output)
                        predictions.append(output.numpy())
                        actuals.append(batch_y.numpy())
                #10. 将预测和实际结果合并
                predictions = torch.cat(predictions).numpy()
                actuals = torch.cat(actuals).numpy()
                #11. 反归一化
                predictions = predictions * (max_val - min_val) + min_val
                actuals = actuals * (max_val - min_val) + min_val
                
                print("predictions:",predictions)
                print("actuals",actuals)

                return "ok"
                # df = self._prepare_df(df,req)
                # content = self._to_html(df)
                # return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")

