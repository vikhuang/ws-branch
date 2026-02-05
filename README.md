# ws-branch

高速 PNL 運算：JSON → Parquet → 3D Tensor

## 使用

```bash
uv sync
uv run python etl.py 2345.json          # 輸出 daily_trade_summary.parquet
uv run python tensor.py 2345.parquet    # 輸出 pnl_tensor.npy
```

## Docker

```bash
docker build -t ws-branch .
docker run -v $(pwd):/app ws-branch etl.py 2345.json
```
