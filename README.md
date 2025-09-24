# Binance Vision Data 下载工具

这是一个用于下载Binance历史数据的Python工具，支持下载期货、现货和期权的历史交易数据。

## 安装依赖

```bash
pip install requests tqdm
```

## 使用方法

### 1. 下载目录结构（首次运行必需）

首先需要下载XML目录文件来获取数据结构：

```bash
python download_xml_multiprocess.py --max-depth 10 --workers 8
```

### 2. 下载数据文件

下载指定年月的数据：

```bash
# 下载2025年4月的数据
python download_zip_data.py --year 2025 --month 04

# 下载2024年12月的数据  
python download_zip_data.py --year 2024 --month 12
```

下载所有数据：

```bash
python download_zip_data.py
```

## 参数说明

### download_xml_multiprocess.py
- `--workers`: 并发进程数（默认64）
- `--max-depth`: 最大下载深度（默认10）
- `--save-dir`: 保存目录（默认./data）

### download_zip_data.py
- `--year`: 年份（如2025）
- `--month`: 月份（如04）
- `--workers`: 并发线程数（默认8）
- `--download-dir`: 下载目录（默认./downloads）
- `--data-dir`: xml数据文件夹

## 下载流程

1. 运行 `download_xml_multiprocess.py` 获取目录结构
2. 运行 `download_zip_data.py` 下载实际数据文件
3. 下载的文件保存在 `downloads` 目录中

