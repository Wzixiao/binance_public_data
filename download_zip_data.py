#!/usr/bin/env python3
"""
Binance Vision Data Downloader

用于根据年份和月份下载Binance Vision数据的脚本。
支持从XML文件中提取符合条件的zip文件并下载到对应文件夹。

使用方法:
    python download_data.py --year 2025 --month 04
    python download_data.py -y 2024 -m 12
"""

import argparse
import os
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import re
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# 配置
S3_BASE_URL = "https://data.binance.vision/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
}


def setup_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """设置带重试机制的requests会话"""
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def extract_zip_files_from_xml(xml_file_path: Path) -> List[str]:
    """
    从XML文件中提取所有zip文件的Key

    Args:
        xml_file_path: XML文件路径

    Returns:
        包含所有zip文件Key的列表
    """
    zip_files = []

    try:
        with open(xml_file_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        root = ET.fromstring(xml_content)
        namespaces = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        # 查找所有Contents元素
        for contents_elem in root.findall("s3:Contents", namespaces):
            key_elem = contents_elem.find("s3:Key", namespaces)
            if key_elem is not None and key_elem.text:
                # 检查文件是否以.zip结尾（排除.CHECKSUM文件）
                if key_elem.text.endswith(".zip") and not key_elem.text.endswith(
                    ".zip.CHECKSUM"
                ):
                    zip_files.append(key_elem.text)

    except ET.ParseError as e:
        print(f"XML解析错误 {xml_file_path}: {e}")
    except FileNotFoundError:
        print(f"文件不存在: {xml_file_path}")
    except Exception as e:
        print(f"处理XML文件时出错 {xml_file_path}: {e}")

    return zip_files


def filter_files_by_date(zip_files: List[str], year: str, month: str) -> List[str]:
    """
    根据年份和月份过滤zip文件

    Args:
        zip_files: zip文件Key列表
        year: 年份（如 "2025"）
        month: 月份（如 "04"）

    Returns:
        过滤后的zip文件列表
    """
    if not year or not month:
        return zip_files

    # 构建日期模式，如 "2025-04"
    date_pattern = f"{year}-{month.zfill(2)}"

    filtered_files = []
    for zip_file in zip_files:
        if date_pattern in zip_file:
            filtered_files.append(zip_file)

    return filtered_files


def download_file_worker(args_tuple: Tuple[str, Path, int]) -> Tuple[bool, str, Path]:
    """
    多进程下载工作函数

    Args:
        args_tuple: (url, local_path, retries) 元组

    Returns:
        (下载是否成功, url, local_path) 元组
    """
    url, local_path, retries = args_tuple

    # 为每个线程创建独立的session
    session = setup_session(retries=retries)
    try:
        # 检查文件是否已存在
        if local_path.exists():
            return True, url, local_path

        # 确保目录存在（线程安全）
        local_path.parent.mkdir(parents=True, exist_ok=True)

        response = session.get(url, headers=HEADERS, stream=True)
        response.raise_for_status()

        # 获取文件大小
        total_size = int(response.headers.get("content-length", 0))

        with open(local_path, "wb") as f:
            if total_size == 0:
                f.write(response.content)
            else:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return True, url, local_path

    except Exception as e:
        # 删除不完整的文件
        if local_path.exists():
            try:
                local_path.unlink()
            except:
                pass
        return False, url, local_path


def find_all_xml_files(data_dir: Path) -> List[Path]:
    """
    查找所有XML文件

    Args:
        data_dir: 数据目录路径

    Returns:
        XML文件路径列表
    """
    xml_files = []
    for xml_file in data_dir.rglob("*.xml"):
        xml_files.append(xml_file)
    return xml_files


def main():
    parser = argparse.ArgumentParser(
        description="下载Binance Vision数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python download_zip_data.py --year 2025 --month 04
    python download_zip_data.py --year 2025 --month 04 --download-dir ./test_downloads
    python download_zip_data.py --download-dir ./all_downloads  # 下载所有数据
    python download_zip_data.py --workers 8  # 使用8个线程下载所有数据
        """,
    )

    parser.add_argument(
        "-y", "--year", help="年份 (如: 2025) - 可选，不指定则下载所有数据"
    )

    parser.add_argument(
        "-m", "--month", help="月份 (如: 04 或 4) - 可选，不指定则下载所有数据"
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("./data"),
        help="数据目录路径 (默认: ./data)",
    )

    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path("./downloads"),
        help="下载目录路径 (默认: ./downloads)",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="并发下载线程数 (默认: 4)",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="下载重试次数 (默认: 3)",
    )

    args = parser.parse_args()

    # 验证参数
    year = None
    month = None

    if args.year or args.month:
        # 如果提供了年份或月份中的任何一个，则两个都必须提供
        if not (args.year and args.month):
            print("错误: 如果指定年份或月份，则必须同时指定两者")
            sys.exit(1)

        try:
            year_int = int(args.year)
            month_int = int(args.month)
            if not (1 <= month_int <= 12):
                raise ValueError("月份必须在1-12之间")

            year = args.year
            month = f"{month_int:02d}"
        except ValueError as e:
            print(f"参数错误: {e}")
            sys.exit(1)

    if year and month:
        print(f"开始搜索 {year}年{month}月 的数据文件...")
    else:
        print("开始搜索所有数据文件...")

    print(f"数据目录: {args.data_dir}")
    print(f"下载目录: {args.download_dir}")
    print(f"并发线程数: {args.workers}")
    print(f"重试次数: {args.retries}")
    print("=" * 60)

    # 检查数据目录是否存在
    if not args.data_dir.exists():
        print(f"错误: 数据目录不存在 {args.data_dir}")
        sys.exit(1)

    # 查找所有XML文件
    print("正在查找XML文件...")
    xml_files = find_all_xml_files(args.data_dir)
    print(f"找到 {len(xml_files)} 个XML文件")

    if not xml_files:
        print("未找到XML文件，请先运行main.py获取目录结构")
        sys.exit(1)

    # 提取所有zip文件
    print("正在解析XML文件...")
    all_zip_files = []

    for xml_file in tqdm(xml_files, desc="解析XML文件"):
        zip_files = extract_zip_files_from_xml(xml_file)
        all_zip_files.extend(zip_files)

    print(f"总共找到 {len(all_zip_files)} 个zip文件")

    # 根据年份月份过滤（如果指定了的话）
    if year and month:
        filtered_files = filter_files_by_date(all_zip_files, year, month)
        print(f"符合 {year}年{month}月 条件的文件: {len(filtered_files)} 个")

        if not filtered_files:
            print(f"未找到 {year}年{month}月 的数据文件")
            sys.exit(0)
    else:
        filtered_files = all_zip_files
        print(f"所有zip文件: {len(filtered_files)} 个")

        if not filtered_files:
            print("未找到任何zip文件")
            sys.exit(0)

    # 显示将要下载的文件
    print("\n将要下载的文件:")
    for i, file_key in enumerate(filtered_files[:10], 1):  # 只显示前10个
        print(f"  {i}. {file_key}")
    if len(filtered_files) > 10:
        print(f"  ... 还有 {len(filtered_files) - 10} 个文件")

    # 确认下载
    if year and month:
        action_desc = f"下载这 {len(filtered_files)} 个 {year}年{month}月 的文件"
    else:
        action_desc = f"下载所有 {len(filtered_files)} 个文件"

    response = input(f"\n确定要{action_desc}吗? (y/N): ")
    if response.lower() not in ["y", "yes"]:
        print("取消下载")
        sys.exit(0)

    # 创建下载目录
    args.download_dir.mkdir(parents=True, exist_ok=True)

    # 准备下载任务
    download_tasks = []
    for file_key in filtered_files:
        download_url = urljoin(S3_BASE_URL, file_key)
        local_path = args.download_dir / file_key
        download_tasks.append((download_url, local_path, args.retries))

    # 多线程下载文件
    print(f"\n开始使用 {args.workers} 个线程下载 {len(filtered_files)} 个文件...")
    successful_downloads = 0
    failed_downloads = 0
    failed_files = []

    # 创建线程锁用于安全地更新进度条
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # 提交所有下载任务
        future_to_task = {
            executor.submit(download_file_worker, task): task for task in download_tasks
        }

        # 使用tqdm显示进度
        with tqdm(total=len(download_tasks), desc="下载进度", unit="files") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                url, local_path, _ = task

                try:
                    success, result_url, result_path = future.result()

                    with lock:
                        if success:
                            successful_downloads += 1
                            pbar.set_description(f"下载成功: {result_path.name}")
                        else:
                            failed_downloads += 1
                            failed_files.append(result_url)
                            pbar.set_description(f"下载失败: {result_path.name}")

                        pbar.update(1)

                except Exception as e:
                    with lock:
                        failed_downloads += 1
                        failed_files.append(url)
                        pbar.set_description(f"任务异常: {Path(url).name}")
                        pbar.update(1)

    # 下载完成统计
    print("\n" + "=" * 60)
    print("下载完成!")
    print(f"成功下载: {successful_downloads} 个文件")
    print(f"下载失败: {failed_downloads} 个文件")
    print(f"下载目录: {args.download_dir}")

    if failed_downloads > 0:
        print(f"\n注意: 有 {failed_downloads} 个文件下载失败，请检查网络连接后重新运行")
        print("\n失败的文件列表:")
        for i, failed_url in enumerate(failed_files[:10], 1):
            print(f"  {i}. {failed_url}")
        if len(failed_files) > 10:
            print(f"  ... 还有 {len(failed_files) - 10} 个失败文件")


if __name__ == "__main__":
    main()
