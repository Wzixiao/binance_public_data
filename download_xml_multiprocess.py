#!/usr/bin/env python3
"""
多进程XML下载脚本

用于批量下载Binance Vision数据的XML目录文件。
使用队列管理下载任务，支持多进程并发下载以提高效率。

使用方法:
    python download_xml_multiprocess.py --max-depth 5
    python download_xml_multiprocess.py --workers 8 --max-depth 3
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
from typing import List, Tuple, Set
from multiprocessing import Pool, Manager, Queue, Process
from queue import Empty
import time
import logging

# 配置
S3_BASE_URL = (
    "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="
)
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


def download_xml_worker(
    args_tuple: Tuple[str, Path, int],
) -> Tuple[bool, str, Path, List[str]]:
    """
    多进程XML下载工作函数

    Args:
        args_tuple: (prefix, save_dir, retries) 元组

    Returns:
        (下载是否成功, 错误信息, 保存路径, 子目录前缀列表) 元组
    """
    prefix, save_dir, retries = args_tuple

    # 为每个进程创建独立的session
    session = setup_session(retries=retries)

    try:
        # 创建本地目录
        local_dir = save_dir / prefix
        local_dir.mkdir(exist_ok=True, parents=True)

        # 构建完整的S3 URL
        full_url = S3_BASE_URL + prefix

        # 获取XML内容
        response = session.get(full_url, headers=HEADERS)
        response.raise_for_status()
        xml_content = response.content

        # 保存XML文件到本地
        xml_filename = f"directory_{prefix.replace('/', '_').rstrip('_')}.xml"
        xml_file_path = local_dir / xml_filename

        with open(xml_file_path, "wb") as f:
            f.write(xml_content)

        # 解析XML获取子目录前缀
        child_prefixes = get_common_prefixes(xml_content.decode("utf-8"))

        return True, "", xml_file_path, child_prefixes

    except Exception as e:
        return False, str(e), Path(""), []


def get_common_prefixes(xml_string: str) -> List[str]:
    """从XML内容中提取子目录前缀"""
    prefixes = []
    try:
        root = ET.fromstring(xml_string)
        namespaces = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        for common_prefix_elem in root.findall("s3:CommonPrefixes", namespaces):
            prefix_elem = common_prefix_elem.find("s3:Prefix", namespaces)
            if prefix_elem is not None:
                prefixes.append(prefix_elem.text)
    except ET.ParseError as e:
        print(f"XML 解析错误: {e}")
        return []

    return prefixes


def process_level_with_multiprocessing(
    current_prefixes: List[str],
    save_dir: Path,
    workers: int,
    retries: int,
    level: int,
    progress_callback=None,
) -> Tuple[List[str], int, int]:
    """
    使用多进程处理当前层级的所有前缀

    Args:
        current_prefixes: 当前层级要处理的前缀列表
        save_dir: 保存目录
        workers: 进程数
        retries: 重试次数
        level: 当前层级
        progress_callback: 进度回调函数

    Returns:
        (下一层级前缀列表, 成功数量, 失败数量) 元组
    """
    if not current_prefixes:
        return [], 0, 0

    print(f"\n处理第 {level} 层级，共 {len(current_prefixes)} 个目录...")

    # 准备下载任务
    download_tasks = []
    for prefix in current_prefixes:
        download_tasks.append((prefix, save_dir, retries))

    next_level_prefixes = []
    successful_downloads = 0
    failed_downloads = 0
    failed_tasks = []

    # 使用进程池处理下载任务
    with Pool(processes=workers) as pool:
        # 使用tqdm显示进度
        with tqdm(total=len(download_tasks), desc=f"层级 {level}", unit="dirs") as pbar:
            # 提交所有任务并获取结果
            results = []
            for task in download_tasks:
                result = pool.apply_async(download_xml_worker, (task,))
                results.append((result, task))

            # 收集结果
            for result_async, task in results:
                try:
                    success, error_msg, xml_path, child_prefixes = result_async.get(
                        timeout=60
                    )

                    if success:
                        successful_downloads += 1
                        next_level_prefixes.extend(child_prefixes)
                        pbar.set_description(f"层级 {level} - 成功: {xml_path.name}")
                        if progress_callback:
                            progress_callback(f"成功下载: {xml_path.name}")
                    else:
                        failed_downloads += 1
                        failed_tasks.append((task[0], error_msg))
                        pbar.set_description(f"层级 {level} - 失败: {task[0]}")
                        if progress_callback:
                            progress_callback(f"下载失败: {task[0]} - {error_msg}")

                except Exception as e:
                    failed_downloads += 1
                    failed_tasks.append((task[0], str(e)))
                    pbar.set_description(f"层级 {level} - 异常: {task[0]}")
                    if progress_callback:
                        progress_callback(f"任务异常: {task[0]} - {str(e)}")

                pbar.update(1)

    # 显示当前层级统计
    print(f"层级 {level} 完成: 成功 {successful_downloads}, 失败 {failed_downloads}")
    if failed_tasks:
        print(f"失败的目录:")
        for i, (prefix, error) in enumerate(failed_tasks[:5], 1):
            print(f"  {i}. {prefix}: {error}")
        if len(failed_tasks) > 5:
            print(f"  ... 还有 {len(failed_tasks) - 5} 个失败目录")

    return next_level_prefixes, successful_downloads, failed_downloads


def main():
    parser = argparse.ArgumentParser(
        description="多进程下载Binance Vision XML目录文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python download_xml_multiprocess.py --max-depth 5
    python download_xml_multiprocess.py --workers 8 --max-depth 3 --start-prefix data/futures/cm/daily/
        """,
    )

    parser.add_argument("--workers", type=int, default=64, help="并发进程数 (默认: 4)")

    parser.add_argument(
        "--max-depth", type=int, default=10, help="最大下载深度 (默认: 10)"
    )

    parser.add_argument("--retries", type=int, default=3, help="下载重试次数 (默认: 3)")

    parser.add_argument(
        "--save-dir",
        type=Path,
        default=Path("./data"),
        help="本地保存目录 (默认: ./data)",
    )

    parser.add_argument(
        "--start-prefix",
        type=str,
        default="data/",
        help="起始目录前缀 (默认: data/futures/cm/daily/)",
    )

    parser.add_argument("--log-file", type=Path, help="日志文件路径 (可选)")

    args = parser.parse_args()

    # 设置日志
    if args.log_file:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(args.log_file), logging.StreamHandler()],
        )
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)

    print(f"开始多进程下载XML目录文件...")
    print(f"起始前缀: {args.start_prefix}")
    print(f"本地保存目录: {args.save_dir}")
    print(f"并发进程数: {args.workers}")
    print(f"最大深度: {args.max_depth}")
    print(f"重试次数: {args.retries}")
    print("=" * 60)

    # 创建保存目录
    args.save_dir.mkdir(parents=True, exist_ok=True)

    # 初始化任务队列
    current_prefixes = [args.start_prefix]
    total_successful = 0
    total_failed = 0

    # 进度回调函数
    def progress_callback(message: str):
        logger.info(message)

    # 逐层处理目录
    for level in range(args.max_depth):
        if not current_prefixes:
            print(f"\n第 {level} 层级没有更多目录需要处理，下载完成！")
            break

        # 去重
        current_prefixes = list(set(current_prefixes))

        # 处理当前层级
        next_prefixes, successful, failed = process_level_with_multiprocessing(
            current_prefixes=current_prefixes,
            save_dir=args.save_dir,
            workers=args.workers,
            retries=args.retries,
            level=level + 1,
            progress_callback=progress_callback,
        )

        # 更新统计
        total_successful += successful
        total_failed += failed

        # 准备下一层级
        current_prefixes = next_prefixes

        print(f"发现下一层级目录数: {len(next_prefixes)}")

        # 如果没有子目录，提前结束
        if not next_prefixes:
            print(f"第 {level + 1} 层级没有子目录，下载完成！")
            break

    # 最终统计
    print("\n" + "=" * 60)
    print("XML目录文件下载完成!")
    print(f"总成功下载: {total_successful} 个XML文件")
    print(f"总下载失败: {total_failed} 个XML文件")
    print(f"本地保存目录: {args.save_dir}")

    if total_failed > 0:
        print(f"\n注意: 有 {total_failed} 个XML文件下载失败")
        if args.log_file:
            print(f"详细错误信息请查看日志文件: {args.log_file}")

    # 显示目录结构概览
    try:
        xml_count = len(list(args.save_dir.rglob("*.xml")))
        print(f"\n目录结构下载完成，共生成 {xml_count} 个XML文件")
    except Exception as e:
        print(f"统计XML文件数量时出错: {e}")


if __name__ == "__main__":
    main()
