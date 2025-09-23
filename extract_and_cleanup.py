#!/usr/bin/env python3
"""
批量解压zip文件并清理脚本

用于解压指定目录下的所有zip文件，解压完成后删除原始zip文件。
支持多线程并发解压以提高效率。

使用方法:
    python extract_and_cleanup.py --directory binance_vision_data_202508
    python extract_and_cleanup.py -d ./downloads --workers 8
"""

import argparse
import os
import sys
from pathlib import Path
import zipfile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Tuple
import shutil


def extract_zip_file(args_tuple: Tuple[Path, Path]) -> Tuple[bool, str, Path]:
    """
    解压单个zip文件的工作函数

    Args:
        args_tuple: (zip_file_path, extract_to_dir) 元组

    Returns:
        (解压是否成功, 错误信息, zip文件路径) 元组
    """
    zip_file_path, extract_to_dir = args_tuple

    try:
        # 检查zip文件是否存在
        if not zip_file_path.exists():
            return False, f"文件不存在: {zip_file_path}", zip_file_path

        # 检查是否是有效的zip文件
        if not zipfile.is_zipfile(zip_file_path):
            return False, f"不是有效的zip文件: {zip_file_path}", zip_file_path

        # 创建解压目录（通常解压到zip文件所在的目录）
        if extract_to_dir is None:
            extract_to_dir = zip_file_path.parent

        extract_to_dir.mkdir(parents=True, exist_ok=True)

        # 解压文件
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(extract_to_dir)

        return True, "", zip_file_path

    except zipfile.BadZipFile:
        return False, f"损坏的zip文件: {zip_file_path}", zip_file_path
    except PermissionError:
        return False, f"权限不足: {zip_file_path}", zip_file_path
    except Exception as e:
        return False, f"解压失败 {zip_file_path}: {str(e)}", zip_file_path


def find_all_zip_files(directory: Path) -> List[Path]:
    """
    递归查找目录下的所有zip文件

    Args:
        directory: 要搜索的目录

    Returns:
        zip文件路径列表
    """
    zip_files = []

    if not directory.exists():
        print(f"错误: 目录不存在 {directory}")
        return zip_files

    print(f"正在搜索 {directory} 目录下的zip文件...")

    # 使用glob递归查找所有.zip文件
    for zip_file in directory.rglob("*.zip"):
        zip_files.append(zip_file)

    return zip_files


def get_directory_size(directory: Path) -> int:
    """
    计算目录的总大小（字节）

    Args:
        directory: 目录路径

    Returns:
        目录大小（字节）
    """
    total_size = 0
    try:
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
    except Exception as e:
        print(f"计算目录大小时出错: {e}")

    return total_size


def format_size(size_bytes: int) -> str:
    """
    格式化文件大小显示

    Args:
        size_bytes: 字节大小

    Returns:
        格式化的大小字符串
    """
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024
        i += 1

    return f"{size_bytes:.2f} {size_names[i]}"


def main():
    parser = argparse.ArgumentParser(
        description="批量解压zip文件并清理",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python extract_and_cleanup.py --directory binance_vision_data_202508
    python extract_and_cleanup.py -d ./downloads --workers 8 --no-delete
        """,
    )

    parser.add_argument(
        "-d", "--directory", type=Path, required=True, help="包含zip文件的目录路径"
    )

    parser.add_argument(
        "--workers", type=int, default=4, help="并发解压线程数 (默认: 4)"
    )

    parser.add_argument(
        "--no-delete", action="store_true", help="解压后不删除原始zip文件"
    )

    parser.add_argument(
        "--extract-to", type=Path, help="解压到指定目录 (默认: 解压到zip文件所在目录)"
    )

    args = parser.parse_args()

    # 验证目录
    if not args.directory.exists():
        print(f"错误: 目录不存在 {args.directory}")
        sys.exit(1)

    if not args.directory.is_dir():
        print(f"错误: {args.directory} 不是一个目录")
        sys.exit(1)

    print(f"目标目录: {args.directory}")
    print(f"并发线程数: {args.workers}")
    print(f"解压后删除zip: {'否' if args.no_delete else '是'}")
    if args.extract_to:
        print(f"解压到: {args.extract_to}")
    print("=" * 60)

    # 查找所有zip文件
    zip_files = find_all_zip_files(args.directory)

    if not zip_files:
        print("未找到任何zip文件")
        sys.exit(0)

    print(f"找到 {len(zip_files)} 个zip文件")

    # 计算zip文件总大小
    total_zip_size = 0
    for zip_file in zip_files:
        try:
            total_zip_size += zip_file.stat().st_size
        except:
            pass

    print(f"zip文件总大小: {format_size(total_zip_size)}")

    # 显示前几个文件作为示例
    print("\n将要解压的文件示例:")
    for i, zip_file in enumerate(zip_files[:5], 1):
        print(f"  {i}. {zip_file.relative_to(args.directory)}")
    if len(zip_files) > 5:
        print(f"  ... 还有 {len(zip_files) - 5} 个文件")

    # 确认操作
    action_desc = "解压并删除" if not args.no_delete else "仅解压"
    response = input(f"\n确定要{action_desc}这 {len(zip_files)} 个zip文件吗? (y/N): ")
    if response.lower() not in ["y", "yes"]:
        print("操作已取消")
        sys.exit(0)

    # 准备解压任务
    extract_tasks = []
    for zip_file in zip_files:
        extract_to_dir = args.extract_to if args.extract_to else None
        extract_tasks.append((zip_file, extract_to_dir))

    # 多线程解压文件
    print(f"\n开始使用 {args.workers} 个线程解压 {len(zip_files)} 个文件...")
    successful_extracts = 0
    failed_extracts = 0
    failed_files = []

    # 创建线程锁用于安全地更新进度条
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # 提交所有解压任务
        future_to_task = {
            executor.submit(extract_zip_file, task): task for task in extract_tasks
        }

        # 使用tqdm显示进度
        with tqdm(total=len(extract_tasks), desc="解压进度", unit="files") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                zip_file_path, _ = task

                try:
                    success, error_msg, result_path = future.result()

                    with lock:
                        if success:
                            successful_extracts += 1
                            pbar.set_description(f"解压成功: {result_path.name}")
                        else:
                            failed_extracts += 1
                            failed_files.append((result_path, error_msg))
                            pbar.set_description(f"解压失败: {result_path.name}")

                        pbar.update(1)

                except Exception as e:
                    with lock:
                        failed_extracts += 1
                        failed_files.append((zip_file_path, str(e)))
                        pbar.set_description(f"任务异常: {zip_file_path.name}")
                        pbar.update(1)

    print("\n" + "=" * 60)
    print("解压完成!")
    print(f"成功解压: {successful_extracts} 个文件")
    print(f"解压失败: {failed_extracts} 个文件")

    # 显示失败的文件
    if failed_files:
        print(f"\n解压失败的文件:")
        for i, (failed_path, error_msg) in enumerate(failed_files[:10], 1):
            print(f"  {i}. {failed_path.name}: {error_msg}")
        if len(failed_files) > 10:
            print(f"  ... 还有 {len(failed_files) - 10} 个失败文件")

    # 删除成功解压的zip文件
    if not args.no_delete and successful_extracts > 0:
        print(f"\n开始删除已成功解压的zip文件...")
        deleted_count = 0
        deleted_size = 0

        # 只删除成功解压的zip文件
        successful_zip_files = []
        for zip_file in zip_files:
            # 检查这个文件是否在失败列表中
            is_failed = any(failed_path == zip_file for failed_path, _ in failed_files)
            if not is_failed:
                successful_zip_files.append(zip_file)

        with tqdm(
            total=len(successful_zip_files), desc="删除进度", unit="files"
        ) as pbar:
            for zip_file in successful_zip_files:
                try:
                    file_size = zip_file.stat().st_size
                    zip_file.unlink()
                    deleted_count += 1
                    deleted_size += file_size
                    pbar.set_description(f"已删除: {zip_file.name}")
                except Exception as e:
                    pbar.set_description(f"删除失败: {zip_file.name}")

                pbar.update(1)

        print(f"\n成功删除 {deleted_count} 个zip文件")
        print(f"释放空间: {format_size(deleted_size)}")

        if deleted_count < len(successful_zip_files):
            print(
                f"注意: 有 {len(successful_zip_files) - deleted_count} 个文件删除失败"
            )

    print(f"\n操作完成! 目标目录: {args.directory}")


if __name__ == "__main__":
    main()
