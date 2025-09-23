#!/usr/bin/env python3
"""
XML文件中zip文件统计脚本

用于统计指定目录下所有XML文件中Contents元素包含的.zip文件数量。
支持多进程并发处理以提高效率。

使用方法:
    python count_xml_zip_files.py --directory ./data
    python count_xml_zip_files.py -d ./binance_vision_data_202508 --workers 8
"""

import argparse
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from tqdm import tqdm
from typing import List, Tuple, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
import threading
from collections import defaultdict
import json


def count_zip_files_in_xml(xml_file_path: Path) -> Tuple[str, int, List[str], str]:
    """
    统计单个XML文件中的zip文件数量

    Args:
        xml_file_path: XML文件路径

    Returns:
        (文件路径, zip文件数量, zip文件列表, 错误信息) 元组
    """
    try:
        with open(xml_file_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        root = ET.fromstring(xml_content)
        namespaces = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        zip_files = []

        # 查找所有Contents元素
        for contents_elem in root.findall("s3:Contents", namespaces):
            key_elem = contents_elem.find("s3:Key", namespaces)
            if key_elem is not None and key_elem.text:
                # 检查文件是否以.zip结尾（排除.CHECKSUM文件）
                if key_elem.text.endswith(".zip") and not key_elem.text.endswith(
                    ".zip.CHECKSUM"
                ):
                    zip_files.append(key_elem.text)

        return str(xml_file_path), len(zip_files), zip_files, ""

    except ET.ParseError as e:
        return str(xml_file_path), 0, [], f"XML解析错误: {e}"
    except FileNotFoundError:
        return str(xml_file_path), 0, [], "文件不存在"
    except Exception as e:
        return str(xml_file_path), 0, [], f"处理错误: {e}"


def find_all_xml_files(directory: Path) -> List[Path]:
    """
    递归查找目录下的所有XML文件

    Args:
        directory: 要搜索的目录

    Returns:
        XML文件路径列表
    """
    xml_files = []

    if not directory.exists():
        print(f"错误: 目录不存在 {directory}")
        return xml_files

    print(f"正在搜索 {directory} 目录下的XML文件...")

    # 使用glob递归查找所有.xml文件
    for xml_file in directory.rglob("*.xml"):
        xml_files.append(xml_file)

    return xml_files


def format_number(num: int) -> str:
    """
    格式化数字显示（添加千位分隔符）

    Args:
        num: 要格式化的数字

    Returns:
        格式化后的字符串
    """
    return f"{num:,}"


def analyze_zip_distribution(results: List[Tuple[str, int, List[str], str]]) -> Dict:
    """
    分析zip文件分布情况

    Args:
        results: 处理结果列表

    Returns:
        分析结果字典
    """
    analysis = {
        "total_xml_files": len(results),
        "total_zip_files": 0,
        "xml_with_zips": 0,
        "xml_without_zips": 0,
        "error_files": 0,
        "distribution": defaultdict(int),  # zip数量分布
        "top_xml_files": [],  # zip数量最多的XML文件
        "file_type_stats": defaultdict(int),  # 不同类型文件统计
    }

    valid_results = []

    for xml_path, zip_count, zip_list, error in results:
        if error:
            analysis["error_files"] += 1
            continue

        valid_results.append((xml_path, zip_count, zip_list))
        analysis["total_zip_files"] += zip_count

        if zip_count > 0:
            analysis["xml_with_zips"] += 1
            analysis["distribution"][zip_count] += 1

            # 分析文件类型
            for zip_file in zip_list:
                # 从文件名中提取类型信息
                if "trades" in zip_file:
                    analysis["file_type_stats"]["trades"] += 1
                elif "klines" in zip_file:
                    analysis["file_type_stats"]["klines"] += 1
                elif "bookDepth" in zip_file:
                    analysis["file_type_stats"]["bookDepth"] += 1
                elif "aggTrades" in zip_file:
                    analysis["file_type_stats"]["aggTrades"] += 1
                elif "bookTicker" in zip_file:
                    analysis["file_type_stats"]["bookTicker"] += 1
                else:
                    analysis["file_type_stats"]["other"] += 1
        else:
            analysis["xml_without_zips"] += 1

    # 找出zip文件数量最多的前10个XML文件
    valid_results.sort(key=lambda x: x[1], reverse=True)
    analysis["top_xml_files"] = valid_results[:10]

    return analysis


def main():
    parser = argparse.ArgumentParser(
        description="统计XML文件中的zip文件数量",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python count_xml_zip_files.py --directory ./data
    python count_xml_zip_files.py -d ./binance_vision_data_202508 --workers 8 --output report.json
        """,
    )

    parser.add_argument(
        "-d", "--directory", type=Path, required=True, help="包含XML文件的目录路径"
    )

    parser.add_argument(
        "--workers", type=int, default=4, help="并发处理进程数 (默认: 4)"
    )

    parser.add_argument("--output", type=Path, help="输出详细报告到JSON文件 (可选)")

    parser.add_argument(
        "--show-details", action="store_true", help="显示详细的文件列表"
    )

    parser.add_argument("--show-errors", action="store_true", help="显示处理错误的文件")

    args = parser.parse_args()

    # 验证目录
    if not args.directory.exists():
        print(f"错误: 目录不存在 {args.directory}")
        sys.exit(1)

    if not args.directory.is_dir():
        print(f"错误: {args.directory} 不是一个目录")
        sys.exit(1)

    print(f"目标目录: {args.directory}")
    print(f"并发进程数: {args.workers}")
    print("=" * 60)

    # 查找所有XML文件
    xml_files = find_all_xml_files(args.directory)

    if not xml_files:
        print("未找到任何XML文件")
        sys.exit(0)

    print(f"找到 {format_number(len(xml_files))} 个XML文件")

    # 多进程处理XML文件
    print(f"\n开始使用 {args.workers} 个进程统计zip文件...")
    results = []
    failed_files = []

    # 创建线程锁用于安全地更新进度条
    lock = threading.Lock()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # 提交所有处理任务
        future_to_file = {
            executor.submit(count_zip_files_in_xml, xml_file): xml_file
            for xml_file in xml_files
        }

        # 使用tqdm显示进度
        with tqdm(total=len(xml_files), desc="处理进度", unit="files") as pbar:
            for future in as_completed(future_to_file):
                xml_file = future_to_file[future]

                try:
                    xml_path, zip_count, zip_list, error = future.result()
                    results.append((xml_path, zip_count, zip_list, error))

                    with lock:
                        if error:
                            failed_files.append((xml_path, error))
                            pbar.set_description(f"处理失败: {Path(xml_path).name}")
                        else:
                            pbar.set_description(
                                f"找到 {zip_count} 个zip: {Path(xml_path).name}"
                            )

                        pbar.update(1)

                except Exception as e:
                    with lock:
                        failed_files.append((str(xml_file), str(e)))
                        pbar.set_description(f"任务异常: {xml_file.name}")
                        pbar.update(1)

    # 分析结果
    print("\n" + "=" * 60)
    print("统计分析结果:")

    analysis = analyze_zip_distribution(results)

    print(f"XML文件总数: {format_number(analysis['total_xml_files'])}")
    print(f"zip文件总数: {format_number(analysis['total_zip_files'])}")
    print(f"包含zip的XML文件: {format_number(analysis['xml_with_zips'])}")
    print(f"不包含zip的XML文件: {format_number(analysis['xml_without_zips'])}")
    print(f"处理错误的XML文件: {format_number(analysis['error_files'])}")

    # 显示文件类型统计
    if analysis["file_type_stats"]:
        print(f"\nzip文件类型分布:")
        for file_type, count in sorted(
            analysis["file_type_stats"].items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  {file_type}: {format_number(count)} 个文件")

    # 显示zip数量分布（前10个）
    if analysis["distribution"]:
        print(f"\nzip文件数量分布 (前10个):")
        sorted_dist = sorted(
            analysis["distribution"].items(), key=lambda x: x[1], reverse=True
        )
        for zip_count, xml_count in sorted_dist[:10]:
            print(f"  {zip_count} 个zip文件: {format_number(xml_count)} 个XML文件")

    # 显示包含最多zip文件的XML文件
    if analysis["top_xml_files"]:
        print(f"\n包含zip文件最多的XML文件 (前5个):")
        for i, (xml_path, zip_count, _) in enumerate(analysis["top_xml_files"][:5], 1):
            relative_path = Path(xml_path).relative_to(args.directory)
            print(f"  {i}. {relative_path}: {format_number(zip_count)} 个zip文件")

    # 显示错误文件
    if failed_files and args.show_errors:
        print(f"\n处理错误的文件:")
        for i, (xml_path, error) in enumerate(failed_files[:10], 1):
            relative_path = Path(xml_path).relative_to(args.directory)
            print(f"  {i}. {relative_path}: {error}")
        if len(failed_files) > 10:
            print(f"  ... 还有 {len(failed_files) - 10} 个错误文件")

    # 显示详细信息
    if args.show_details:
        print(f"\n详细文件列表:")
        valid_results = [
            (path, count, files) for path, count, files, error in results if not error
        ]
        valid_results.sort(key=lambda x: x[1], reverse=True)

        for i, (xml_path, zip_count, zip_files) in enumerate(valid_results[:20], 1):
            relative_path = Path(xml_path).relative_to(args.directory)
            print(f"  {i}. {relative_path}: {format_number(zip_count)} 个zip文件")
            if zip_count > 0 and zip_count <= 5:  # 只显示少量文件的具体列表
                for zip_file in zip_files:
                    print(f"     - {Path(zip_file).name}")

        if len(valid_results) > 20:
            print(f"  ... 还有 {len(valid_results) - 20} 个文件")

    # 保存详细报告到JSON文件
    if args.output:
        print(f"\n保存详细报告到: {args.output}")

        # 准备JSON数据
        json_data = {
            "summary": {
                "total_xml_files": analysis["total_xml_files"],
                "total_zip_files": analysis["total_zip_files"],
                "xml_with_zips": analysis["xml_with_zips"],
                "xml_without_zips": analysis["xml_without_zips"],
                "error_files": analysis["error_files"],
            },
            "file_type_distribution": dict(analysis["file_type_stats"]),
            "zip_count_distribution": dict(analysis["distribution"]),
            "top_xml_files": [
                {
                    "path": path,
                    "zip_count": count,
                    "zip_files": files[:100],  # 限制数量避免文件过大
                }
                for path, count, files in analysis["top_xml_files"]
            ],
            "error_files": [
                {"path": path, "error": error} for path, error in failed_files
            ],
        }

        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            print(f"报告已保存到: {args.output}")
        except Exception as e:
            print(f"保存报告失败: {e}")

    print(f"\n统计完成! 目标目录: {args.directory}")


if __name__ == "__main__":
    main()
