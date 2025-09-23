import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import xml.etree.ElementTree as ET

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
}


def save_error_url(write_text, loacl):
    if os.path.isfile(loacl):
        mode = "a"
    else:
        mode = "w"

    with open(loacl, mode) as f:
        f.write(write_text + "\n")


def send_xml_request_with_retry(url, headers=HEADERS, retries=3, backoff_factor=0.5):
    session = requests.Session()

    #  Create a retry mechanism with the given parameters
    retry = Retry(total=retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)

    # Mount the adapter to the session for both http and https requests
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.get(url, headers=headers)

    # Raise an exception if the status code is not successful
    response.raise_for_status()
    # PDF is binary, so it returns content
    return response.content


def get_common_prefixes(xml_string):
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


def get_zip_files_from_xml(xml_string):
    """从XML内容中提取所有以.zip结尾的文件名"""
    zip_files = []
    try:
        root = ET.fromstring(xml_string)
        namespaces = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        # 查找所有Contents元素
        for contents_elem in root.findall("s3:Contents", namespaces):
            key_elem = contents_elem.find("s3:Key", namespaces)
            if key_elem is not None and key_elem.text:
                # 检查文件是否以.zip结尾
                if key_elem.text.endswith(".zip"):
                    zip_files.append(key_elem.text)
    except ET.ParseError as e:
        print(f"XML 解析错误: {e}")
        return []

    return zip_files


def save_xml_file(xml_content, file_path):
    """保存XML内容到指定文件路径"""
    try:
        with open(file_path, "wb") as f:
            f.write(xml_content)
        print(f"XML文件已保存: {file_path}")
        return True
    except Exception as e:
        print(f"保存XML文件失败 {file_path}: {e}")
        return False


def process_s3_directory_recursive(
    s3_base_url, prefix, save_dir, max_depth=10, current_depth=0
):
    """
    递归处理S3目录结构，创建本地目录并下载XML文件

    Args:
        s3_base_url: S3基础URL
        prefix: 当前处理的前缀路径
        save_dir: 本地保存目录
        max_depth: 最大递归深度，防止无限递归
        current_depth: 当前递归深度
    """
    if current_depth >= max_depth:
        print(f"达到最大递归深度 {max_depth}，停止处理: {prefix}")
        return

    print(f"处理目录层级 {current_depth}: {prefix}")

    # 创建本地目录
    local_dir = save_dir / prefix
    local_dir.mkdir(exist_ok=True, parents=True)
    print(f"创建本地目录: {local_dir}")

    try:
        # 构建完整的S3 URL
        full_url = s3_base_url + prefix
        print(f"请求URL: {full_url}")

        # 获取当前目录的XML内容
        xml_content = send_xml_request_with_retry(full_url)

        # 保存XML文件到本地
        xml_filename = f"directory_{prefix.replace('/', '_').rstrip('_')}.xml"
        xml_file_path = local_dir / xml_filename
        save_xml_file(xml_content, xml_file_path)

        # 获取子目录列表
        child_prefixes = get_common_prefixes(xml_content)
        print(f"发现 {len(child_prefixes)} 个子目录: {child_prefixes}")

        # 递归处理每个子目录
        for child_prefix in child_prefixes:
            process_s3_directory_recursive(
                s3_base_url, child_prefix, save_dir, max_depth, current_depth + 1
            )

    except Exception as e:
        error_msg = f"处理目录失败 {prefix}: {e}"
        print(error_msg)
        save_error_url(error_msg, save_dir / "errors.log")
