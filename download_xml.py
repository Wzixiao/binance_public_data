from pathlib import Path

from utils import process_s3_directory_recursive

if __name__ == "__main__":
    # S3基础URL配置
    s3_base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="

    # 起始处理的目录前缀
    origin_prefix = "data/"

    # 本地保存目录
    save_dir = Path("./data")

    print("开始处理S3目录结构...")
    print(f"起始前缀: {origin_prefix}")
    print(f"本地保存目录: {save_dir}")
    print("=" * 50)

    # 递归处理S3目录结构，创建本地目录并下载XML文件
    try:
        process_s3_directory_recursive(
            s3_base_url=s3_base_url,
            prefix=origin_prefix,
            save_dir=save_dir,
            max_depth=10,  # 设置最大递归深度，防止无限递归
        )
        print("=" * 50)
        print("S3目录结构处理完成!")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        print("请查看 errors.log 文件获取详细错误信息")
