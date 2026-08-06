"""
archive_now.py — 手动归档一次，立即写入现有数据

用法：
    python archive_now.py

不用找 PID，不用发信号(kill -USR2)，跑一下这个脚本就会把【此刻】各个来源表里
现有的 SKU 数据，作为一条新的快照追加写进归档表，然后自己退出。

复用的是 sku_sync.py 里已有的逻辑（compute_sku_frequency / write_archive_snapshot），
只是多了这一个"随时手动跑一次"的入口 —— 跟自动的每天8点快照、以及 SIGUSR2 是
同一套写入函数，行为完全一致，包括：
  - 只追加、不覆盖、不删除任何已有的行
  - 每条记录带完整的"日期+时间"（不只是日期），所以一天内跑几次都不会互相冲突

注意：这个脚本要放在跟 sku_sync.py 同一个目录下、并且用同一个 .env 配置
(即同一个 Feishu App 凭据、同一个 ARCHIVE_SPREADSHEET_TOKEN / ARCHIVE_SHEET_ID)。
只是导入函数、跑一次就退出，不会启动 sku_sync.py 里那些常驻的后台线程。
"""
import sys

from sku_sync import compute_sku_frequency, write_archive_snapshot, log


def main():
    log.info("[archive_now] 手动归档：开始读取当前各来源表的数据…")
    merged = compute_sku_frequency()

    if not merged:
        log.warning("[archive_now] 没有读到任何 SKU，跳过本次归档")
        sys.exit(1)

    total_skus = len(merged)
    total_count = sum(sum(counts.values()) for counts in merged.values())
    log.info(f"[archive_now] 读到 {total_skus} 个不同 SKU，累计 {total_count} 条记录，写入归档表…")

    write_archive_snapshot(merged)
    log.info("[archive_now] 完成")


if __name__ == "__main__":
    main()