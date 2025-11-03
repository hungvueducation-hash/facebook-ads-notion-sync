"""
Auto Delete - Notion Database Clearer (8 threads, no confirmation)

Cách sử dụng:
    python test_database_clearer.py

Tốc độ:
    - Sequential: 500 pages = 250 giây
    - 8 threads: 500 pages = 30-40 giây (8x nhanh!)
    
✅ Xóa luôn, không cần xác nhận!
"""

import os
from dotenv import load_dotenv
from module.notion_database_clearer import NotionDatabaseClearer
from concurrent.futures import ThreadPoolExecutor
import time
import threading

load_dotenv()

NOTION_API_KEY = os.getenv("NOTION_API_KEY")

config = {}
try:
    with open(".env.config", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
except FileNotFoundError:
    print("✗ Lỗi: Không tìm thấy file .env.config")
    exit(1)

NOTION_DATABASE_ID_DAILY = config.get("NOTION_DATABASE_ID_DAILY")

if not NOTION_API_KEY or not NOTION_DATABASE_ID_DAILY:
    print("✗ Lỗi: Thiếu NOTION_API_KEY hoặc NOTION_DATABASE_ID_DAILY")
    exit(1)

print("=" * 60)
print("🚀 Notion Database Clearer (Auto Mode)")
print("=" * 60)

clearer = NotionDatabaseClearer(NOTION_API_KEY)
pages = clearer.get_all_pages(NOTION_DATABASE_ID_DAILY)
total_pages = len(pages)

print(f"📊 Tìm thấy: {total_pages} pages")

if total_pages == 0:
    print("✅ Database đã trống!")
    exit(0)

print(f"⚡ Cấu hình: 8 threads")
print(f"⏱️  Bắt đầu xóa...\n")

start_time = time.time()
result_lock = threading.Lock()
deleted_count = 0
failed_count = 0

def delete_worker(page_id):
    global deleted_count, failed_count
    try:
        if clearer.delete_page(page_id):
            with result_lock:
                deleted_count += 1
        else:
            with result_lock:
                failed_count += 1
    except Exception:
        with result_lock:
            failed_count += 1

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(delete_worker, page["id"]) for page in pages]
    
    completed = 0
    for future in futures:
        future.result()
        completed += 1
        if completed % 50 == 0 or completed == total_pages:
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            print(f"[{completed}/{total_pages}] ({100*completed//total_pages}%) {rate:.1f} pages/s")

elapsed_time = time.time() - start_time

print("\n" + "=" * 60)
print("✅ Hoàn thành!")
print("=" * 60)
print(f"Tổng: {total_pages} | Thành công: {deleted_count} | Lỗi: {failed_count}")
print(f"Thời gian: {elapsed_time:.1f}s | Tốc độ: {total_pages/elapsed_time:.1f} pages/s")
print("=" * 60)
