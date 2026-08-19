import sys
import uuid
import time
import datetime
import re
import os
import sqlite3
import subprocess
import json
import threading
from queue import Queue
import requests
from collections import deque
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
from PyQt5.QtCore import QThread, pyqtSignal, QTimer, QUrl
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTextEdit, QLabel, QFileDialog, QMessageBox, 
    QTabWidget, QGroupBox, QFormLayout, QLineEdit, QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar
)
try:
    from PyQt5.QtWebEngineWidgets import QWebEngineView
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    import pymysql
except ImportError:
    pymysql = None

DB_NAME = "doanh_nghiep.db"
URL_FILE = "urls.txt"
VISITED_FILE = "visited.txt"
CRAWLED_DETAILS_FILE = "crawled_details.txt"
BASE = "https://masothue.com/"

MYSQL_CONFIG = {
    "host": "161.153.108.144",
    "user": "timhieuluat",
    "password": "timhieuluat",
    "database": "timhieuluat"
}

def get_mysql_connection():
    if not pymysql:
        raise Exception("Thư viện pymysql chưa được cài đặt. Vui lòng chạy: pip install pymysql")
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        charset="utf8mb4",
        autocommit=True
    )

def init_mysql_tables():
    if not pymysql:
        return
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `companies` (
              `tax_code` varchar(20) NOT NULL PRIMARY KEY,
              `company_name` longtext DEFAULT NULL,
              `tax_address` longtext DEFAULT NULL,
              `address` longtext DEFAULT NULL,
              `status` varchar(200) DEFAULT NULL,
              `short_name` varchar(500) DEFAULT NULL,
              `legal_rep` longtext DEFAULT NULL,
              `phone` varchar(100) DEFAULT NULL,
              `founding_date` varchar(100) DEFAULT NULL,
              `tax_management` longtext DEFAULT NULL,
              `company_type` varchar(500) DEFAULT NULL,
              `main_industry` longtext DEFAULT NULL,
              `business_lines` longtext DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `crawler_urls` (
              `url` varchar(500) NOT NULL PRIMARY KEY,
              `status` varchar(50) DEFAULT 'PENDING',
              `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `crawler_logs` (
              `id` int AUTO_INCREMENT PRIMARY KEY,
              `log_message` longtext,
              `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Lỗi khởi tạo bảng MySQL: {e}")

def get_mysql_companies_count():
    if not pymysql: return 0
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM companies")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception:
        return 0

def save_url_to_mysql(url, status="PENDING"):
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO crawler_urls (url, status) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE status = VALUES(status)
        """, (url, status))
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False

def save_urls_batch_to_mysql(urls, status="PENDING"):
    if not urls:
        return 0
    unique_urls = list(set(urls))
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO crawler_urls (url, status)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE status = status
        """
        params = [(u, status) for u in unique_urls]
        cursor.executemany(sql, params)
        cursor.close()
        conn.close()
        return len(unique_urls)
    except Exception as e:
        print(f"Lỗi batch save URLs MySQL: {e}")
        return 0

def requeue_incomplete_companies_in_mysql():
    if not pymysql:
        return 0
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT tax_code FROM companies 
            WHERE company_name IS NULL OR company_name = 'Không có' OR company_name = ''
               OR tax_address IS NULL OR tax_address = 'Không có' OR tax_address = ''
               OR address IS NULL OR address = 'Không có' OR address = ''
               OR legal_rep IS NULL OR legal_rep = 'Không có' OR legal_rep = ''
               OR main_industry IS NULL OR main_industry = 'Không có' OR main_industry = ''
               OR business_lines IS NULL OR business_lines = 'Không có' OR business_lines = ''
            LIMIT 5000
        """)
        rows = cursor.fetchall()
        if not rows:
            cursor.close()
            conn.close()
            return 0
        
        tax_codes = [row[0] for row in rows if row[0]]
        if not tax_codes:
            cursor.close()
            conn.close()
            return 0
        
        count_updated = 0
        for tc in tax_codes:
            cursor.execute("""
                UPDATE crawler_urls 
                SET status = 'PENDING' 
                WHERE url LIKE %s AND status != 'PENDING'
            """, (f"%/{tc}%",))
            count_updated += cursor.rowcount
            
        cursor.close()
        conn.close()
        return count_updated
    except Exception as e:
        print(f"Lỗi requeue_incomplete_companies: {e}")
        return 0

def reset_all_urls_to_pending_in_mysql():
    if not pymysql:
        return 0
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE crawler_urls SET status = 'PENDING'")
        count = cursor.rowcount
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Lỗi reset_all_urls_to_pending: {e}")
        return 0

def normalize(url):
    url = urldefrag(url)[0].strip()
    if not url: return ""
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/": path = path.rstrip("/")
    normalized = f"{scheme}://{netloc}{path}"
    if parsed.query: normalized += "?" + parsed.query
    return normalized

def is_internal_domain(url):
    parsed = urlparse(url)
    return not parsed.netloc or parsed.netloc == "masothue.com"

def fetch_url_content_safe(url, logger_signal=None, max_retries=2):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://masothue.com/",
        "Connection": "keep-alive"
    }
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=8)
            if response.status_code == 200:
                return response.text
            elif response.status_code in (429, 403):
                attempt += 1
                time.sleep(0.8)
                continue
            else:
                return None
        except Exception:
            attempt += 1
            time.sleep(0.5)
    return None

# ==========================================
# WORKER KIỂM TRA KẾT NỐI MYSQL
# ==========================================
class TestConnectionWorker(QThread):
    success_signal = pyqtSignal(int)
    error_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            conn = pymysql.connect(
                host=self.config["host"],
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"],
                charset="utf8mb4"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            init_mysql_tables()
            count = get_mysql_companies_count()
            self.success_signal.emit(count)
        except Exception as e:
            self.error_signal.emit(str(e))

# ==========================================
# WORKERS TẢI DỮ LIỆU TỪ MYSQL
# ==========================================
class LoadMySQLDataWorker(QThread):
    batch_loaded_signal = pyqtSignal(list, int, int)
    error_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM companies")
            total_rows = cursor.fetchone()[0]
            if total_rows == 0:
                self.finished_signal.emit("Bảng companies trên MySQL hiện đang trống.")
                cursor.close()
                conn.close()
                return

            cursor.execute("""
                SELECT tax_code, company_name, tax_address, address, status, 
                       short_name, legal_rep, phone, founding_date, tax_management, 
                       company_type, main_industry, business_lines 
                FROM companies
            """)
            
            batch_size = 200
            loaded_count = 0
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                cleaned_rows = [[str(val or "") for val in row] for row in rows]
                loaded_count += len(cleaned_rows)
                self.batch_loaded_signal.emit(cleaned_rows, loaded_count, total_rows)

            cursor.close()
            conn.close()
            self.finished_signal.emit(f"🎉 Tải thành công tổng cộng {loaded_count:,} bản ghi từ MySQL!")
        except Exception as e:
            self.error_signal.emit(str(e))

class LoadUrlsWorker(QThread):
    urls_loaded_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT url, status, DATE_FORMAT(updated_at, '%%Y-%%m-%%d %%H:%%i:%%s') FROM crawler_urls ORDER BY updated_at DESC LIMIT 5000")
            rows = cursor.fetchall()
            cleaned_rows = [[str(val or "") for val in row] for row in rows]
            cursor.close()
            conn.close()
            self.urls_loaded_signal.emit(cleaned_rows)
        except Exception as e:
            self.error_signal.emit(str(e))

class LoadLogsWorker(QThread):
    logs_loaded_signal = pyqtSignal(list)
    error_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id, log_message, DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:%%i:%%s') FROM crawler_logs ORDER BY id DESC LIMIT 2000")
            rows = cursor.fetchall()
            cleaned_rows = [[str(val or "") for val in row] for row in rows]
            cursor.close()
            conn.close()
            self.logs_loaded_signal.emit(cleaned_rows)
        except Exception as e:
            self.error_signal.emit(str(e))

# ==========================================
# WORKER ĐẨY DỮ LIỆU CỦ VÀO MYSQL
# ==========================================
class PushOldDataWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    finished_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        self.log_signal.emit(">>> Bắt đầu tiến trình đồng bộ dữ liệu cũ lên MySQL...")

        txt_files = [
            (URL_FILE, "PENDING"),
            (VISITED_FILE, "VISITED"),
            (CRAWLED_DETAILS_FILE, "CRAWLED")
        ]
        
        for filename, default_status in txt_files:
            if os.path.exists(filename):
                self.log_signal.emit(f"\n--- Đang xử lý file '{filename}' ---")
                try:
                    with open(filename, "r", encoding="utf-8") as f:
                        lines = [line.strip() for line in f if line.strip()]
                    
                    total_urls = len(lines)
                    if total_urls > 0:
                        self.log_signal.emit(f"Tổng số URL tìm thấy: {total_urls:,}")
                        conn = get_mysql_connection()
                        cursor = conn.cursor()
                        
                        sql_insert_url = """
                            INSERT INTO crawler_urls (url, status) 
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE status = VALUES(status)
                        """
                        
                        success_count = 0
                        batch_size = 2000
                        for i in range(0, total_urls, batch_size):
                            batch_urls = lines[i:i+batch_size]
                            data_to_insert = [(u, default_status) for u in batch_urls]
                            try:
                                cursor.executemany(sql_insert_url, data_to_insert)
                                success_count += len(batch_urls)
                            except Exception as ex:
                                self.log_signal.emit(f"⚠️ Lỗi khi đẩy nhóm URL {i}: {ex}")
                            
                            current_processed = min(i + batch_size, total_urls)
                            self.progress_signal.emit(current_processed, total_urls)
                            self.log_signal.emit(f"   + Đã đẩy thành công: {success_count:,} / {total_urls:,} URLs lên MySQL (Đang xử lý đến {current_processed:,})...")

                        cursor.close()
                        conn.close()
                        self.log_signal.emit(f"✅ Hoàn tất xử lý file {filename}! Tổng cộng đã đẩy thành công: {success_count:,}/{total_urls:,}")
                except Exception as ex:
                    self.log_signal.emit(f"❌ Lỗi đọc file {filename}: {ex}")

        if os.path.exists(DB_NAME):
            self.log_signal.emit(f"\n--- Đang xử lý dữ liệu từ file SQLite '{DB_NAME}' ---")
            try:
                sqlite_conn = sqlite3.connect(DB_NAME)
                sqlite_cursor = sqlite_conn.cursor()
                sqlite_cursor.execute("SELECT * FROM companies")
                rows = sqlite_cursor.fetchall()
                sqlite_conn.close()

                total_rows = len(rows)
                if total_rows > 0:
                    self.log_signal.emit(f"Tổng số bản ghi Doanh nghiệp tìm thấy: {total_rows:,}")
                    conn = get_mysql_connection()
                    cursor = conn.cursor()

                    sql_upsert = """
                        INSERT INTO `companies` (
                            `tax_code`, `company_name`, `tax_address`, `address`, `status`,
                            `short_name`, `legal_rep`, `phone`, `founding_date`, `tax_management`,
                            `company_type`, `main_industry`, `business_lines`
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            `company_name`=VALUES(`company_name`),
                            `tax_address`=VALUES(`tax_address`),
                            `address`=VALUES(`address`),
                            `status`=VALUES(`status`),
                            `short_name`=VALUES(`short_name`),
                            `legal_rep`=VALUES(`legal_rep`),
                            `phone`=VALUES(`phone`),
                            `founding_date`=VALUES(`founding_date`),
                            `tax_management`=VALUES(`tax_management`),
                            `company_type`=VALUES(`company_type`),
                            `main_industry`=VALUES(`main_industry`),
                            `business_lines`=VALUES(`business_lines`)
                    """

                    success_count = 0
                    batch_size = 100
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i+batch_size]
                        try:
                            cursor.executemany(sql_upsert, batch)
                            success_count += len(batch)
                        except Exception as ex:
                            self.log_signal.emit(f"⚠️ Lỗi đẩy nhóm doanh nghiệp {i}: {ex}")
                            
                        current_processed = min(i + batch_size, total_rows)
                        self.progress_signal.emit(current_processed, total_rows)
                        self.log_signal.emit(f"   + Đã đẩy thành công: {success_count:,} / {total_rows:,} Doanh nghiệp lên MySQL (Đang xử lý đến {current_processed:,})...")

                    cursor.close()
                    conn.close()
                    self.finished_signal.emit(f"🎉 Hoàn thành xuất sắc! Đã xử lý {total_rows:,} Doanh nghiệp. Đẩy thành công: {success_count:,} bản ghi.")
                else:
                    self.finished_signal.emit("Không có bản ghi doanh nghiệp nào trong SQLite để đẩy.")
            except Exception as e:
                self.log_signal.emit(f"❌ Lỗi khi đẩy dữ liệu SQLite: {str(e)}")
        else:
            self.finished_signal.emit("Hoàn thành! Đã đẩy các file URLs lên MySQL.")

# ==========================================
# CRAWL WORKERS (ASYNC DB WRITER & TRUE DEEP SPIDER & INFINITE LOOP)
# ==========================================
class DatabaseWriterThread(QThread):
    log_signal = pyqtSignal(str)
    count_signal = pyqtSignal(int)

    def __init__(self):
        super().__init__()
        self.queue = Queue()
        self.is_running = True

    def run(self):
        while self.is_running:
            batch = []
            while len(batch) < 15 and not self.queue.empty():
                batch.append(self.queue.get())
            
            if batch:
                try:
                    conn_mysql = get_mysql_connection()
                    cursor_mysql = conn_mysql.cursor()

                    sql_insert = """
                        INSERT INTO `companies` (
                            `tax_code`, `company_name`, `tax_address`, `address`, `status`,
                            `short_name`, `legal_rep`, `phone`, `founding_date`, `tax_management`,
                            `company_type`, `main_industry`, `business_lines`
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            `company_name`=VALUES(`company_name`),
                            `tax_address`=VALUES(`tax_address`),
                            `address`=VALUES(`address`),
                            `status`=VALUES(`status`),
                            `short_name`=VALUES(`short_name`),
                            `legal_rep`=VALUES(`legal_rep`),
                            `phone`=VALUES(`phone`),
                            `founding_date`=VALUES(`founding_date`),
                            `tax_management`=VALUES(`tax_management`),
                            `company_type`=VALUES(`company_type`),
                            `main_industry`=VALUES(`main_industry`),
                            `business_lines`=VALUES(`business_lines`)
                    """
                    
                    unique_batch_dict = {}
                    for item in batch:
                        unique_batch_dict[item['mst']] = item
                    unique_batch = list(unique_batch_dict.values())
                    
                    mysql_params = [item['data'] for item in unique_batch]
                    urls_to_update = [(item['url'],) for item in unique_batch]
                    
                    cursor_mysql.executemany(sql_insert, mysql_params)
                    cursor_mysql.executemany("UPDATE crawler_urls SET status = 'CRAWLED' WHERE url = %s", urls_to_update)

                    cursor_mysql.close()
                    conn_mysql.close()

                    for item in unique_batch:
                        self.log_signal.emit(f"✅ [LƯU MYSQL THÀNH CÔNG] MST: {item['mst']} - {item['name'][:40]}")
                    
                    total = get_mysql_companies_count()
                    self.count_signal.emit(total)
                except Exception as e:
                    self.log_signal.emit(f"⚠️ Lỗi ghi database nền: {e}")
            else:
                time.sleep(0.3)

    def stop(self):
        self.is_running = False

class Step1HunterWorker(QThread):
    log_signal = pyqtSignal(str)
    def __init__(self):
        super().__init__()
        self.is_running = True

    def run(self):
        self.log_signal.emit("[Bước 1] 🕷️ Bắt đầu tiến trình Nhện Săn Link thông minh (Cào & tỏa link lặp vô hạn)...")
        
        seeds = [
            BASE,
            "https://masothue.com/tra-cuu-ma-so-thue-theo-tinh",
            "https://masothue.com/tra-cuu-ma-so-thue-theo-nganh-nghe"
        ]
        save_urls_batch_to_mysql(seeds, "PENDING")

        while self.is_running:
            target_url = None
            try:
                conn = get_mysql_connection()
                cursor = conn.cursor()
                
                worker_id = "L_" + uuid.uuid4().hex[:8]
                # Ưu tiên lấy link PENDING điều hướng (không chứa MST 10 chữ số)
                cursor.execute("UPDATE crawler_urls SET status = %s WHERE status = 'PENDING' AND url NOT REGEXP '/[0-9]{10}' LIMIT 1", (worker_id,))
                cursor.execute("SELECT url FROM crawler_urls WHERE status = %s LIMIT 1", (worker_id,))
                row = cursor.fetchone()
                
                # Nếu hết link điều hướng, lấy bất kỳ link PENDING nào
                if not row:
                    cursor.execute("UPDATE crawler_urls SET status = %s WHERE status = 'PENDING' LIMIT 1", (worker_id,))
                    cursor.execute("SELECT url FROM crawler_urls WHERE status = %s LIMIT 1", (worker_id,))
                    row = cursor.fetchone()

                if row:
                    target_url = row[0]
                    cursor.execute("UPDATE crawler_urls SET status = 'VISITED' WHERE url = %s", (target_url,))

                cursor.close()
                conn.close()
            except Exception as e:
                self.log_signal.emit(f"⚠️ Nhện săn link lỗi kết nối MySQL: {e}")
                time.sleep(3)
                continue

            if not target_url:
                self.log_signal.emit("⏳ [Nhện] Hàng đợi link điều hướng PENDING trống. Đang nạp lại seed URLs & xoay vòng link điều hướng...")
                save_urls_batch_to_mysql(seeds, "PENDING")
                
                try:
                    conn = get_mysql_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE crawler_urls 
                        SET status = 'PENDING' 
                        WHERE status = 'VISITED' 
                        AND url NOT REGEXP '/[0-9]{10}'
                        ORDER BY updated_at ASC
                        LIMIT 30
                    """)
                    cursor.close()
                    conn.close()
                except Exception:
                    pass

                for _ in range(6):
                    if not self.is_running: break
                    time.sleep(0.5)
                continue

            norm_url = normalize(target_url)
            self.log_signal.emit(f"🔍 [Nhện đang cào & tỏa link từ trang]: {norm_url}")
            
            content = fetch_url_content_safe(norm_url, self.log_signal)
            if content and len(content) > 300:
                try:
                    soup = BeautifulSoup(content, "lxml")
                    a_tags = soup.find_all("a", href=True)
                    
                    found_links = set()
                    mst_links_count = 0
                    nav_links_count = 0

                    for a_tag in a_tags:
                        href = a_tag['href']
                        link = urljoin(norm_url, href)
                        norm_link = normalize(link)
                        
                        if is_internal_domain(norm_link) and "Search?q=" not in norm_link and not any(norm_link.lower().endswith(ext) for ext in ['.jpg', '.png', '.css', '.js', '.pdf', '.ico', '.zip', '.svg', '.woff', '.woff2']):
                            found_links.add(norm_link)
                            if re.search(r'/[0-9]{10}', norm_link):
                                mst_links_count += 1
                            else:
                                nav_links_count += 1
                    
                    if found_links:
                        save_urls_batch_to_mysql(list(found_links), "PENDING")
                        self.log_signal.emit(f"   🎯 Đã bóc tách & đẩy ngay {len(found_links)} link ({nav_links_count} điều hướng | {mst_links_count} doanh nghiệp) lên MySQL (loại trùng lặp)!")
                    else:
                        self.log_signal.emit(f"   ⚠️ Không tìm thấy link mới tại {norm_url}")
                except Exception as ex:
                    self.log_signal.emit(f"⚠️ Lỗi bóc tách link tại {norm_url}: {ex}")

            # Đặt độ trễ 2.0 giây cho nhện săn link để ưu tiên băng thông & log cho luồng cào chi tiết DN
            time.sleep(2.0)

    def stop(self): self.is_running = False

class Step2LiveCrawlerWorker(QThread):
    log_signal = pyqtSignal(str)
    count_signal = pyqtSignal(int)

    def __init__(self, writer_thread):
        super().__init__()
        self.is_running = True
        self.writer_thread = writer_thread
        self.crawled_count = 0

    def bypass_cloudflare(self):
        if not HAS_PLAYWRIGHT: return
        self.log_signal.emit("🛡️ [Cloudflare Bypass] Mở trình duyệt giả lập để qua mặt Bot Check...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    channel="msedge",
                    headless=False,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-first-run",
                        "--no-default-browser-check",
                    ]
                )
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edge/122.0.0.0"
                )
                page = context.new_page()
                page.goto(BASE, timeout=15000)
                self.log_signal.emit("🌐 [Cloudflare Bypass] Đang chờ ở trang chủ 15 giây...")
                time.sleep(15)
                browser.close()
                self.log_signal.emit("✅ [Cloudflare Bypass] Đã đóng trình duyệt, IP an toàn, cào tiếp!")
        except Exception as e:
            self.log_signal.emit(f"❌ [Cloudflare Bypass] Lỗi: {e}")

    def run(self):
        self.log_signal.emit("[Bước 2] 🚀 Tiến trình cào chi tiết chuẩn sát 3s/company khởi chạy...")
        total_db = get_mysql_companies_count()
        self.count_signal.emit(total_db)
        
        while self.is_running:
            detail_urls = []
            try:
                conn = get_mysql_connection()
                cursor = conn.cursor()
                worker_id = "L_" + uuid.uuid4().hex[:8]
                cursor.execute("UPDATE crawler_urls SET status = %s WHERE status = 'PENDING' AND url REGEXP '/[0-9]{10}' LIMIT 10", (worker_id,))
                cursor.execute("SELECT url FROM crawler_urls WHERE status = %s", (worker_id,))
                detail_urls = [row[0] for row in cursor.fetchall()]
                
                # CẬP NHẬT NGAY trạng thái về 'VISITED' để các lượt lặp sau không lấy trùng
                if detail_urls:
                    cursor.executemany("UPDATE crawler_urls SET status = 'VISITED' WHERE url = %s", [(u,) for u in detail_urls])
                
                cursor.close()
                conn.close()
            except Exception as e:
                self.log_signal.emit(f"⚠️ Lỗi truy vấn URL cào chi tiết: {e}")
                time.sleep(2)
                continue

            if not detail_urls:
                self.log_signal.emit("⏳ [Bước 2] Hết link doanh nghiệp PENDING. Đang quét tự động các DN thiếu thông tin...")
                requeued = requeue_incomplete_companies_in_mysql()
                if requeued > 0:
                    self.log_signal.emit(f"🔄 [Bước 2] Đã tự động đưa {requeued:,} URL doanh nghiệp thiếu thông tin về PENDING để cào lại!")
                    time.sleep(1.0)
                    continue
                else:
                    self.log_signal.emit("⏳ [Bước 2] Đang chờ thêm link doanh nghiệp mới từ Nhện Săn Link...")
                    time.sleep(2.0)
                    continue

            for target_url in detail_urls:
                if not self.is_running:
                    break
                
                norm_target = normalize(target_url)
                self.log_signal.emit(f"🏢 [ĐANG CÀO CHI TIẾT DN] {norm_target}")
                
                success = False
                while self.is_running and not success:
                    content = fetch_url_content_safe(norm_target, self.log_signal)
                    
                    if content and len(content) > 500:
                        try:
                            soup = BeautifulSoup(content, "lxml")
                            
                            mst_match = re.search(r'/(\d{10})', norm_target)
                            mst = mst_match.group(1) if mst_match else "Không có"
                            
                            h1 = soup.find("h1")
                            company_name = re.sub(r'^\d+\s*-\s*', '', h1.get_text(strip=True)) if h1 else "Không có"

                            def get_val(label):
                                td = soup.find(lambda tag: tag.name == "td" and label in tag.text)
                                if td and td.find_next_sibling("td"):
                                    for b in td.find_next_sibling("td").find_all("button"): b.decompose()
                                    return re.sub(r"\s+", " ", td.find_next_sibling("td").get_text(separator=" ")).strip()
                                return "Không có"

                            business_lines_list = []
                            table_nganh = soup.find("table", {"id": "orther_dl"}) or soup.find("table", class_="table")
                            if table_nganh:
                                for row in table_nganh.find_all("tr"):
                                    cols = row.find_all("td")
                                    if len(cols) >= 2:
                                        code = cols[0].get_text(strip=True)
                                        name = cols[1].get_text(strip=True)
                                        if code and name:
                                            business_lines_list.append(f"{code} - {name}")
                            
                            business_lines_text = "\n".join(business_lines_list) if business_lines_list else "Không có"
                            main_ind = get_val("Ngành nghề chính")

                            company_data = (
                                mst, company_name, get_val("Địa chỉ Thuế"), get_val("Địa chỉ"), 
                                get_val("Tình trạng"), get_val("Tên viết tắt"), get_val("Người đại diện"), 
                                get_val("Điện thoại"), get_val("Ngày hoạt động"), get_val("Quản lý bởi"), 
                                get_val("Loại hình DN"), main_ind, business_lines_text
                            )

                            self.writer_thread.queue.put({
                                'data': company_data,
                                'url': norm_target,
                                'mst': mst,
                                'name': company_name
                            })
                            self.crawled_count += 1
                            success = True
                            if self.crawled_count >= 5:
                                self.bypass_cloudflare()
                                self.crawled_count = 0

                        except Exception as ex:
                            self.log_signal.emit(f"❌ Lỗi xử lý cào URL {norm_target}: {ex}")
                            save_url_to_mysql(norm_target, "CRAWLED")
                            success = True
                    else:
                        self.log_signal.emit(f"⚠️ Phát hiện bị chặn tại {norm_target}. Trả về PENDING và mở Bypass thử lại ngay...")
                        save_url_to_mysql(norm_target, "PENDING")
                        self.bypass_cloudflare()
                        time.sleep(1.0)

                if success:
                    time.sleep(3.0)

    def stop(self): self.is_running = False

# ==========================================
# CLOUDFLARE VERIFIER
# ==========================================
class CloudflareVerifierThread(QThread):
    log_signal = pyqtSignal(str)

    def __init__(self, url=BASE, interval=10, stay_seconds=5):
        super().__init__()
        self.url = url
        self.interval = interval
        self.stay_seconds = stay_seconds
        self.is_running = True

    def _open_once(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-first-run",
                    "--no-default-browser-check",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/122.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                page.goto(self.url, timeout=10000)
                self.log_signal.emit(f"🌐 [Cloudflare Verifier] Đã vào {self.url}, đợi {self.stay_seconds}s...")
            except Exception:
                pass

            for _ in range(self.stay_seconds * 10):
                if not self.is_running:
                    break
                time.sleep(0.1)

            browser.close()
            self.log_signal.emit(f"✅ [Cloudflare Verifier] Đã đóng Chromium sau {self.stay_seconds}s.")

    def run(self):
        if not HAS_PLAYWRIGHT:
            self.log_signal.emit("❌ [Cloudflare Verifier] Chưa cài playwright. Chạy: pip install playwright && playwright install chromium")
            return

        self.log_signal.emit("🛡️ [Cloudflare Verifier] Bắt đầu - cứ 10s mở Chromium xác minh tại masothue.com...")

        while self.is_running:
            try:
                self._open_once()
            except Exception as e:
                self.log_signal.emit(f"❌ [Cloudflare Verifier] Lỗi: {e}")

            for _ in range(self.interval * 10):
                if not self.is_running:
                    break
                time.sleep(0.1)

    def stop(self):
        self.is_running = False

# ==========================================
# GIAO DIỆN CHÍNH (GUI)
# ==========================================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        QTimer.singleShot(500, self._auto_connect_on_start)
        
        if "--auto-run" in sys.argv:
            QTimer.singleShot(2000, self.start_all)
            # Auto exit after 4 hours 50 minutes (17,400,000 ms)
            QTimer.singleShot(17400000, lambda: sys.exit(0))

    def initUI(self):
        self.setWindowTitle("Tool Cào Doanh Nghiệp & MySQL Manager")
        self.resize(1300, 750)

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Tab 1: Trạng thái & Điều khiển
        tab_main = QWidget()
        l_main = QVBoxLayout(tab_main)
        
        g_stats = QGroupBox("📊 Thống kê doanh nghiệp trên Remote MySQL")
        f_stats = QFormLayout(g_stats)
        self.lbl_total_companies = QLabel("Đang kết nối MySQL...")
        self.lbl_total_companies.setStyleSheet("font-size: 16px; font-weight: bold; color: #2980b9;")
        f_stats.addRow("Tổng số doanh nghiệp hiện có trên MySQL:", self.lbl_total_companies)
        l_main.addWidget(g_stats)

        g_ctrl = QGroupBox("Bảng điều khiển hệ thống Crawler MySQL")
        f_ctrl = QFormLayout(g_ctrl)
        hb_btns = QHBoxLayout()
        self.btn_run_all = QPushButton("🚀 Bắt Đầu Cào & Lưu Trực Tiếp Lên MySQL")
        self.btn_run_all.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; padding: 12px;")
        self.btn_run_all.clicked.connect(self.start_all)
        hb_btns.addWidget(self.btn_run_all)

        self.btn_stop_all = QPushButton("⏹ Dừng Lại")
        self.btn_stop_all.setStyleSheet("background-color: #c0392b; color: white; font-weight: bold; padding: 12px;")
        self.btn_stop_all.setEnabled(False)
        self.btn_stop_all.clicked.connect(self.stop_all)
        hb_btns.addWidget(self.btn_stop_all)

        f_ctrl.addRow(hb_btns)

        hb_extra = QHBoxLayout()
        self.btn_requeue_incomplete = QPushButton("🔄 Quét & Cào Lại DN Thiếu Thông Tin")
        self.btn_requeue_incomplete.setStyleSheet("background-color: #e67e22; color: white; font-weight: bold; padding: 10px;")
        self.btn_requeue_incomplete.clicked.connect(self.trigger_requeue_incomplete)
        hb_extra.addWidget(self.btn_requeue_incomplete)

        self.btn_reset_all_urls = QPushButton("♾️ Reset Toàn Bộ URL về PENDING (Cào Lặp Vô Hạn)")
        self.btn_reset_all_urls.setStyleSheet("background-color: #8e44ad; color: white; font-weight: bold; padding: 10px;")
        self.btn_reset_all_urls.clicked.connect(self.trigger_reset_all_urls)
        hb_extra.addWidget(self.btn_reset_all_urls)

        f_ctrl.addRow(hb_extra)

        hb_cf = QHBoxLayout()
        self.btn_cf_start = QPushButton("🛡️ Bật Cloudflare Verifier (10s/lần)")
        self.btn_cf_start.setStyleSheet("background-color: #2471a3; color: white; font-weight: bold; padding: 10px;")
        self.btn_cf_start.clicked.connect(self.start_cf_verifier)
        hb_cf.addWidget(self.btn_cf_start)

        self.btn_cf_stop = QPushButton("🛑 Tắt Verifier")
        self.btn_cf_stop.setStyleSheet("background-color: #717d7e; color: white; font-weight: bold; padding: 10px;")
        self.btn_cf_stop.setEnabled(False)
        self.btn_cf_stop.clicked.connect(self.stop_cf_verifier)
        hb_cf.addWidget(self.btn_cf_stop)

        self.lbl_cf_status = QLabel("⚫ Verifier: Đang tắt")
        self.lbl_cf_status.setStyleSheet("color: #717d7e; font-weight: bold; padding-left: 8px;")
        hb_cf.addWidget(self.lbl_cf_status)

        f_ctrl.addRow(hb_cf)
        l_main.addWidget(g_ctrl)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        l_main.addWidget(self.log_view)
        self.tabs.addTab(tab_main, "⚡ Trạng thái Hoạt động")

        # Tab 2: Doanh nghiệp
        tab_cloud_view = QWidget()
        l_cloud = QVBoxLayout(tab_cloud_view)
        self.btn_load_cloud_data = QPushButton("🔄 Tải dữ liệu Doanh nghiệp từ MySQL (Realtime Streaming)")
        self.btn_load_cloud_data.setStyleSheet("background-color: #8e44ad; color: white; font-weight: bold; padding: 10px;")
        self.btn_load_cloud_data.clicked.connect(self.start_load_cloud_data)
        l_cloud.addWidget(self.btn_load_cloud_data)

        self.load_progress_bar = QProgressBar()
        self.load_progress_bar.setValue(0)
        self.load_progress_bar.setFormat("Đang tải realtime: %v / %m bản ghi (%p%)")
        self.load_progress_bar.setStyleSheet("height: 22px; text-align: center; font-weight: bold;")
        self.load_progress_bar.hide()
        l_cloud.addWidget(self.load_progress_bar)

        self.table_companies = QTableWidget()
        l_cloud.addWidget(self.table_companies)
        self.tabs.addTab(tab_cloud_view, "📂 Doanh nghiệp trên MySQL")

        # Tab 3: Đẩy dữ liệu cũ
        tab_push_old = QWidget()
        l_push_old = QVBoxLayout(tab_push_old)
        g_push_old = QGroupBox("Đẩy dữ liệu cũ (SQLite & File txt URLs) lên MySQL")
        f_push_old = QFormLayout(g_push_old)
        
        self.btn_push_all_old = QPushButton("☁️ Bắt đầu đẩy toàn bộ dữ liệu cũ lên MySQL")
        self.btn_push_all_old.setStyleSheet("background-color: #d35400; color: white; font-weight: bold; padding: 12px; font-size: 13px;")
        self.btn_push_all_old.clicked.connect(self.start_push_old_data)
        f_push_old.addRow(self.btn_push_all_old)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%v / %m URLs (%p%)")
        self.progress_bar.setStyleSheet("height: 25px; text-align: center; font-weight: bold;")
        f_push_old.addRow(QLabel("Tiến trình đồng bộ:"), self.progress_bar)

        l_push_old.addWidget(g_push_old)

        self.log_push_view = QTextEdit()
        self.log_push_view.setReadOnly(True)
        l_push_old.addWidget(self.log_push_view)
        self.tabs.addTab(tab_push_old, "📤 Đẩy dữ liệu cũ")

        # Tab 4: URLs
        tab_url_view = QWidget()
        l_url = QVBoxLayout(tab_url_view)
        self.btn_load_urls = QPushButton("🔄 Tải danh sách URL từ MySQL")
        self.btn_load_urls.setStyleSheet("background-color: #2980b9; color: white; font-weight: bold; padding: 10px;")
        self.btn_load_urls.clicked.connect(self.start_load_urls)
        l_url.addWidget(self.btn_load_urls)
        self.url_progress_bar = QProgressBar()
        self.url_progress_bar.setRange(0, 0)
        self.url_progress_bar.hide()
        l_url.addWidget(self.url_progress_bar)
        self.table_urls = QTableWidget()
        l_url.addWidget(self.table_urls)
        self.tabs.addTab(tab_url_view, "🌐 URLs trên MySQL")

        # Tab 5: Logs
        tab_log_view = QWidget()
        l_log = QVBoxLayout(tab_log_view)
        self.btn_load_logs = QPushButton("🔄 Tải nhật ký Log từ MySQL")
        self.btn_load_logs.setStyleSheet("background-color: #e67e22; color: white; font-weight: bold; padding: 10px;")
        self.btn_load_logs.clicked.connect(self.start_load_logs)
        l_log.addWidget(self.btn_load_logs)
        self.log_progress_bar = QProgressBar()
        self.log_progress_bar.setRange(0, 0)
        self.log_progress_bar.hide()
        l_log.addWidget(self.log_progress_bar)
        self.table_logs = QTableWidget()
        l_log.addWidget(self.table_logs)
        self.tabs.addTab(tab_log_view, "📜 Nhật ký Log trên MySQL")

        # Tab Browser
        tab_browser = QWidget()
        l_browser = QVBoxLayout(tab_browser)
        hb_browser_ctrl = QHBoxLayout()
        self.browser_url_input = QLineEdit(BASE)
        self.browser_url_input.setPlaceholderText("Nhập URL rồi nhấn Go...")
        self.btn_browser_go = QPushButton("🌐 Go")
        self.btn_browser_go.setStyleSheet("background-color: #1a5276; color: white; font-weight: bold; padding: 6px 14px;")
        self.btn_browser_go.clicked.connect(self._browser_navigate)
        hb_browser_ctrl.addWidget(self.browser_url_input)
        hb_browser_ctrl.addWidget(self.btn_browser_go)
        l_browser.addLayout(hb_browser_ctrl)

        if HAS_WEBENGINE:
            self.web_view = QWebEngineView()
            self.web_view.load(QUrl(BASE))
            l_browser.addWidget(self.web_view)
        else:
            lbl_no_web = QLabel("⚠️ Chưa cài PyQtWebEngine. Chạy: pip install PyQtWebEngine")
            lbl_no_web.setStyleSheet("color: red; font-size: 14px; padding: 20px;")
            l_browser.addWidget(lbl_no_web)

        self.tabs.addTab(tab_browser, "🌐 Trình duyệt")

        # Tab 6: Cấu hình MySQL
        tab_mysql_config = QWidget()
        l_mysql_config = QVBoxLayout(tab_mysql_config)
        g_mysql = QGroupBox("Cấu hình kết nối MySQL Database")
        f_mysql = QFormLayout(g_mysql)
        
        self.txt_host = QLineEdit(MYSQL_CONFIG["host"])
        self.txt_user = QLineEdit(MYSQL_CONFIG["user"])
        self.txt_pass = QLineEdit(MYSQL_CONFIG["password"])
        self.txt_pass.setEchoMode(QLineEdit.Password)
        self.txt_database = QLineEdit(MYSQL_CONFIG["database"])

        f_mysql.addRow("Host/Server IP:", self.txt_host)
        f_mysql.addRow("User:", self.txt_user)
        f_mysql.addRow("Password:", self.txt_pass)
        f_mysql.addRow("Database Name:", self.txt_database)
        l_mysql_config.addWidget(g_mysql)

        self.btn_test_conn = QPushButton("🔌 Kiểm tra kết nối MySQL Database")
        self.btn_test_conn.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; padding: 12px;")
        self.btn_test_conn.clicked.connect(self.test_mysql_connection)
        l_mysql_config.addWidget(self.btn_test_conn)

        self.log_oracle_view = QTextEdit()
        self.log_oracle_view.setReadOnly(True)
        l_mysql_config.addWidget(self.log_oracle_view)
        self.tabs.addTab(tab_mysql_config, "🐬 Cấu hình MySQL")

        self.worker_1 = None
        self.worker_2 = None
        self.writer_thread = None
        self.push_worker = None
        self.load_worker = None
        self.urls_worker = None
        self.logs_worker = None
        self.cf_verifier = None

    def trigger_requeue_incomplete(self):
        self.log_view.append(">>> Đang quét danh sách doanh nghiệp bị thiếu thông tin trong MySQL...")
        count = requeue_incomplete_companies_in_mysql()
        self.log_view.append(f"✅ Đã đưa {count:,} URL doanh nghiệp thiếu thông tin về PENDING để cào lại!")
        QMessageBox.information(self, "Thành công", f"Đã đưa {count:,} URL doanh nghiệp chưa đầy đủ thông tin về trạng thái PENDING để cào lại!")

    def trigger_reset_all_urls(self):
        reply = QMessageBox.question(self, "Xác nhận", "Bạn có chắc chắn muốn đặt lại TOÀN BỘ danh sách URL về trạng thái PENDING để cào lặp vô hạn từ đầu không?", QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            count = reset_all_urls_to_pending_in_mysql()
            self.log_view.append(f"✅ Đã reset toàn bộ {count:,} URL về PENDING!")
            QMessageBox.information(self, "Thành công", f"Đã reset toàn bộ {count:,} URL về PENDING thành công!")

    def _auto_connect_on_start(self):
        self._auto_worker = TestConnectionWorker(dict(MYSQL_CONFIG))
        self._auto_worker.success_signal.connect(self._on_auto_connect_success)
        self._auto_worker.error_signal.connect(self._on_auto_connect_error)
        self._auto_worker.start()

    def _on_auto_connect_success(self, count):
        self.lbl_total_companies.setText(f"{count:,} doanh nghiệp")
        self.lbl_total_companies.setStyleSheet("font-size: 16px; font-weight: bold; color: #27ae60;")

    def _on_auto_connect_error(self, error_msg):
        self.lbl_total_companies.setText("❌ Không thể kết nối MySQL")
        self.lbl_total_companies.setStyleSheet("font-size: 16px; font-weight: bold; color: #e74c3c;")

    def update_initial_stats(self):
        self._auto_connect_on_start()

    def test_mysql_connection(self):
        MYSQL_CONFIG["host"] = self.txt_host.text().strip()
        MYSQL_CONFIG["user"] = self.txt_user.text().strip()
        MYSQL_CONFIG["password"] = self.txt_pass.text()
        MYSQL_CONFIG["database"] = self.txt_database.text().strip()

        self.btn_test_conn.setEnabled(False)
        self.btn_test_conn.setText("⏳ Đang kiểm tra kết nối...")
        self.log_oracle_view.append(">>> Đang kết nối tới Remote MySQL, vui lòng đợi...")

        self.test_conn_worker = TestConnectionWorker(dict(MYSQL_CONFIG))
        self.test_conn_worker.success_signal.connect(self._on_conn_success)
        self.test_conn_worker.error_signal.connect(self._on_conn_error)
        self.test_conn_worker.start()

    def _on_conn_success(self, count):
        self.btn_test_conn.setEnabled(True)
        self.btn_test_conn.setText("🔌 Kiểm tra kết nối MySQL Database")
        self.log_oracle_view.append("✅ Kết nối MySQL thành công!")
        self.lbl_total_companies.setText(f"{count:,} doanh nghiệp")
        QMessageBox.information(self, "Thành công", f"Kết nối MySQL thành công!\nHiện có {count:,} doanh nghiệp trên MySQL.")

    def _on_conn_error(self, error_msg):
        self.btn_test_conn.setEnabled(True)
        self.btn_test_conn.setText("🔌 Kiểm tra kết nối MySQL Database")
        self.log_oracle_view.append(f"❌ Lỗi kết nối: {error_msg}")
        QMessageBox.critical(self, "Lỗi kết nối", error_msg)

    def start_load_cloud_data(self):
        self.btn_load_cloud_data.setEnabled(False)
        self.load_progress_bar.setValue(0)
        self.load_progress_bar.show()
        
        headers = [
            "Mã số thuế", "Tên doanh nghiệp", "Địa chỉ thuế", "Địa chỉ thực tế", "Tình trạng", 
            "Tên viết tắt", "Người đại diện", "Điện thoại", "Ngày hoạt động", "Quản lý bởi", 
            "Loại hình DN", "Ngành nghề chính", "Ngành nghề kinh doanh"
        ]
        self.table_companies.setColumnCount(len(headers))
        self.table_companies.setHorizontalHeaderLabels(headers)
        self.table_companies.setRowCount(0)

        self.load_worker = LoadMySQLDataWorker(MYSQL_CONFIG)
        self.load_worker.batch_loaded_signal.connect(self.append_cloud_table_batch)
        self.load_worker.error_signal.connect(lambda err: self.load_error(err, self.load_progress_bar, self.btn_load_cloud_data))
        self.load_worker.finished_signal.connect(self.load_cloud_finished)
        self.load_worker.start()

    def append_cloud_table_batch(self, batch_rows, current_count, total_count):
        self.load_progress_bar.setMaximum(total_count)
        self.load_progress_bar.setValue(current_count)
        
        start_row = self.table_companies.rowCount()
        self.table_companies.setRowCount(start_row + len(batch_rows))

        for idx, row_data in enumerate(batch_rows):
            target_row = start_row + idx
            for col_idx, cell_data in enumerate(row_data):
                self.table_companies.setItem(target_row, col_idx, QTableWidgetItem(cell_data))

    def load_cloud_finished(self, msg):
        self.load_progress_bar.hide()
        self.btn_load_cloud_data.setEnabled(True)
        self.table_companies.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_companies.setColumnWidth(11, 350)
        self.table_companies.setColumnWidth(12, 500)
        
        total_rows = self.table_companies.rowCount()
        self.lbl_total_companies.setText(f"{total_rows:,} doanh nghiệp")
        QMessageBox.information(self, "Thành công", msg)

    def start_load_urls(self):
        self.btn_load_urls.setEnabled(False)
        self.url_progress_bar.show()
        self.urls_worker = LoadUrlsWorker(MYSQL_CONFIG)
        self.urls_worker.urls_loaded_signal.connect(self.populate_urls_table)
        self.urls_worker.error_signal.connect(lambda err: self.load_error(err, self.url_progress_bar, self.btn_load_urls))
        self.urls_worker.start()

    def populate_urls_table(self, rows):
        self.url_progress_bar.hide()
        self.btn_load_urls.setEnabled(True)
        headers = ["URL", "Trạng thái", "Thời gian cập nhật"]
        self.table_urls.setColumnCount(len(headers))
        self.table_urls.setHorizontalHeaderLabels(headers)
        self.table_urls.setRowCount(len(rows))
        for row_idx, row_data in enumerate(rows):
            for col_idx, cell_data in enumerate(row_data):
                self.table_urls.setItem(row_idx, col_idx, QTableWidgetItem(cell_data))
        self.table_urls.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

    def start_load_logs(self):
        self.btn_load_logs.setEnabled(False)
        self.log_progress_bar.show()
        self.logs_worker = LoadLogsWorker(MYSQL_CONFIG)
        self.logs_worker.logs_loaded_signal.connect(self.populate_logs_table)
        self.logs_worker.error_signal.connect(lambda err: self.load_error(err, self.log_progress_bar, self.btn_load_logs))
        self.logs_worker.start()

    def populate_logs_table(self, rows):
        self.log_progress_bar.hide()
        self.btn_load_logs.setEnabled(True)
        headers = ["ID", "Nội dung Log", "Thời gian"]
        self.table_logs.setColumnCount(len(headers))
        self.table_logs.setHorizontalHeaderLabels(headers)
        self.table_logs.setRowCount(len(rows))
        for row_idx, row_data in enumerate(rows):
            for col_idx, cell_data in enumerate(row_data):
                self.table_logs.setItem(row_idx, col_idx, QTableWidgetItem(cell_data))
        self.table_logs.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

    def load_error(self, err_msg, pbar, btn):
        pbar.hide()
        btn.setEnabled(True)
        QMessageBox.critical(self, "Lỗi tải dữ liệu", err_msg)

    def start_push_old_data(self):
        self.btn_push_all_old.setEnabled(False)
        self.progress_bar.setValue(0)
        self.log_push_view.clear()
        self.push_worker = PushOldDataWorker(MYSQL_CONFIG)
        self.push_worker.log_signal.connect(self.log_push_view.append)
        self.push_worker.progress_signal.connect(self.update_progress)
        self.push_worker.finished_signal.connect(self.push_finished)
        self.push_worker.start()

    def update_progress(self, current, total):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)

    def push_finished(self, msg):
        self.btn_push_all_old.setEnabled(True)
        self.log_push_view.append(msg)
        self.update_initial_stats()
        QMessageBox.information(self, "Thành công", msg)

    def start_cf_verifier(self):
        if self.cf_verifier and self.cf_verifier.isRunning():
            return
        self.cf_verifier = CloudflareVerifierThread(url=BASE, interval=10, stay_seconds=3)
        self.cf_verifier.log_signal.connect(self.log_view.append)
        self.cf_verifier.start()
        self.btn_cf_start.setEnabled(False)
        self.btn_cf_stop.setEnabled(True)
        self.lbl_cf_status.setText("🟢 Verifier: Đang chạy (10s/lần)")
        self.lbl_cf_status.setStyleSheet("color: #27ae60; font-weight: bold; padding-left: 8px;")

    def stop_cf_verifier(self):
        if self.cf_verifier:
            self.cf_verifier.stop()
            self.cf_verifier = None
        self.btn_cf_start.setEnabled(True)
        self.btn_cf_stop.setEnabled(False)
        self.lbl_cf_status.setText("⚫ Verifier: Đang tắt")
        self.lbl_cf_status.setStyleSheet("color: #717d7e; font-weight: bold; padding-left: 8px;")
        self.log_view.append("🛡️ [Cloudflare Verifier] Đã dừng.")

    def _browser_navigate(self):
        if HAS_WEBENGINE and hasattr(self, 'web_view'):
            url = self.browser_url_input.text().strip()
            if not url.startswith("http"):
                url = "https://" + url
            self.web_view.load(QUrl(url))

    def append_log(self, text):
        self.log_view.append(text)
        print(text, flush=True)

    def start_all(self):
        self.btn_run_all.setEnabled(False)
        self.btn_stop_all.setEnabled(True)
        
        self.writer_thread = DatabaseWriterThread()
        self.writer_thread.log_signal.connect(self.append_log)
        self.writer_thread.count_signal.connect(self.update_live_count)
        self.writer_thread.start()

        self.worker_1 = Step1HunterWorker()
        self.worker_1.log_signal.connect(self.append_log)
        self.worker_1.start()

        self.worker_2 = Step2LiveCrawlerWorker(self.writer_thread)
        self.worker_2.log_signal.connect(self.append_log)
        self.worker_2.count_signal.connect(self.update_live_count)
        self.worker_2.start()
        
        self.append_log(">>> Hệ thống bắt đầu chạy trực tiếp trên MySQL (Chuẩn tốc độ cao Async DB & Cào lặp vô hạn).")

    def update_live_count(self, count):
        self.lbl_total_companies.setText(f"{count:,} doanh nghiệp")

    def stop_all(self):
        if self.worker_1: self.worker_1.stop()
        if self.worker_2: self.worker_2.stop()
        if self.writer_thread: self.writer_thread.stop()
        self.btn_run_all.setEnabled(True)
        self.btn_stop_all.setEnabled(False)
        self.update_initial_stats()

if __name__ == "__main__":
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')

    if "--auto-run" in sys.argv:
        from PyQt5.QtCore import QCoreApplication
        app = QCoreApplication(sys.argv)
        
        writer_thread = DatabaseWriterThread()
        writer_thread.log_signal.connect(lambda msg: print(msg, flush=True))
        writer_thread.start()

        worker_1 = Step1HunterWorker()
        worker_1.log_signal.connect(lambda msg: print(msg, flush=True))
        worker_1.start()

        worker_2 = Step2LiveCrawlerWorker(writer_thread)
        worker_2.log_signal.connect(lambda msg: print(msg, flush=True))
        worker_2.start()
        
        print(">>> Hệ thống bắt đầu chạy Auto-Run (No GUI) trên MySQL...", flush=True)
        
        QTimer.singleShot(17400000, lambda: sys.exit(0))
        sys.exit(app.exec_())
    else:
        app = QApplication(sys.argv)
        window = MainWindow()
        window.show()
        sys.exit(app.exec_())
