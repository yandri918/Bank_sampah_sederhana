import sqlite3
import pandas as pd
from datetime import datetime
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "bank_sampah.db")

def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Tables for Waste Bank Ecosystem
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password TEXT,
            full_name TEXT,
            role TEXT DEFAULT 'staff'
        );

        CREATE TABLE IF NOT EXISTS nasabah (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nama TEXT UNIQUE,
            rt_rw TEXT,
            total_poin REAL DEFAULT 0,
            last_transaction TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS transaksi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tanggal TIMESTAMP,
            nasabah_name TEXT,
            rt_rw TEXT,
            jenis_sampah TEXT,
            berat_kg REAL,
            harga_per_kg REAL,
            nilai_rp REAL,
            pembayaran REAL,
            status_alur TEXT DEFAULT 'Selesai',
            source TEXT DEFAULT 'GSheet',
            gsheet_id TEXT UNIQUE -- To prevent duplicates from GSheet
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    conn.close()

def save_transaction(data: dict):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO transaksi (
                tanggal, nasabah_name, rt_rw, jenis_sampah, berat_kg, 
                harga_per_kg, nilai_rp, pembayaran, status_alur, source, gsheet_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('tanggal'), data.get('nama_nasabah'), data.get('rt_rw'),
            data.get('jenis_sampah'), data.get('berat_kg'), data.get('harga_per_kg'),
            data.get('nilai_rp'), data.get('pembayaran'), data.get('status_alur'),
            data.get('source', 'Manual'), data.get('gsheet_id')
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False # Duplicate GSheet ID
    finally:
        conn.close()

def upsert_gsheet_data(df: pd.DataFrame):
    """Import normalized GSheet dataframe into local SQLite database."""
    conn = get_connection()
    success_count = 0
    duplicate_count = 0
    
    for _, row in df.iterrows():
        # Create a unique ID for GSheet rows to avoid re-syncing same entry
        # Using more fields to ensure uniqueness
        timestamp_str = row['tanggal'].strftime('%Y%m%d%H%M%S') if pd.notnull(row['tanggal']) else "0"
        gsheet_id = f"{timestamp_str}_{row['nama_nasabah']}_{row['berat_kg']}_{row['jenis_sampah']}"
        
        try:
            conn.execute("""
                INSERT INTO transaksi (
                    tanggal, nasabah_name, rt_rw, jenis_sampah, berat_kg, 
                    harga_per_kg, nilai_rp, pembayaran, status_alur, source, gsheet_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['tanggal'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['tanggal']) else None,
                row['nama_nasabah'],
                row.get('rt_rw', '-'),
                row['jenis_sampah'],
                row['berat_kg'],
                row['harga_per_kg'],
                row['nilai_rp'],
                row['pembayaran'],
                row['status_alur'],
                'GSheet',
                gsheet_id
            ))
            success_count += 1
        except sqlite3.IntegrityError:
            duplicate_count += 1
            
    conn.commit()
    conn.close()
    return success_count, duplicate_count

def create_user(username, password, full_name, role='staff'):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO users (username, password, full_name, role) VALUES (?, ?, ?, ?)",
                    (username, password, full_name, role))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def verify_user(username, password):
    conn = get_connection()
    user = conn.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password)).fetchone()
    conn.close()
    return user

def clear_all_data():
    conn = get_connection()
    conn.execute("DELETE FROM transaksi")
    conn.execute("DELETE FROM nasabah")
    conn.commit()
    conn.close()

def get_setting(key, default=None):
    conn = get_connection()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default

def update_setting(key, value):
    conn = get_connection()
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()

def get_transactions_df():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM transaksi ORDER BY tanggal DESC", conn)
    conn.close()
    return df

def get_nasabah_summary():
    conn = get_connection()
    query = """
        SELECT 
            nasabah_name as nama_nasabah, 
            COUNT(*) as total_transaksi,
            SUM(berat_kg) as total_berat_kg,
            SUM(nilai_rp) as total_nilai_rp
        FROM transaksi 
        GROUP BY nasabah_name
        ORDER BY total_nilai_rp DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
