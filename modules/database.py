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
    
    # Table for Transaksi
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaksi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tanggal TIMESTAMP,
            nama_nasabah TEXT
        );
    """)

    # Auto-Migration for transaksi: Add missing columns
    cursor.execute("PRAGMA table_info(transaksi)")
    existing_trans_cols = [row[1] for row in cursor.fetchall()]
    
    required_trans_cols = {
        "rt_rw": "TEXT",
        "jenis_sampah": "TEXT",
        "berat_kg": "REAL",
        "harga_per_kg": "REAL",
        "nilai_rp": "REAL",
        "pembayaran": "REAL",
        "status_alur": "TEXT DEFAULT 'Selesai'",
        "source": "TEXT DEFAULT 'GSheet'",
        "gsheet_id": "TEXT UNIQUE"
    }
    
    if "nama_nasabah" not in existing_trans_cols:
        cursor.execute("ALTER TABLE transaksi ADD COLUMN nama_nasabah TEXT")
    
    for col, col_type in required_trans_cols.items():
        if col not in existing_trans_cols:
            cursor.execute(f"ALTER TABLE transaksi ADD COLUMN {col} {col_type}")

    # Table for Penarikan (New)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS penarikan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tanggal TIMESTAMP,
            nama_nasabah TEXT,
            nominal REAL,
            keterangan TEXT,
            source TEXT DEFAULT 'Manual',
            gsheet_id TEXT UNIQUE
        );
    """)

    # Table for Master Sampah (New)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS master_sampah (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nama_jenis TEXT UNIQUE,
            harga_per_kg REAL
        );
    """)

    # Seed Master Sampah if empty
    cursor.execute("SELECT COUNT(*) FROM master_sampah")
    if cursor.fetchone()[0] == 0:
        initial_data = [
            ("Kardus", 2000),
            ("Plastik", 1500),
            ("Logam/Besi", 4000),
            ("Kertas/HVS", 1000),
            ("Botol Kaca", 500)
        ]
        cursor.executemany("INSERT INTO master_sampah (nama_jenis, harga_per_kg) VALUES (?, ?)", initial_data)

    # Table for Nasabah
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nasabah (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nama TEXT UNIQUE
        );
    """)

    # Auto-Migration for Nasabah
    cursor.execute("PRAGMA table_info(nasabah)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    required_cols = {
        "email": "TEXT", "alamat": "TEXT", "no_hp": "TEXT", "unit": "TEXT",
        "jenis_nasabah": "TEXT", "status_aturan": "TEXT",
        "total_poin": "REAL DEFAULT 0", "last_transaction": "TIMESTAMP"
    }
    for col, col_type in required_cols.items():
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE nasabah ADD COLUMN {col} {col_type}")

    # Table for Settings & Users
    cursor.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE, password TEXT, full_name TEXT, role TEXT DEFAULT 'staff'
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
                tanggal, nama_nasabah, rt_rw, jenis_sampah, berat_kg, 
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
                    tanggal, nama_nasabah, rt_rw, jenis_sampah, berat_kg, 
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

def upsert_nasabah_data(df: pd.DataFrame):
    """Import normalized Member Registration dataframe."""
    conn = get_connection()
    success_count = 0
    update_count = 0
    
    for _, row in df.iterrows():
        try:
            # Check if exists
            existing = conn.execute("SELECT id FROM nasabah WHERE nama = ?", (row['nama'],)).fetchone()
            if existing:
                conn.execute("""
                    UPDATE nasabah SET 
                        email = ?, alamat = ?, no_hp = ?, unit = ?, 
                        jenis_nasabah = ?, status_aturan = ?
                    WHERE nama = ?
                """, (
                    row.get('email'), row.get('alamat'), row.get('no_hp'),
                    row.get('unit'), row.get('jenis_nasabah'), row.get('status_aturan'),
                    row['nama']
                ))
                update_count += 1
            else:
                conn.execute("""
                    INSERT INTO nasabah (
                        nama, email, alamat, no_hp, unit, jenis_nasabah, status_aturan
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['nama'], row.get('email'), row.get('alamat'), row.get('no_hp'),
                    row.get('unit'), row.get('jenis_nasabah'), row.get('status_aturan')
                ))
                success_count += 1
        except Exception:
            pass
            
    conn.commit()
    conn.close()
    return success_count, update_count

def get_nasabah_df():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM nasabah ORDER BY nama ASC", conn)
    conn.close()
    return df

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
    # Ensure column is named nama_nasabah for streamlit dashboard compatibility
    df = pd.read_sql_query("SELECT * FROM transaksi ORDER BY tanggal DESC", conn)
    
    # Handle duplicate/old columns gracefully
    if 'nasabah_name' in df.columns:
        if 'nama_nasabah' not in df.columns:
            df = df.rename(columns={'nasabah_name': 'nama_nasabah'})
        else:
            # If both exist, drop the old one to avoid duplicates
            df = df.drop(columns=['nasabah_name'])
            
    # Final safety: remove any other potential duplicate column names
    df = df.loc[:, ~df.columns.duplicated()]
    conn.close()
    return df

def get_nasabah_summary():
    conn = get_connection()
    # Updated summary to include actual saldo calculation
    # Balance = SUM(Setoran) - SUM(Penarikan)
    
    query = """
        SELECT 
            n.nama as nama_nasabah, 
            COUNT(t.id) as total_transaksi,
            IFNULL(SUM(t.berat_kg), 0) as total_berat_kg,
            IFNULL(SUM(t.nilai_rp), 0) as total_setoran,
            IFNULL(p.total_penarikan, 0) as total_penarikan,
            (IFNULL(SUM(t.nilai_rp), 0) - IFNULL(p.total_penarikan, 0)) as saldo
        FROM nasabah n
        LEFT JOIN transaksi t ON n.nama = t.nama_nasabah
        LEFT JOIN (
            SELECT nama_nasabah, SUM(nominal) as total_penarikan
            FROM penarikan
            GROUP BY nama_nasabah
        ) p ON n.nama = p.nama_nasabah
        GROUP BY n.nama
        ORDER BY saldo DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- New Helper Functions for Hybrid System ---

def get_master_sampah():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM master_sampah ORDER BY nama_jenis ASC", conn)
    conn.close()
    return df

def update_master_sampah(nama, harga):
    conn = get_connection()
    conn.execute("INSERT OR REPLACE INTO master_sampah (nama_jenis, harga_per_kg) VALUES (?, ?)", (nama, harga))
    conn.commit()
    conn.close()

def delete_master_sampah(id):
    conn = get_connection()
    conn.execute("DELETE FROM master_sampah WHERE id = ?", (id,))
    conn.commit()
    conn.close()

def save_penarikan(data: dict):
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO penarikan (tanggal, nama_nasabah, nominal, keterangan, source)
            VALUES (?, ?, ?, ?, ?)
        """, (data['tanggal'], data['nama_nasabah'], data['nominal'], data['keterangan'], 'Manual'))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def get_withdrawals_df():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM penarikan ORDER BY tanggal DESC", conn)
    conn.close()
    return df

def upsert_withdrawal_data(df: pd.DataFrame):
    """Import normalized Withdrawal dataframe from GSheet."""
    conn = get_connection()
    success_count = 0
    duplicate_count = 0
    for _, row in df.iterrows():
        timestamp_str = row['tanggal'].strftime('%Y%m%d%H%M%S') if pd.notnull(row['tanggal']) else "0"
        gsheet_id = f"WD_{timestamp_str}_{row['nama_nasabah']}_{row['nominal']}"
        try:
            conn.execute("""
                INSERT INTO penarikan (tanggal, nama_nasabah, nominal, keterangan, source, gsheet_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['tanggal'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['tanggal']) else None,
                row['nama_nasabah'], row['nominal'], row.get('keterangan', '-'), 'GSheet', gsheet_id
            ))
            success_count += 1
        except sqlite3.IntegrityError:
            duplicate_count += 1
    conn.commit()
    conn.close()
    return success_count, duplicate_count
