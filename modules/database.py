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
        "jenis_nasabah": "TEXT",
        "gsheet_id": "TEXT UNIQUE"
    }
    
    if "nama_nasabah" not in existing_trans_cols:
        cursor.execute("ALTER TABLE transaksi ADD COLUMN nama_nasabah TEXT")
    
    for col, col_type in required_trans_cols.items():
        if col not in existing_trans_cols:
            cursor.execute(f"ALTER TABLE transaksi ADD COLUMN {col} {col_type}")

    # Table for Penarikan (Advanced)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS penarikan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tanggal TIMESTAMP,
            nama_nasabah TEXT,
            nominal REAL,
            keterangan TEXT,
            metode TEXT DEFAULT 'Cash',
            petugas TEXT,
            unit TEXT,
            source TEXT DEFAULT 'Manual',
            gsheet_id TEXT UNIQUE
        );
    """)

    # Migration for existing penarikan table
    cursor.execute("PRAGMA table_info(penarikan)")
    existing_wd_cols = [col[1] for col in cursor.fetchall()]
    new_wd_cols = {
        "metode": "TEXT DEFAULT 'Cash'",
        "petugas": "TEXT",
        "unit": "TEXT"
    }
    for col, col_type in new_wd_cols.items():
        if col not in existing_wd_cols:
            cursor.execute(f"ALTER TABLE penarikan ADD COLUMN {col} {col_type}")

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
    cursor.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT, value TEXT, bsi_id INTEGER)")
    
    # Table for Users (Advanced Multi-Tenant)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE, 
            password TEXT, 
            full_name TEXT, 
            role TEXT DEFAULT 'staff',
            bsi_id INTEGER
        );
    """)

    # Migration Logic for Multi-Tenant (Add bsi_id to all tables)
    tables_to_migrate = {
        "transaksi": "INTEGER",
        "penarikan": "INTEGER",
        "nasabah": "INTEGER",
        "settings": "INTEGER",
        "master_sampah": "INTEGER",
        "users": "INTEGER"
    }
    
    for table, col_type in tables_to_migrate.items():
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cursor.fetchall()]
        if "bsi_id" not in cols:
            # Default bsi_id = 1 for existing data
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN bsi_id {col_type} DEFAULT 1")

    conn.commit()
    conn.close()

def save_transaction(data: dict, bsi_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO transaksi (
                tanggal, nama_nasabah, jenis_nasabah, rt_rw, jenis_sampah, berat_kg, 
                harga_per_kg, nilai_rp, pembayaran, status_alur, source, gsheet_id, bsi_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('tanggal'), data.get('nama_nasabah'), data.get('jenis_nasabah'), data.get('rt_rw'),
            data.get('jenis_sampah'), data.get('berat_kg'), data.get('harga_per_kg'),
            data.get('nilai_rp'), data.get('pembayaran'), data.get('status_alur'),
            data.get('source', 'Manual'), data.get('gsheet_id'), bsi_id
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False # Duplicate GSheet ID
    finally:
        conn.close()

def upsert_gsheet_data(df: pd.DataFrame, bsi_id: int):
    """Import normalized GSheet dataframe into local SQLite database for specific BSI."""
    conn = get_connection()
    success_count = 0
    duplicate_count = 0
    
    for _, row in df.iterrows():
        timestamp_str = row['tanggal'].strftime('%Y%m%d%H%M%S') if pd.notnull(row['tanggal']) else "0"
        gsheet_id = f"{timestamp_str}_{row['nama_nasabah']}_{row['berat_kg']}_{row['jenis_sampah']}"
        
        try:
            conn.execute("""
                INSERT OR REPLACE INTO transaksi (
                    tanggal, nama_nasabah, jenis_nasabah, rt_rw, jenis_sampah, berat_kg, 
                    harga_per_kg, nilai_rp, pembayaran, status_alur, source, gsheet_id, bsi_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['tanggal'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['tanggal']) else None,
                row['nama_nasabah'], row.get('jenis_nasabah', '-'), row.get('rt_rw', '-'),
                row['jenis_sampah'], row['berat_kg'], row['harga_per_kg'], row['nilai_rp'],
                row['pembayaran'], row['status_alur'], 'GSheet', gsheet_id, bsi_id
            ))
            success_count += 1
        except Exception:
            pass
            
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
    if user:
        return {
            "id": user[0],
            "username": user[1],
            "full_name": user[3],
            "role": user[4],
            "bsi_id": user[5]
        }
    return None

def register_bsi(username, password, full_name, bsi_name):
    """Registers a new BSI Organization with its first Admin user."""
    conn = get_connection()
    try:
        # Generate new BSI ID
        res = conn.execute("SELECT MAX(bsi_id) FROM users").fetchone()
        new_bsi_id = (res[0] or 0) + 1
        
        # Create Admin User
        conn.execute("""
            INSERT INTO users (username, password, full_name, role, bsi_id)
            VALUES (?, ?, ?, 'admin', ?)
        """, (username, password, full_name, new_bsi_id))
        
        # Create Default Settings for this BSI
        conn.execute("INSERT INTO settings (key, value, bsi_id) VALUES ('BSI_NAME', ?, ?)", (bsi_name, new_bsi_id))
        
        conn.commit()
        return True, "Registrasi Berhasil! Silakan Login."
    except sqlite3.IntegrityError:
        return False, "Username sudah digunakan."
    finally:
        conn.close()

def upsert_nasabah_data(df: pd.DataFrame, bsi_id: int):
    """Import normalized Member Registration dataframe for specific BSI."""
    conn = get_connection()
    success_count = 0
    update_count = 0
    
    for _, row in df.iterrows():
        try:
            # Check if exists for THIS BSI
            existing = conn.execute("SELECT id FROM nasabah WHERE nama = ? AND bsi_id = ?", (row['nama'], bsi_id)).fetchone()
            if existing:
                conn.execute("""
                    UPDATE nasabah SET 
                        email = ?, alamat = ?, no_hp = ?, unit = ?, 
                        jenis_nasabah = ?, status_aturan = ?
                    WHERE nama = ? AND bsi_id = ?
                """, (
                    row.get('email'), row.get('alamat'), row.get('no_hp'),
                    row.get('unit'), row.get('jenis_nasabah'), row.get('status_aturan'),
                    row['nama'], bsi_id
                ))
                update_count += 1
            else:
                conn.execute("""
                    INSERT INTO nasabah (
                        nama, email, alamat, no_hp, unit, jenis_nasabah, status_aturan, bsi_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['nama'], row.get('email'), row.get('alamat'), row.get('no_hp'),
                    row.get('unit'), row.get('jenis_nasabah'), row.get('status_aturan'), bsi_id
                ))
                success_count += 1
        except Exception:
            pass
            
    conn.commit()
    conn.close()
    return success_count, update_count

def get_nasabah_df(bsi_id: int):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM nasabah WHERE bsi_id = ? ORDER BY nama ASC", conn, params=(bsi_id,))
    conn.close()
    return df

def get_setting(key, bsi_id: int, default=None):
    conn = get_connection()
    row = conn.execute("SELECT value FROM settings WHERE key = ? AND bsi_id = ?", (key, bsi_id)).fetchone()
    conn.close()
    return row[0] if row else default

def update_setting(key, value, bsi_id: int):
    conn = get_connection()
    # Insert or replace for specific BSI
    existing = conn.execute("SELECT bsi_id FROM settings WHERE key = ? AND bsi_id = ?", (key, bsi_id)).fetchone()
    if existing:
        conn.execute("UPDATE settings SET value = ? WHERE key = ? AND bsi_id = ?", (value, key, bsi_id))
    else:
        conn.execute("INSERT INTO settings (key, value, bsi_id) VALUES (?, ?, ?)", (key, value, bsi_id))
    conn.commit()
    conn.close()

def get_transactions_df(bsi_id: int):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM transaksi WHERE bsi_id = ? ORDER BY tanggal DESC", conn, params=(bsi_id,))
    conn.close()
    return df

def get_nasabah_summary(bsi_id: int):
    conn = get_connection()
    query = """
        SELECT 
            n.nama as nama_nasabah, 
            COUNT(t.id) as total_transaksi,
            IFNULL(SUM(t.berat_kg), 0) as total_berat_kg,
            IFNULL(SUM(t.nilai_rp), 0) as total_setoran,
            IFNULL(p.total_penarikan, 0) as total_penarikan,
            (IFNULL(SUM(t.nilai_rp), 0) - IFNULL(p.total_penarikan, 0)) as saldo
        FROM nasabah n
        LEFT JOIN transaksi t ON n.nama = t.nama_nasabah AND t.bsi_id = n.bsi_id
        LEFT JOIN (
            SELECT nama_nasabah, SUM(nominal) as total_penarikan
            FROM penarikan
            WHERE bsi_id = ?
            GROUP BY nama_nasabah
        ) p ON n.nama = p.nama_nasabah
        WHERE n.bsi_id = ?
        GROUP BY n.nama
        ORDER BY saldo DESC
    """
    df = pd.read_sql_query(query, conn, params=(bsi_id, bsi_id))
    conn.close()
    return df

# --- New Helper Functions for Hybrid System ---

def get_master_sampah(bsi_id: int):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM master_sampah WHERE bsi_id = ? ORDER BY nama_jenis ASC", conn, params=(bsi_id,))
    conn.close()
    return df

def update_master_sampah(nama, harga, bsi_id: int):
    conn = get_connection()
    existing = conn.execute("SELECT id FROM master_sampah WHERE nama_jenis = ? AND bsi_id = ?", (nama, bsi_id)).fetchone()
    if existing:
        conn.execute("UPDATE master_sampah SET harga_per_kg = ? WHERE nama_jenis = ? AND bsi_id = ?", (harga, nama, bsi_id))
    else:
        conn.execute("INSERT INTO master_sampah (nama_jenis, harga_per_kg, bsi_id) VALUES (?, ?, ?)", (nama, harga, bsi_id))
    conn.commit()
    conn.close()

def delete_master_sampah(id, bsi_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM master_sampah WHERE id = ? AND bsi_id = ?", (id, bsi_id))
    conn.commit()
    conn.close()

def save_penarikan(data: dict, bsi_id: int):
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO penarikan (tanggal, nama_nasabah, nominal, keterangan, metode, petugas, unit, source, bsi_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['tanggal'], data['nama_nasabah'], data['nominal'], 
            data.get('keterangan', '-'), data.get('metode', 'Cash'),
            data.get('petugas', '-'), data.get('unit', '-'), 'Manual', bsi_id
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error save_penarikan: {e}")
        return False
    finally:
        conn.close()

def get_withdrawals_df(bsi_id: int):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM penarikan WHERE bsi_id = ? ORDER BY tanggal DESC", conn, params=(bsi_id,))
    conn.close()
    return df

def upsert_withdrawal_data(df: pd.DataFrame, bsi_id: int):
    """Import normalized Withdrawal dataframe from GSheet for specific BSI."""
    conn = get_connection()
    success_count = 0
    duplicate_count = 0
    for _, row in df.iterrows():
        timestamp_str = row['tanggal'].strftime('%Y%m%d%H%M%S') if pd.notnull(row['tanggal']) else "0"
        gsheet_id = f"WD_{timestamp_str}_{row['nama_nasabah']}_{row['nominal']}"
        try:
            conn.execute("""
                INSERT INTO penarikan (tanggal, nama_nasabah, nominal, keterangan, metode, petugas, unit, source, gsheet_id, bsi_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['tanggal'].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['tanggal']) else None,
                row['nama_nasabah'], row['nominal'], row.get('keterangan', '-'),
                row.get('metode', 'Cash'), row.get('petugas', '-'), row.get('unit', '-'),
                'GSheet', gsheet_id, bsi_id
            ))
            success_count += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return success_count, duplicate_count

def get_waste_stats_by_type(bsi_id: int):
    """Aggregates total weight per waste type for specific BSI."""
    conn = get_connection()
    query = """
        SELECT jenis_sampah, SUM(berat_kg) as total_berat
        FROM transaksi
        WHERE bsi_id = ? AND jenis_sampah IS NOT NULL AND jenis_sampah != ''
        GROUP BY jenis_sampah
        ORDER BY total_berat DESC
    """
    df = pd.read_sql_query(query, conn, params=(bsi_id,))
    conn.close()
    return df

def get_bsu_summary(bsi_id: int):
    """Aggregates performance data per Bank Sampah Unit (BSU) for specific BSI."""
    conn = get_connection()
    query = """
        SELECT 
            n.unit as bsu,
            COUNT(DISTINCT n.nama) as jml_nasabah,
            IFNULL(SUM(t.berat_kg), 0) as total_berat,
            IFNULL(SUM(t.nilai_rp), 0) as total_rp
        FROM nasabah n
        LEFT JOIN transaksi t ON n.nama = t.nama_nasabah AND t.bsi_id = n.bsi_id
        WHERE n.bsi_id = ? AND n.unit IS NOT NULL AND n.unit != ''
        GROUP BY n.unit
        ORDER BY total_rp DESC
    """
    df = pd.read_sql_query(query, conn, params=(bsi_id,))
    conn.close()
    return df

def clear_all_data(bsi_id: int):
    """Wipe all transaction and withdrawal records for specific BSI."""
    conn = get_connection()
    conn.execute("DELETE FROM transaksi WHERE bsi_id = ?", (bsi_id,))
    conn.execute("DELETE FROM penarikan WHERE bsi_id = ?", (bsi_id,))
    conn.commit()
    conn.close()
    return True
