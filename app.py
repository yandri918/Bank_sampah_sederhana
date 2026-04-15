import re
from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from modules.database import (
    init_db, upsert_gsheet_data, get_transactions_df, 
    verify_user, create_user, get_setting, update_setting,
    upsert_nasabah_data, get_nasabah_df
)

# Initialize Database
init_db()

st.set_page_config(
    page_title="Bank Sampah Streamlit",
    page_icon="♻️",
    layout="wide",
)

EXPECTED_FIELDS: Dict[str, Iterable[str]] = {
    "tanggal": ("timestamp", "tanggal", "date"),
    "nama_nasabah": ("nama nasabah", "nasabah", "nama", "name"),
    "rt_rw": ("rt/rw", "rt rw", "rt", "rw"),
    "jenis_sampah": ("jenis sampah", "kategori sampah", "sampah"),
    "berat_kg": ("berat", "kg", "berat (kg)", "berat_kg"),
    "harga_per_kg": ("harga", "harga/kg", "harga per kg", "rate"),
    "nilai_rp": ("nilai", "total", "rupiah", "rp", "subtotal"),
    "status_alur": ("status", "alur", "proses", "tahap"),
    "pembayaran": ("pembayaran", "dibayar", "cash out", "transfer"),
}

EXPECTED_FIELDS_REGISTRATION: Dict[str, Iterable[str]] = {
    "nama": ("nama", "name"),
    "email": ("email",),
    "alamat": ("alamat lengkap", "alamat", "address"),
    "no_hp": ("no hp", "no wa", "whatsapp", "phone"),
    "unit": ("bank sampah unit", "unit"),
    "jenis_nasabah": ("jenis nasabah", "kategori nasabah"),
    "status_aturan": ("bersedia mengikuti aturan", "aturan"),
}


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text).strip().lower()).strip()


def _find_col(columns: Iterable[str], aliases: Iterable[str]) -> Optional[str]:
    normalized = {_slugify(col): col for col in columns}
    for alias in aliases:
        alias_key = _slugify(alias)
        for key, original in normalized.items():
            if alias_key in key:
                return original
    return None


def _build_sheet_csv_url(sheet_url: str) -> str:
    # Extract Spreadsheet ID
    id_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not id_match:
        return ""
    spreadsheet_id = id_match.group(1)
    
    # Extract optional Resource Key
    rk_match = re.search(r"resourcekey=([a-zA-Z0-9-_]+)", sheet_url)
    resource_key = rk_match.group(1) if rk_match else ""
    
    # Build export URL
    base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv"
    if resource_key:
        base_url += f"&resourcekey={resource_key}"
    return base_url


def _normalize_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    mapped_cols = {}

    for field_name, aliases in EXPECTED_FIELDS.items():
        found = _find_col(df.columns, aliases)
        if found:
            mapped_cols[field_name] = found

    if "tanggal" in mapped_cols:
        df["tanggal"] = pd.to_datetime(df[mapped_cols["tanggal"]], errors="coerce")
    else:
        df["tanggal"] = pd.NaT

    if "nama_nasabah" in mapped_cols:
        df["nama_nasabah"] = df[mapped_cols["nama_nasabah"]].astype(str)
    else:
        df["nama_nasabah"] = "Tidak Diketahui"

    if "jenis_sampah" in mapped_cols:
        df["jenis_sampah"] = df[mapped_cols["jenis_sampah"]].astype(str)
    else:
        df["jenis_sampah"] = "Lainnya"

    if "status_alur" in mapped_cols:
        df["status_alur"] = df[mapped_cols["status_alur"]].fillna("Belum Diproses").astype(str)
    else:
        df["status_alur"] = "Belum Diproses"

    for num_field in ("berat_kg", "harga_per_kg", "nilai_rp", "pembayaran"):
        if num_field in mapped_cols:
            parsed = (
                df[mapped_cols[num_field]]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^0-9.\-]", "", regex=True)
            )
            df[num_field] = pd.to_numeric(parsed, errors="coerce").fillna(0.0)
        else:
            df[num_field] = 0.0

    if (df["nilai_rp"] <= 0).all() and (df["berat_kg"] > 0).any() and (df["harga_per_kg"] > 0).any():
        df["nilai_rp"] = df["berat_kg"] * df["harga_per_kg"]

    return df


def _normalize_nasabah_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    mapped_cols = {}

    for field_name, aliases in EXPECTED_FIELDS_REGISTRATION.items():
        found = _find_col(df.columns, aliases)
        if found:
            mapped_cols[field_name] = found

    for field_name in EXPECTED_FIELDS_REGISTRATION.keys():
        if field_name in mapped_cols:
            df[field_name] = df[mapped_cols[field_name]].astype(str)
        else:
            df[field_name] = "-"
            
    return df


@st.cache_data(ttl=120)
def _load_gsheet_csv(csv_url: str) -> pd.DataFrame:
    return pd.read_csv(csv_url)


st.title("Bank Sampah Dashboard")
st.caption("Integrasi Google Form ke visualisasi nasabah, alur sampah, pembukuan, dan keuangan.")

# --- Authentication & Sidebar ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

with st.sidebar:
    st.header("🔑 Akses Sistem")
    if not st.session_state.authenticated:
        auth_tab1, auth_tab2 = st.tabs(["Masuk", "Daftar"])
        
        with auth_tab1:
            with st.form("login_form"):
                user = st.text_input("Username")
                pw = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Masuk", use_container_width=True)
                if submitted:
                    res = verify_user(user, pw)
                    if res:
                        st.session_state.authenticated = True
                        st.session_state.user = res
                        st.rerun()
                    elif user == "admin" and pw == "admin":
                        create_user("admin", "admin", "Administrator", "admin")
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error("Username atau password salah")
        
        with auth_tab2:
            with st.form("register_form"):
                new_user = st.text_input("Username Baru")
                new_pw = st.text_input("Password Baru", type="password")
                full_name = st.text_input("Nama Lengkap")
                reg_submitted = st.form_submit_button("Daftar Sekarang", use_container_width=True)
                if reg_submitted:
                    if new_user and new_pw and full_name:
                        if create_user(new_user, new_pw, full_name):
                            st.success("Akun berhasil dibuat! Silakan masuk.")
                        else:
                            st.error("Username mungkin sudah digunakan.")
                    else:
                        st.warning("Mohon isi semua field.")
    else:
        st.success(f"Halo, {st.session_state.user[3] if len(st.session_state.user) > 3 else 'User'}")
        if st.button("Keluar", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()

    st.divider()
    st.header("⚙️ Konfigurasi")
    st.link_button(
        "Buka Google Form",
        "https://docs.google.com/forms/d/e/1FAIpQLSdXSuFX_RaEspHEZ7HLsdQ4cGHYJUO4IUQrE8qk1DexHJ9-HA/viewform",
        use_container_width=True,
    )
    
    # Load URL from DB or Secrets as fallback
    # We use the URL you provided as the ultimate fallback
    provided_url = "https://docs.google.com/spreadsheets/d/1wng2B3hZ3Y1TeAeLNHhYVQJXGG4yo3vjdsfrx8VIY6Q/edit#gid=0"
    db_url = get_setting("BANK_SAMPAH_SHEET_URL")
    reg_url = get_setting("BANK_SAMPAH_REGISTRATION_URL", "")
    
    if not db_url or "sheet_id" in db_url:
        default_url = st.secrets.get("BANK_SAMPAH_SHEET_URL", provided_url)
    else:
        default_url = db_url
    
    sheet_url = st.text_input(
        "URL Google Sheet Transaksi",
        value=default_url,
        placeholder="https://docs.google.com/spreadsheets/d/<sheet_id>/edit#gid=0",
    )
    
    reg_sheet_url = st.text_input(
        "URL Google Sheet Pendaftaran",
        value=reg_url,
        placeholder="https://docs.google.com/spreadsheets/d/<sheet_id>/edit#gid=0",
    )
    
    if st.session_state.authenticated:
        st.info("💡 Klik tombol di bawah untuk sinkronisasi.")
        col_sync1, col_sync2 = st.columns(2)
        with col_sync1:
            if st.button("🚀 SYNC TRANSAKSI", use_container_width=True, type="primary"):
                if sheet_url:
                    with st.spinner("Sinkron Transaksi..."):
                        try:
                            csv_url_sync = _build_sheet_csv_url(sheet_url)
                            if not csv_url_sync:
                                st.sidebar.error("Link Sheet Transaksi tidak valid.")
                            else:
                                raw_data = _load_gsheet_csv(csv_url_sync)
                                norm_df = _normalize_dataframe(raw_data)
                                added, dups = upsert_gsheet_data(norm_df)
                                st.sidebar.success(f"Transaksi: +{added} baru")
                                st.cache_data.clear()
                                st.rerun()
                        except Exception as e:
                            st.sidebar.error(f"Gagal: {e}")
        
        with col_sync2:
            if st.button("👥 SYNC NASABAH", use_container_width=True):
                if reg_sheet_url:
                    with st.spinner("Sinkron Nasabah..."):
                        try:
                            csv_url_reg = _build_sheet_csv_url(reg_sheet_url)
                            if not csv_url_reg:
                                st.sidebar.error("Link Sheet Pendaftaran tidak valid.")
                            else:
                                raw_reg = _load_gsheet_csv(csv_url_reg)
                                norm_reg = _normalize_nasabah_dataframe(raw_reg)
                                added, updated = upsert_nasabah_data(norm_reg)
                                st.sidebar.success(f"Nasabah: +{added} baru, {updated} update")
                                st.rerun()
                        except Exception as e:
                            st.sidebar.error(f"Gagal: {e}")
            else:
                st.sidebar.warning("Masukkan URL GSheet dulu.")

# --- Data Loading ---
df_db = get_transactions_df()
if not df_db.empty:
    # Deduplicate columns before any processing to avoid reindex errors
    df_db = df_db.loc[:, ~df_db.columns.duplicated()]
    
    # Fail-safe: Ensure all required columns exist even if DB schema hasn't migrated yet
    required_cols = ["tanggal", "nama_nasabah", "berat_kg", "nilai_rp", "pembayaran", "status_alur", "jenis_sampah"]
    df_db = df_db.reindex(columns=required_cols)
    
    # Fill numeric NaNs with 0 and strings with empty
    for col in ["berat_kg", "nilai_rp", "pembayaran"]:
        df_db[col] = pd.to_numeric(df_db[col], errors='coerce').fillna(0.0)
    
    df_db['tanggal'] = pd.to_datetime(df_db['tanggal'])
    
    # Filter by Date
    min_date = df_db['tanggal'].min().date()
    max_date = df_db['tanggal'].max().date()
    
    with st.sidebar:
        st.divider()
        st.header("📅 Filter Data")
        # Ensure we have a valid list for date_input
        date_range = st.date_input(
            "Rentang Tanggal",
            value=[min_date, max_date],
            min_value=min_date,
            max_value=max_date
        )
        
        if isinstance(date_range, list) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date
    
    # Apply filter
    df = df_db[(df_db['tanggal'].dt.date >= start_date) & (df_db['tanggal'].dt.date <= end_date)].copy()
else:
    df = pd.DataFrame()
    st.warning("👋 Selamat Datang! Data masih kosong.")
    st.info("Silakan login di sidebar (atau gunakan akun admin) lalu klik tombol **🚀 MULAI SINKRONISASI** untuk menarik data dari Google Sheet.")

if df.empty:
    st.info("Tidak ada data untuk rentang waktu ini atau database masih kosong.")
    if not st.session_state.authenticated:
        st.write("Silakan Login & Sinkronisasi di sidebar.")
    st.stop()

# --- Calculations & Metrics ---
total_nasabah = int(df["nama_nasabah"].nunique())
total_berat = float(df["berat_kg"].sum())
total_pendapatan = float(df["nilai_rp"].sum())
total_pengeluaran = float(df["pembayaran"].sum())
saldo = total_pendapatan - total_pengeluaran

# Simple Trend (MoM)
current_month = datetime.now().strftime('%Y-%m')
prev_month = (datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m')

def get_monthly_sum(dataframe, month_str, col):
    return dataframe[dataframe['tanggal'].dt.strftime('%Y-%m') == month_str][col].sum()

berat_delta = get_monthly_sum(df, current_month, 'berat_kg') - get_monthly_sum(df, prev_month, 'berat_kg')
pendapatan_delta = get_monthly_sum(df, current_month, 'nilai_rp') - get_monthly_sum(df, prev_month, 'nilai_rp')

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Nasabah", f"{total_nasabah}")
c2.metric("Total Berat", f"{total_berat:,.1f} kg", delta=f"{berat_delta:,.1f} kg (Bulan ini)")
c3.metric("Pendapatan", f"Rp {total_pendapatan:,.0f}", delta=f"Rp {pendapatan_delta:,.0f} (Bulan ini)")
c4.metric("Saldo", f"Rp {saldo:,.0f}")

tab_nasabah, tab_alur, tab_pembukuan, tab_keuangan, tab_raw, tab_settings = st.tabs(
    ["Database Nasabah", "Alur Sampah", "Pembukuan", "Keuangan", "Data Mentah", "⚙️ Pengaturan"]
)

with tab_nasabah:
    # Merge transaction summary with detailed profile
    nasabah_list_df = get_nasabah_df()
    trans_summary = (
        df.groupby("nama_nasabah", as_index=False)
        .agg(
            total_transaksi=("nama_nasabah", "count"),
            total_berat_kg=("berat_kg", "sum"),
            total_nilai_rp=("nilai_rp", "sum"),
        )
    )
    
    merged_nasabah = pd.merge(
        nasabah_list_df, trans_summary, 
        left_on="nama", right_on="nama_nasabah", 
        how="left"
    ).fillna(0)
    
    st.subheader("Database Anggota Lengkap")
    # Using reindex for safety - it will create columns with NaN if they don't exist yet
    display_cols = [
        "nama", "unit", "jenis_nasabah", "total_transaksi", 
        "total_berat_kg", "total_nilai_rp", "email", "no_hp", "alamat"
    ]
    st.dataframe(
        merged_nasabah.reindex(columns=display_cols), 
        use_container_width=True, 
        hide_index=True
    )
    
    st.plotly_chart(
        px.bar(merged_nasabah.sort_values("total_nilai_rp", ascending=False).head(10), 
               x="nama", y="total_nilai_rp", color="unit",
               title="Top 10 Nasabah Berdasarkan Nilai Setoran"),
        use_container_width=True,
    )

with tab_alur:
    flow_df = df.groupby(["status_alur", "jenis_sampah"], as_index=False)["berat_kg"].sum()
    st.dataframe(flow_df, use_container_width=True, hide_index=True)
    st.plotly_chart(
        px.sunburst(flow_df, path=["status_alur", "jenis_sampah"], values="berat_kg", title="Distribusi Alur Sampah"),
        use_container_width=True,
    )

with tab_pembukuan:
    book_df = df.copy()
    book_df["bulan"] = book_df["tanggal"].dt.to_period("M").astype(str)
    monthly_book = (
        book_df.groupby("bulan", as_index=False)
        .agg(
            jumlah_transaksi=("nama_nasabah", "count"),
            total_berat_kg=("berat_kg", "sum"),
            total_nilai_rp=("nilai_rp", "sum"),
            total_pembayaran=("pembayaran", "sum"),
        )
        .sort_values("bulan")
    )
    st.dataframe(monthly_book, use_container_width=True, hide_index=True)
    st.plotly_chart(
        px.line(
            monthly_book,
            x="bulan",
            y=["total_nilai_rp", "total_pembayaran"],
            markers=True,
            title="Tren Pembukuan Bulanan",
        ),
        use_container_width=True,
    )

with tab_keuangan:
    finance_df = pd.DataFrame(
        {
            "Komponen": ["Pendapatan", "Pengeluaran", "Saldo"],
            "Nilai": [total_pendapatan, total_pengeluaran, saldo],
        }
    )
    st.dataframe(finance_df, use_container_width=True, hide_index=True)
    st.plotly_chart(
        px.bar(finance_df, x="Komponen", y="Nilai", color="Komponen", title="Ringkasan Keuangan"),
        use_container_width=True,
    )

with tab_raw:
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button(
        "Unduh CSV Data",
        df.to_csv(index=False).encode("utf-8"),
        file_name="bank_sampah_data.csv",
        mime="text/csv",
    )

with tab_settings:
    st.header("Konfigurasi Sistem")
    if st.session_state.authenticated:
        st.subheader("Google Sheets Link")
        new_url = st.text_input("URL Google Sheet Transaksi", value=sheet_url)
        new_reg_url = st.text_input("URL Google Sheet Pendaftaran Nasabah", value=reg_sheet_url)
        
        if st.button("Simpan Pengaturan"):
            update_setting("BANK_SAMPAH_SHEET_URL", new_url)
            update_setting("BANK_SAMPAH_REGISTRATION_URL", new_reg_url)
            st.success("Konfigurasi berhasil disimpan ke database!")
            st.rerun()
        
        st.divider()
        st.subheader("Manajemen Data")
        if st.button("⚠️ Bersihkan Semua Data Transaksi", type="secondary"):
            from modules.database import clear_all_data
            clear_all_data()
            st.warning("Semua data telah dihapus.")
            st.rerun()
    else:
        st.info("Silakan login untuk mengakses pengaturan sistem.")
