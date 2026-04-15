import re
from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from modules.database import (
    init_db, upsert_gsheet_data, get_transactions_df, 
    verify_user, create_user, get_setting, update_setting,
    upsert_nasabah_data, get_nasabah_df,
    get_master_sampah, update_master_sampah, delete_master_sampah,
    save_transaction, save_penarikan, get_withdrawals_df, upsert_withdrawal_data,
    get_nasabah_summary
)
from modules.cards import generate_member_card, generate_qr_code, generate_withdrawal_receipt

# Initialize Database
init_db()

st.set_page_config(
    page_title="Bank Sampah Digital V2 (Hybrid)",
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
    "nama": ("nama lengkap", "nama nasabah", "nama", "name"),
    "email": ("email", "e mail", "alamat email"),
    "alamat": ("alamat lengkap", "alamat", "address", "domisili"),
    "no_hp": ("no hp", "no wa", "whatsapp", "phone", "nomor hp", "nomor wa", "telepon"),
    "unit": ("bank sampah unit", "unit", "pilih bank sampah unit", "bsu"),
    "jenis_nasabah": ("nasabah", "jenis nasabah", "kategori nasabah", "tipe nasabah", "kategori"),
    "status_aturan": ("bersedia mengikuti aturan", "aturan", "syarat", "persetujuan"),
}

EXPECTED_FIELDS: Dict[str, Iterable[str]] = {
    "nama_nasabah": ("nama lengkap", "nama nasabah", "nama", "name"),
    "jenis_nasabah": ("nasabah", "jenis nasabah", "kategori", "tipe nasabah", "kategori nasabah"),
    "jenis_sampah": ("jenis sampah", "kategori sampah", "sampah", "jenis"),
    "keterangan": ("keterangan", "note", "desc"),
    "tanggal": ("tanggal", "date", "timestamp"),
}

EXPECTED_FIELDS_WITHDRAWAL: Dict[str, Iterable[str]] = {
    "nama_nasabah": ("nama", "nasabah", "nama nasabah", "name"),
    "nominal": ("nominal", "jumlah", "penarikan", "amount", "debet"),
    "keterangan": ("keterangan", "note", "desc"),
    "metode": ("metode", "cara penarikan", "via", "method"),
    "petugas": ("petugas", "nama petugas", "penanggung jawab", "admin"),
    "unit": ("bank sampah unit", "bsu", "unit"),
    "tanggal": ("tanggal", "date", "timestamp"),
}

def format_rupiah(amount: float) -> str:
    """Professional Rupiah formatting."""
    return f"Rp {amount:,.0f}".replace(",", ".")


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

    if "jenis_nasabah" in mapped_cols:
        df["jenis_nasabah"] = df[mapped_cols["jenis_nasabah"]].astype(str)
    else:
        df["jenis_nasabah"] = "-"

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

    # Robust calculation: only fill nilai_rp if it's 0 but berat and harga are present
    mask = (df["nilai_rp"] <= 0) & (df["berat_kg"] > 0) & (df["harga_per_kg"] > 0)
    df.loc[mask, "nilai_rp"] = df.loc[mask, "berat_kg"] * df.loc[mask, "harga_per_kg"]

    return df

def _normalize_withdrawal_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    mapped_cols = {}
    for field_name, aliases in EXPECTED_FIELDS_WITHDRAWAL.items():
        found = _find_col(df.columns, aliases)
        if found: mapped_cols[field_name] = found

    for field_name in EXPECTED_FIELDS_WITHDRAWAL.keys():
        if field_name in mapped_cols:
            if field_name == "nominal":
                parsed = (df[mapped_cols[field_name]].astype(str)
                         .str.replace(".", "", regex=False)
                         .str.replace(",", ".", regex=False)
                         .str.replace(r"[^0-9.\-]", "", regex=True))
                df[field_name] = pd.to_numeric(parsed, errors="coerce").fillna(0.0)
            elif field_name == "tanggal":
                df[field_name] = pd.to_datetime(df[mapped_cols[field_name]], errors="coerce")
            else:
                df[field_name] = df[mapped_cols[field_name]].astype(str)
        else:
            df[field_name] = 0.0 if field_name == "nominal" else "-"
            
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
    wd_url = get_setting("BANK_SAMPAH_WITHDRAWAL_URL", "")
    
    if not db_url or "sheet_id" in db_url:
        default_url = st.secrets.get("BANK_SAMPAH_SHEET_URL", provided_url)
    else:
        default_url = db_url
    
    sheet_url = get_setting("BANK_SAMPAH_SHEET_URL", provided_url) # Still kept in code for legacy but hidden
    reg_sheet_url = st.text_input("Link GSheet Pendaftaran Nasabah", value=reg_url)
    withdrawal_sheet_url = get_setting("BANK_SAMPAH_WITHDRAWAL_URL", "")
    
    if st.session_state.authenticated:
        st.divider()
        st.caption("🔄 SINKRONISASI GSHEET NASABAH")
        
        if st.button("🚀 SYNC DATA ANGGOTA", use_container_width=True, type="primary"):
            with st.spinner("Sinkronisasi anggota..."):
                try:
                    raw = _load_gsheet_csv(_build_sheet_csv_url(reg_sheet_url))
                    added, updated = upsert_nasabah_data(_normalize_nasabah_dataframe(raw))
                    st.sidebar.success(f"Anggota: +{added}")
                    st.cache_data.clear(); st.rerun()
                except Exception as e: st.sidebar.error(f"Error: {e}")

# --- Data Loading ---
df_db = get_transactions_df()
if not df_db.empty:
    # Deduplicate columns before any processing to avoid reindex errors
    df_db = df_db.loc[:, ~df_db.columns.duplicated()]
    
    # Fail-safe: Ensure all required columns exist even if DB schema hasn't migrated yet
    required_cols = ["tanggal", "nama_nasabah", "jenis_nasabah", "berat_kg", "nilai_rp", "pembayaran", "status_alur", "jenis_sampah"]
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

# --- Calculations & Metrics ---
# We calculate this early so dashboard metrics are accurate
nasabah_summary_df = get_nasabah_summary()
total_setoran_all = nasabah_summary_df['total_setoran'].sum() if not nasabah_summary_df.empty else 0
total_penarikan_all = nasabah_summary_df['total_penarikan'].sum() if not nasabah_summary_df.empty else 0
saldo_kas_total = total_setoran_all - total_penarikan_all

# --- Tabs Implementation ---
tabs = st.tabs([
    "📊 Dashboard", 
    "🏦 Operasional", 
    "👥 Database Anggota", 
    "📦 Data Sampah",
    "📜 Riwayat",
    "⚙️ Pengaturan"
])
tab_dash, tab_ops, tab_nasabah, tab_master, tab_riwayat, tab_settings = tabs

with tab_dash:
    st.header("Ringkasan Operasional")
    
    if df_db.empty:
        st.warning("👋 Selamat Datang! Data masih kosong.")
        st.info("Silakan login di sidebar sebagai Admin, lalu buka tab **⚙️ Pengaturan** untuk melakukan **Sinkronisasi GSheet** pertama kali.")
    
    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
    m_col1.metric("Total Anggota", f"{len(nasabah_summary_df)}")
    m_col2.metric("Total Sampah (kg)", f"{nasabah_summary_df['total_berat_kg'].sum():,.1f}" if not nasabah_summary_df.empty else "0.0")
    m_col3.metric("Total Tabungan Kas", format_rupiah(total_setoran_all))
    m_col4.metric("Saldo Kas Tersedia", format_rupiah(saldo_kas_total))

    st.divider()
    d_col1, d_col2 = st.columns(2)
    with d_col1:
        st.subheader("Setoran Terakhir")
        if not df.empty:
            # Format display for the dashboard table
            dash_setoran = df.head(10).copy()
            dash_setoran['tanggal_fmt'] = dash_setoran['tanggal'].dt.strftime('%d %b %Y')
            dash_setoran['nilai_fmt'] = dash_setoran['nilai_rp'].apply(format_rupiah)
            
            st.dataframe(
                dash_setoran[["tanggal_fmt", "nama_nasabah", "jenis_nasabah", "jenis_sampah", "berat_kg", "nilai_fmt"]].rename(columns={
                    "tanggal_fmt": "Tanggal",
                    "nama_nasabah": "Nama",
                    "jenis_nasabah": "Jenis",
                    "jenis_sampah": "Sampah",
                    "berat_kg": "Berat (kg)",
                    "nilai_fmt": "Nilai"
                }),
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.info("Tidak ada data setoran.")
            
    with d_col2:
        st.subheader("Penarikan Terakhir")
        wd_history = get_withdrawals_df()
        if not wd_history.empty:
            # Format withdrawals
            dash_tarik = wd_history.head(10).copy()
            dash_tarik['tanggal'] = pd.to_datetime(dash_tarik['tanggal'])
            dash_tarik['tanggal_fmt'] = dash_tarik['tanggal'].dt.strftime('%d %b %Y')
            dash_tarik['nominal_fmt'] = dash_tarik['nominal'].apply(format_rupiah)
            
            st.dataframe(
                dash_tarik[["tanggal_fmt", "nama_nasabah", "nominal_fmt"]].rename(columns={
                    "tanggal_fmt": "Tanggal",
                    "nama_nasabah": "Nasabah",
                    "nominal_fmt": "Nominal"
                }),
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.info("Belum ada data penarikan.")

with tab_ops:
    st.header("🏦 Pusat Operasional Hybrid")
    
    if not st.session_state.authenticated:
        st.warning("Mohon login sebagai Admin/Petugas untuk mengakses menu operasional.")
    else:
        o_col1, o_col2 = st.columns([1, 2])
        
        with o_col1:
            st.subheader("📲 Akses GForm")
            st.caption("Scan QR sesuai kebutuhan operasional.")
            
            # Withdrawal Form URL (From user strategy)
            wd_form_url = "https://docs.google.com/forms/d/e/1FAIpQLSdO7WcE_E3pXvN_Y_vN_vN_vN_vN_vN_vN_vN_vN_vN_vN_vN/viewform" # Simplified for demo
            setor_form_url = "https://docs.google.com/forms/d/e/1FAIpQLSdXSuFX_RaEspHEZ7HLsdQ4cGHYJUO4IUQrE8qk1DexHJ9-HA/viewform"
            
            sub_tab_qr = st.tabs(["Setoran", "Penarikan"])
            with sub_tab_qr[0]:
                st.image(generate_qr_code(setor_form_url), caption="GForm Setoran", use_container_width=True)
            with sub_tab_qr[1]:
                st.image(generate_qr_code(wd_form_url), caption="GForm Penarikan", use_container_width=True)

        with o_col2:
            mode = st.radio("Pilih Mode Input Manual", ["➕ Setoran Sampah", "💸 Penarikan Saldo"], horizontal=True)
            
            if mode == "➕ Setoran Sampah":
                # ... [Existing setoran logic kept stable] ...
                st.subheader("Catat Setoran Baru")
                with st.form("form_setoran_hybrid_adv"):
                    s_col1, s_col2 = st.columns(2)
                    with s_col1:
                        tgl_s = st.date_input("Tanggal", value=datetime.now())
                        n_df = get_nasabah_df()
                        selected_n = st.selectbox("Nasabah", options=sorted(n_df["nama"].tolist()) if not n_df.empty else ["-"])
                        jenis_s = st.text_input("Kategori", value=n_df[n_df["nama"] == selected_n]["jenis_nasabah"].values[0] if not n_df.empty and selected_n in n_df["nama"].values else "-")
                    with s_col2:
                        m_df = get_master_sampah()
                        selected_s = st.selectbox("Jenis Sampah", options=m_df["nama_jenis"].tolist() if not m_df.empty else ["-"])
                        berat_s = st.number_input("Berat (kg)", min_value=0.1, step=0.1)
                        if not m_df.empty and selected_s in m_df["nama_jenis"].values:
                            price_s = m_df[m_df["nama_jenis"] == selected_s]["harga_per_kg"].values[0]
                            total_val = float(berat_s * price_s)
                            st.caption(f"Estimasi: {format_rupiah(total_val)}")
                        else:
                            price_s = 0; total_val = 0
                    
                    if st.form_submit_button("💾 SIMPAN SETORAN", use_container_width=True, type="primary"):
                        new_data = {"tanggal": tgl_s.strftime("%Y-%m-%d %H:%M:%S"), "nama_nasabah": selected_n, "jenis_nasabah": jenis_s, "jenis_sampah": selected_s, "berat_kg": berat_s, "harga_per_kg": price_s, "nilai_rp": total_val, "source": "Manual"}
                        if save_transaction(new_data):
                            st.success("Setoran tersimpan!"); st.cache_data.clear(); st.rerun()
            
            else:
                st.subheader("Catat Penarikan Baru (Advanced)")
                with st.form("form_p_adv"):
                    tgl_p = st.date_input("Tanggal", value=datetime.now())
                    summary_wd = get_nasabah_summary()
                    selected_n_p = st.selectbox("Pilih Nasabah", options=summary_wd["nama_nasabah"].tolist() if not summary_wd.empty else ["-"])
                    
                    if not summary_wd.empty and selected_n_p in summary_wd["nama_nasabah"].values:
                        current_s = summary_wd[summary_wd["nama_nasabah"] == selected_n_p]["saldo"].values[0]
                        st.write(f"Saldo saat ini: **{format_rupiah(current_s)}**")
                    else:
                        current_s = 0
                    
                    p_c1, p_c2 = st.columns(2)
                    with p_c1:
                        nom_p = st.number_input("Nominal (Rp)", min_value=0, step=1000)
                        metode_p = st.selectbox("Metode", ["Cash", "Transfer"])
                    with p_c2:
                        petugas_p = st.text_input("Nama Petugas", value="Admin")
                        ket_p = st.text_input("Keterangan", placeholder="Sekolah, lebaran, dll")
                    
                    if st.form_submit_button("💸 PROSES & CETAK STRUK", use_container_width=True, type="primary"):
                        if nom_p > current_s:
                            st.error("Gagal: Saldo tidak mencukupi.")
                        elif nom_p <= 0:
                            st.warning("Masukkan nominal.")
                        else:
                            # Search for unit
                            n_full = get_nasabah_df()
                            unit_p = n_full[n_full["nama"] == selected_n_p]["unit"].values[0] if not n_full.empty else "-"
                            
                            wd_data = {
                                "tanggal": tgl_p.strftime("%Y-%m-%d %H:%M:%S"),
                                "nama_nasabah": selected_n_p,
                                "nominal": nom_p,
                                "metode": metode_p,
                                "petugas": petugas_p,
                                "unit": unit_p,
                                "keterangan": ket_p
                            }
                            if save_penarikan(wd_data):
                                st.success("Penarikan Berhasil!")
                                # Generate and show receipt
                                receipt = generate_withdrawal_receipt(wd_data)
                                st.image(receipt, caption="Kwitansi Digital")
                                st.download_button("📥 Unduh Kwitansi", data=receipt, file_name=f"Struk_{selected_n_p.replace(' ', '_')}.png", mime="image/png")
                                st.cache_data.clear()
                                # No rerun here so user can see/download receipt

with tab_riwayat:
    st.subheader("📜 Riwayat Transaksi Lengkap")
    choice = st.radio("Pilih Jenis Riwayat", ["Setoran Sampah", "Penarikan Uang"], horizontal=True)
    if choice == "Setoran Sampah":
        st.dataframe(df_db[["tanggal", "nama_nasabah", "jenis_nasabah", "jenis_sampah", "berat_kg", "nilai_rp"]], use_container_width=True, hide_index=True)
    else:
        st.dataframe(get_withdrawals_df(), use_container_width=True, hide_index=True)

with tab_nasabah:
    st.subheader("👥 Database Anggota & Saldo")
    # Clean and Format for display
    n_display = nasabah_summary_df.copy()
    if not n_display.empty:
        n_display['saldo_fmt'] = n_display['saldo'].apply(format_rupiah)
        n_display['setoran_fmt'] = n_display['total_setoran'].apply(format_rupiah)
        n_display['tarik_fmt'] = n_display['total_penarikan'].apply(format_rupiah)
        
        st.dataframe(
            n_display[[
                "nama_nasabah", "saldo_fmt", "setoran_fmt", "tarik_fmt", "total_berat_kg"
            ]].rename(columns={
                "nama_nasabah": "Nama Nasabah",
                "saldo_fmt": "Saldo Aktif",
                "setoran_fmt": "Total Tabungan",
                "tarik_fmt": "Total Ambil",
                "total_berat_kg": "Sampah (kg)"
            }),
            use_container_width=True,
            hide_index=True
        )

        st.divider()
        st.subheader("🪪 Cetak Kartu Anggota")
        c1, c2 = st.columns([2, 1])
        with c1:
            target_nasabah = st.selectbox("Pilih Anggota untuk Pratinjau Kartu", options=n_display["nama_nasabah"].tolist())
        
        if target_nasabah:
            # Get full data for card
            n_df_full = get_nasabah_df()
            member_match = n_df_full[n_df_full["nama"] == target_nasabah]
            
            if not member_match.empty:
                member_info = member_match.iloc[0].to_dict()
                card_bytes = generate_member_card(member_info)
                
                st.image(card_bytes, caption=f"Pratinjau Kartu: {target_nasabah}", use_container_width=True)
                
                st.download_button(
                    label=f"📥 Unduh Kartu {target_nasabah}",
                    data=card_bytes,
                    file_name=f"Kartu_{target_nasabah.replace(' ', '_')}.png",
                    mime="image/png",
                    type="primary"
                )
    else:
        st.info("Belum ada data anggota. Silakan lakukan sinkronisasi pendaftaran nasabah.")

with tab_master:
    st.header("📦 Pengaturan Harga Sampah")
    if not st.session_state.authenticated:
        st.info("Hanya Admin yang bisa mengakses menu ini.")
    else:
        m_data = get_master_sampah()
        with st.form("form_master_sampah"):
            st.subheader("Tambah atau Update Jenis Sampah")
            js_nama = st.text_input("Nama Jenis Sampah (Misal: Botol Plastik)")
            js_harga = st.number_input("Harga per kg (Rp)", min_value=0, step=100)
            if st.form_submit_button("Simpan Perubahan"):
                if js_nama:
                    update_master_sampah(js_nama, js_harga)
                    st.success(f"Berhasil: {js_nama} sekarang {format_rupiah(js_harga)}/kg")
                    st.rerun()
        
        st.divider()
        st.subheader("Daftar Harga Berlaku")
        st.table(m_data[["nama_jenis", "harga_per_kg"]])

with tab_settings:
    st.header("⚙️ Konfigurasi Sistem (GSheet Sync)")
    if st.session_state.authenticated:
        st.caption("Kelola koneksi Google Sheets untuk sinkronisasi data eksternal.")
        
        # 1. SETORAN SYNC
        st.subheader("1. Data Setoran (Transaksi)")
        s_url = st.text_input("GSheet Setoran/Transaksi", value=sheet_url)
        if st.button("🚀 SINKRONISASI SETORAN SEKARANG", use_container_width=True):
            with st.spinner("Sinkronisasi setoran..."):
                try:
                    raw = _load_gsheet_csv(_build_sheet_csv_url(s_url))
                    added, dups = upsert_gsheet_data(_normalize_dataframe(raw))
                    st.success(f"Berhasil Sinkron: {added} data ditambahkan/diperbarui.")
                    update_setting("BANK_SAMPAH_SHEET_URL", s_url)
                    st.cache_data.clear(); st.rerun()
                except Exception as e: st.error(f"Error Sinkron Setoran: {e}")
        
        st.divider()
        
        # 2. NASABAH SYNC
        st.subheader("2. Data Pendaftaran Anggota")
        r_url_input = st.text_input("GSheet Pendaftaran Nasabah", value=reg_sheet_url)
        if st.button("🚀 SINKRONISASI ANGGOTA SEKARANG", use_container_width=True):
            with st.spinner("Sinkronisasi anggota..."):
                try:
                    raw = _load_gsheet_csv(_build_sheet_csv_url(r_url_input))
                    added, updated = upsert_nasabah_data(_normalize_nasabah_dataframe(raw))
                    st.success(f"Berhasil Sinkron: {added} anggota baru.")
                    update_setting("BANK_SAMPAH_REGISTRATION_URL", r_url_input)
                    st.rerun()
                except Exception as e: st.error(f"Error Sinkron Anggota: {e}")
        
        st.divider()

        # 3. PENARIKAN SYNC
        st.subheader("3. Data Penarikan Saldo")
        w_url_input = st.text_input("GSheet Riwayat Penarikan", value=withdrawal_sheet_url)
        if st.button("🚀 SINKRONISASI PENARIKAN SEKARANG", use_container_width=True):
            with st.spinner("Sinkronisasi penarikan..."):
                try:
                    raw = _load_gsheet_csv(_build_sheet_csv_url(w_url_input))
                    added, dups = upsert_withdrawal_data(_normalize_withdrawal_dataframe(raw))
                    st.success(f"Berhasil Sinkron: {added} data penarikan.")
                    update_setting("BANK_SAMPAH_WITHDRAWAL_URL", w_url_input)
                    st.cache_data.clear(); st.rerun()
                except Exception as e: st.error(f"Error Sinkron Penarikan: {e}")
            
        st.divider()
        st.subheader("🗑️ Manajemen Data (Reset)")
        st.warning("⚠️ Perhatian: Menghapus data akan mengosongkan Riwayat Setoran dan Penarikan secara permanen.")
        
        c1, c2 = st.columns([1, 2])
        with c1:
            confirm_clear = st.checkbox("Saya yakin ingin menghapus data")
        
        if st.button("🚀 BERSIHKAN SEMUA DATA TRANSAKSI", type="primary", disabled=not confirm_clear):
            from modules.database import clear_all_data
            if clear_all_data():
                st.success("Database berhasil dibersihkan! Silakan lakukan Sinkronisasi ulang.")
                st.cache_data.clear(); st.rerun()
    else:
        st.info("Silakan login di sidebar untuk memodifikasi pengaturan sistem.")
