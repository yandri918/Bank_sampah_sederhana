# ♻️ Bank Sampah Digital Indonesia (BSDI) - Multi-Tenant Hub
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_svg.svg)](https://bank-sampah-sederhana.streamlit.app/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![SQLite](https://img.shields.io/badge/database-SQLite-003B57.svg)](https://www.sqlite.org/index.html)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**BSDI Hub** adalah platform manajemen Bank Sampah profesional yang dirancang untuk mendigitalisasi operasional Bank Sampah di tingkat RT, RW, hingga Kecamatan. Platform ini mendukung sistem **Multi-Tenant**, memungkinkan banyak organisasi Bank Sampah mengelola data mereka secara mandiri, aman, dan terisolasi dalam satu aplikasi.

---

## 🔥 Fitur Unggulan (Investor-Ready)

### 1. 🏢 Multi-Tenant & Data Isolation
- **Registrasi Mandiri**: Pengelola BSI baru dapat mendaftarkan unit mereka langsung dari aplikasi.
- **Isolasi Data**: Setiap unit BSI memiliki "ruang kerja" digital yang privat. Data nasabah dan transaksi tidak akan pernah tercampur antar akun.
- **Custom Branding**: Pengaturan nama BSI dan target tahunan yang berbeda untuk setiap akun.

### 2. 🔌 Hybrid Operations Hub
- **Google Form Sync**: Sinkronisasi otomatis dari setoran lapangan melalui Google Forms.
- **Manual Input Advanced**: Fitur input langsung via dashboard untuk operasional cepat.
- **QR Code Dinamis**: QR Code di dashboard akan berubah otomatis mengikuti Google Form yang dikonfigurasi di tab Pengaturan.

### 3. 📊 Strategic Analytics Dashboard
- **KPI Tracking**: Visualisasi target tahunan menggunakan *Gauge Charts* (Nasabah, Sampah, Rupiah).
- **Arus Kas & Saldo**: Monitoring saldo kas vs dana nasabah secara real-time.
- **Analisis Wilayah (BSU)**: Grafik performa antar unit (BSU) untuk evaluasi wilayah.

### 4. 📑 Automated Official Reporting
- **Generator Laporan DLH**: Membuat laporan bulanan resmi (PDF) secara otomatis yang siap diserahkan ke Dinas Lingkungan Hidup.
- **Proposal Penguatan**: Generate proposal otomatis untuk kebutuhan pendanaan CSR atau kerjasama perbankan.
- **Digital Receipt**: Cetak struk penarikan digital untuk nasabah.

### 5. 🪪 Digital Membership System
- **Member Card**: Generate kartu anggota digital untuk setiap nasabah.
- **QR Identity**: Setiap kartu dilengkapi identitas digital untuk mempermudah identifikasi saat penimbangan.

---

## 🛠️ Tech Stack
- **Frontend/UI**: [Streamlit](https://streamlit.io/) (Data-rich dynamic interface)
- **Backend Logic**: Python 3.10+
- **Database**: SQLite (Local filesystem storage)
- **Visualization**: Plotly Express & Chart.js
- **PDF Engine**: FPDF2
- **QR Engine**: Qrcode (Python)

---

## 🚀 Panduan Instalasi

### 1. Clone Repositori
```bash
git clone https://github.com/yandri918/Bank_sampah_sederhana.git
cd Bank_sampah_sederhana
```

### 2. Setup Environment
```bash
# Buat Virtual Environment
python -m venv venv

# Aktivasi di Windows
venv\Scripts\activate

# Aktivasi di Mac/Linux
source venv/bin/activate
```

### 3. Install Dependensi
```bash
pip install -r requirements.txt
```

### 4. Jalankan Aplikasi
```bash
streamlit run app.py
```

---

## ⚙️ Konfigurasi Google Sheets (Opsi Hybrid)
Untuk menggunakan fitur sinkronisasi otomatis:
1. Pastikan Google Sheet Anda diatur ke **"Anyone with the link can view"**.
2. Masukkan link Google Sheet Anda di tab **⚙️ Pengaturan** di dalam aplikasi.
3. Aplikasi akan menarik data secara otomatis dan memindahkannya ke database lokal SQLite.

---

## 🔐 Keamanan Data
Aplikasi ini dijamin keamanannya melalui:
- **Hashing Password**: Menggunakan protokol keamanan internal.
- **Filter Organization ID**: Setiap query database dikunci oleh ID Organisasi (`bsi_id`) dari akun yang sedang login.

---

## 🤝 Kontribusi
Kami sangat terbuka untuk kontribusi dalam pengembangan fitur pengelolaan lingkungan berkelanjutan.
1. Fork proyek ini.
2. Buat branch fitur baru (`git checkout -b fitur/HebatBaru`).
3. Commit perubahan Anda (`git commit -m 'Menambah fitur Hebat'`).
4. Push ke branch (`git push origin fitur/HebatBaru`).
5. Buat Pull Request.

---

**Dibuat dengan ❤️ untuk lingkungan Indonesia yang lebih bersih dan berkelanjutan.** ♻️🇮🇩
