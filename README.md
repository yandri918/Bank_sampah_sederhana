# Bank Sampah Streamlit Baru

Starter project dashboard Bank Sampah berbasis Streamlit yang membaca response Google Form dari Google Sheet.

## Fitur

- Database nasabah
- Visualisasi alur sampah
- Pembukuan bulanan
- Ringkasan keuangan
- Export data mentah CSV

## Setup

1. Buat virtual environment (opsional):
   - `python -m venv .venv`
   - `.venv\Scripts\activate`
2. Install dependency:
   - `pip install -r requirements.txt`
3. Jalankan aplikasi:
   - `streamlit run app.py`

## Hubungkan Google Form

1. Buka Google Form Anda:
   - `https://docs.google.com/forms/d/e/1FAIpQLSdXSuFX_RaEspHEZ7HLsdQ4cGHYJUO4IUQrE8qk1DexHJ9-HA/viewform`
2. Di tab Responses, klik ikon Google Sheet untuk membuat sheet response.
3. Ubah sharing Google Sheet menjadi **Anyone with the link can view**.
4. Tempel URL Google Sheet di sidebar aplikasi.

## Opsi lewat secrets

Buat file `.streamlit/secrets.toml`:

```toml
BANK_SAMPAH_SHEET_URL = "https://docs.google.com/spreadsheets/d/<sheet_id>/edit#gid=0"
```
