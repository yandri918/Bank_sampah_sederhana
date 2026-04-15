from fpdf import FPDF
from datetime import datetime

class BSIPDF(FPDF):
    def header(self):
        # Arial bold 15
        self.set_font('Arial', 'B', 15)
        # Title
        self.cell(0, 10, 'BANK SAMPAH INDUK BINA MANDIRI', 0, 1, 'C')
        self.set_font('Arial', '', 10)
        self.cell(0, 5, 'Laporan Digital Terintegrasi', 0, 1, 'C')
        self.ln(10)
        # Line break
        self.line(10, 30, 200, 30)

    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font('Arial', 'I', 8)
        # Page number
        self.cell(0, 10, 'Halaman ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

def generate_official_report_pdf(data):
    """
    Generates an official monthly report for DLH.
    data: dict with metrics, waste_stats, activities, constraints, plans
    """
    pdf = BSIPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # 1. IDENTITAS
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'I. IDENTITAS OPERASIONAL', 0, 1, 'L')
    pdf.set_font('Arial', '', 11)
    pdf.cell(0, 7, f'Nama Unit: Bank Sampah Induk Bina Mandiri', 0, 1)
    pdf.cell(0, 7, f'Bulan Laporan: {datetime.now().strftime("%B %Y")}', 0, 1)
    pdf.ln(5)

    # 2. DATA UMUM (Metrics)
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'II. RANGKUMAN DATA UMUM', 0, 1, 'L')
    pdf.set_font('Arial', '', 11)
    pdf.cell(0, 7, f'- Jumlah Nasabah Terdaftar: {data["nasabah_count"]}', 0, 1)
    pdf.cell(0, 7, f'- Total Sampah Terkelola (Kg): {data["total_berat"]:,.1f}', 0, 1)
    pdf.cell(0, 7, f'- Total Perputaran Uang (Rp): {data["total_setoran"]:,.0f}', 0, 1)
    pdf.cell(0, 7, f'- Saldo Kas Aktif nasabah (Rp): {data["saldo"]:,.0f}', 0, 1)
    pdf.ln(5)

    # 3. REKAP SAMPAH (Tabel)
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'III. REKAPITULASI SAMPAH BERDASARKAN JENIS', 0, 1, 'L')
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(80, 10, 'Kategori Sampah', 1)
    pdf.cell(40, 10, 'Total Berat (Kg)', 1, 1)
    
    pdf.set_font('Arial', '', 11)
    for _, row in data["waste_stats"].iterrows():
        pdf.cell(80, 10, str(row['jenis_sampah']), 1)
        pdf.cell(40, 10, f"{row['total_berat']:,.2f}", 1, 1)
    pdf.ln(5)

    # 4. KEGIATAN & KENDALA
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'IV. AKTIVITAS & EVALUASI', 0, 1, 'L')
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 7, 'Kegiatan Bulan Ini:', 0, 1)
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 7, data.get("activities", "-"))
    
    pdf.ln(3)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 7, 'Kendala Lapangan:', 0, 1)
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 7, data.get("constraints", "-"))
    
    pdf.ln(3)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 7, 'Rencana Tindak Lanjut:', 0, 1)
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 7, data.get("plans", "-"))

    # Signatures
    pdf.ln(20)
    pdf.cell(120)
    pdf.cell(0, 10, f'Jakarta, {datetime.now().strftime("%d %B %Y")}', 0, 1, 'C')
    pdf.cell(120)
    pdf.cell(0, 10, 'Ketua BSI Bina Mandiri', 0, 1, 'C')
    pdf.ln(15)
    pdf.cell(120)
    pdf.set_font('Arial', 'BU', 11)
    pdf.cell(0, 10, '( ........................................ )', 0, 1, 'C')

    return bytes(pdf.output())

def generate_funding_proposal_pdf(data):
    """
    Generates a funding/CSR proposal to bank/investors.
    """
    pdf = BSIPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    
    pdf.set_font('Arial', 'B', 20)
    pdf.ln(40)
    pdf.cell(0, 20, 'PROPOSAL PENGUATAN OPERASIONAL', 0, 1, 'C')
    pdf.cell(0, 20, 'BANK SAMPAH INDUK BINA MANDIRI', 0, 1, 'C')
    pdf.ln(20)
    pdf.set_font('Arial', '', 14)
    pdf.cell(0, 10, 'Digitalisasi, Ekonomi Kerakyatan, dan Lingkungan Berkelanjutan', 0, 1, 'C')
    
    pdf.add_page()
    # Content
    sections = [
        ("I. LATAR BELAKANG", data.get("background", "")),
        ("II. TUJUAN PROGRAM", data.get("goals", "")),
        ("III. KEUNGGULAN SISTEM DIGITAL", "BSI Bina Mandiri telah mengimplementasikan sistem manajemen digital yang mencakup:\n- Database Nasional Terpadu\n- Dashboard Monitoring Real-time\n- Sistem Kartu Anggota Digital & QR Code\n- Laporan Keuangan Transparan"),
        ("IV. RENCANA ANGGARAN & KEBUTUHAN", data.get("budget", ""))
    ]
    
    for title, content in sections:
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, title, 0, 1)
        pdf.set_font('Arial', '', 11)
        pdf.multi_cell(0, 7, content)
        pdf.ln(5)
        
    return bytes(pdf.output())
