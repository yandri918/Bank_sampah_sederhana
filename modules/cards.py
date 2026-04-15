import qrcode
from PIL import Image, ImageDraw, ImageFont
import io
import os

def generate_member_card(member_data):
    """
    Generates a digital member card image.
    member_data: dict with keys 'nama', 'unit', 'jenis_nasabah', 'id'
    """
    # Card Dimensions (800x500)
    width, height = 800, 500
    
    # Create background (Green Gradient effect)
    card = Image.new('RGB', (width, height), color=(255, 255, 255))
    draw = ImageDraw.Draw(card)
    
    # Draw Green Header Area
    draw.rectangle([0, 0, width, 120], fill=(46, 139, 87)) # SeaGreen
    
    # Try to load fonts
    try:
        # Standard fonts in Windows/Common environments
        font_bold = ImageFont.truetype("arialbd.ttf", 40)
        font_main = ImageFont.truetype("arial.ttf", 30)
        font_small = ImageFont.truetype("arial.ttf", 20)
    except:
        # Fallback to default
        font_bold = ImageFont.load_default()
        font_main = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Header Text
    draw.text((30, 30), "BANK SAMPAH DIGITAL", fill=(255, 255, 255), font=font_bold)
    draw.text((30, 80), "Kartu Anggota Resmi", fill=(200, 255, 200), font=font_small)

    # Member Info Labels
    start_y = 150
    spacing = 50
    labels = [
        ("NAMA", member_data.get('nama', '-')),
        ("UNIT", member_data.get('unit', '-')),
        ("JENIS", member_data.get('jenis_nasabah', '-')),
        ("ID", f"BS-{member_data.get('id', '000'):04}")
    ]

    for i, (label, value) in enumerate(labels):
        y_pos = start_y + (i * spacing)
        draw.text((30, y_pos), label, fill=(100, 100, 100), font=font_small)
        draw.text((150, y_pos), f": {value}", fill=(0, 0, 0), font=font_main)

    # QR Code Generation
    qr_data = f"MEMBERSHIP:{member_data.get('nama')}:{member_data.get('id')}"
    qr = qrcode.QRCode(version=1, box_size=8, border=2)
    qr.add_data(qr_data)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    
    # Resize QR and paste onto card
    qr_img = qr_img.resize((200, 200))
    card.paste(qr_img, (width - 230, height - 250))
    
    # Border & Decoration
    draw.rectangle([0, 0, width-1, height-1], outline=(46, 139, 87), width=5)
    
    # Save to bytes
    img_byte_arr = io.BytesIO()
    card.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    
    return img_byte_arr

def generate_qr_code(data):
    """Generates a simple QR code image from data string and returns bytes."""
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    return img_byte_arr.getvalue()
