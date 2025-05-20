import pandas as pd

# Mapping tới các tỉnh từ file provinces.py
city_to_province = {
    "Ha Noi": "Hanoi",
    "Bien Hoa": "Dong Nai",
    "Buon Me Thuot": "Dak Lak",
    "Cam Pha": "Quang Ninh",
    "Cam Ranh": "Khanh Hoa",
    "Chau Doc": "An Giang",
    "Da Lat": "Lam Dong",
    "Hong Gai": "Quang Ninh",
    "Hue": "Thua Thien Hue",
    "Long Xuyen": "An Giang",
    "My Tho": "Tien Giang",
    "Nha Trang": "Khanh Hoa",
    "Phan Rang": "Ninh Thuan",
    "Phan Thiet": "Binh Thuan",
    "Play Cu": "Gia Lai",
    "Qui Nhon": "Binh Dinh",
    "Rach Gia": "Kien Giang",
    "Tam Ky": "Quang Nam",
    "Tan An": "Long An",
    "Tuy Hoa": "Phu Yen",
    "Uong Bi": "Quang Ninh",
    "Viet Tri": "Phu Tho",
    "Vinh": "Nghe An",
    "Vung Tau": "Ba Ria-Vung Tau"
}

# Load file
csv_path = "d:\\DATN\\Weather_RAG_Chatbot\\dataset\\weather2009-2021.csv"
df = pd.read_csv(csv_path)

# Chuẩn hóa tên tỉnh thành
df['province'] = df['province'].replace(city_to_province)

df.to_csv(csv_path, index=False)
print(f"Province names standardized in {csv_path}")

