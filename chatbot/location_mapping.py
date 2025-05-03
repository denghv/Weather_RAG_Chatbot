"""
Mapping between Vietnamese and English location names for weather data
"""

LOCATION_MAPPING = {
    # Major cities
    "Hà Nội": "Hanoi",
    "TP.HCM": "Ho Chi Minh City",
    "Hồ Chí Minh": "Ho Chi Minh City",
    "Thành phố Hồ Chí Minh": "Ho Chi Minh City",
    "Đà Nẵng": "Da Nang",
    "Cần Thơ": "Can Tho",
    "Hải Phòng": "Hai Phong",
    
    # Other provinces
    "An Giang": "An Giang",
    "Bà Rịa - Vũng Tàu": "Ba Ria-Vung Tau",
    "Bắc Giang": "Bac Giang",
    "Bắc Kạn": "Bac Kan",
    "Bạc Liêu": "Bac Lieu",
    "Bắc Ninh": "Bac Ninh",
    "Bến Tre": "Ben Tre",
    "Bình Định": "Binh Dinh",
    "Bình Dương": "Binh Duong",
    "Bình Phước": "Binh Phuoc",
    "Bình Thuận": "Binh Thuan",
    "Cà Mau": "Ca Mau",
    "Cao Bằng": "Cao Bang",
    "Đắk Lắk": "Dak Lak",
    "Đắk Nông": "Dak Nong",
    "Điện Biên": "Dien Bien",
    "Đồng Nai": "Dong Nai",
    "Đồng Tháp": "Dong Thap",
    "Gia Lai": "Gia Lai",
    "Hà Giang": "Ha Giang",
    "Hà Nam": "Ha Nam",
    "Hà Tĩnh": "Ha Tinh",
    "Hải Dương": "Hai Duong",
    "Hậu Giang": "Hau Giang",
    "Hòa Bình": "Hoa Binh",
    "Hưng Yên": "Hung Yen",
    "Khánh Hòa": "Khanh Hoa",
    "Kiên Giang": "Kien Giang",
    "Kon Tum": "Kon Tum",
    "Lai Châu": "Lai Chau",
    "Lâm Đồng": "Lam Dong",
    "Lạng Sơn": "Lang Son",
    "Lào Cai": "Lao Cai",
    "Long An": "Long An",
    "Nam Định": "Nam Dinh",
    "Nghệ An": "Nghe An",
    "Ninh Bình": "Ninh Binh",
    "Ninh Thuận": "Ninh Thuan",
    "Phú Thọ": "Phu Tho",
    "Phú Yên": "Phu Yen",
    "Quảng Bình": "Quang Binh",
    "Quảng Nam": "Quang Nam",
    "Quảng Ngãi": "Quang Ngai",
    "Quảng Ninh": "Quang Ninh",
    "Quảng Trị": "Quang Tri",
    "Sóc Trăng": "Soc Trang",
    "Sơn La": "Son La",
    "Tây Ninh": "Tay Ninh",
    "Thái Bình": "Thai Binh",
    "Thái Nguyên": "Thai Nguyen",
    "Thanh Hóa": "Thanh Hoa",
    "Thừa Thiên Huế": "Thua Thien Hue",
    "Tiền Giang": "Tien Giang",
    "Trà Vinh": "Tra Vinh",
    "Tuyên Quang": "Tuyen Quang",
    "Vĩnh Long": "Vinh Long",
    "Vĩnh Phúc": "Vinh Phuc",
    "Yên Bái": "Yen Bai"
}

def get_english_location_name(vietnamese_name):
    """
    Convert Vietnamese location name to English
    """
    return LOCATION_MAPPING.get(vietnamese_name, vietnamese_name)
