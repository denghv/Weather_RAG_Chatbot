import os
import time
import datetime
import threading
import pytz
from influxdb_client import InfluxDBClient
from flask_socketio import SocketIO

# Tránh circular import bằng cách tự định nghĩa các hàm cần thiết
# thay vì import từ app.py

# Vietnam timezone
VN_TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')

# Định nghĩa lại các hàm cần thiết để tránh circular import
def get_influxdb_client():
    """Get InfluxDB client connection"""
    return InfluxDBClient(
        url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
        token=os.environ.get("INFLUXDB_TOKEN", "my-token"),
        org=os.environ.get("INFLUXDB_ORG", "my-org")
    )

def get_temperature_warning(temp_c):
    """Generate warning message based on temperature"""
    if temp_c is None:
        return None
    
    try:
        temp = float(temp_c)
        
        if temp < -10:
            return "⚠️ **Cực kỳ nguy hiểm (Lạnh)** — Cảnh báo hạ thân nhiệt và tê cóng, có thể gây tử vong. Khuyên bạn ở trong nhà, mặc ấm, tránh gió lạnh."
        elif -10 <= temp <= 0:
            return "⚠️ **Nguy hiểm (Lạnh)** — Có thể gây hạ thân nhiệt, da tím tái, run rẩy. Bạn nên mặc đủ ấm và hạn chế ra ngoài lâu."
        elif 1 <= temp <= 17:
            return "⚠️ **Cảnh báo (Lạnh nhẹ)** — Dễ bị cảm lạnh. Bạn nên giữ ấm khi ra ngoài sáng sớm hoặc ban đêm."
        elif 18 <= temp <= 32:
            return "✅ **An toàn** — Nhiệt độ lý tưởng. Chúc bạn một ngày tốt lành!"
        elif 33 <= temp <= 37:
            return "⚠️ **Cảnh báo (Nóng nhẹ)** — Có thể gây mất nước, mệt mỏi nhẹ. Bạn nên uống nhiều nước và tránh nắng gắt."
        elif 38 <= temp <= 41:
            return "⚠️ **Nguy hiểm (Nóng)** — Có nguy cơ say nắng, kiệt sức. Bạn nên nghỉ ngơi nơi mát và theo dõi sức khỏe."
        elif temp > 41:
            return "⚠️ **Cực kỳ nguy hiểm (Nóng)** — Có thể gây đột quỵ nhiệt, nguy hiểm tính mạng. Khẩn cấp cảnh báo bạn tìm nơi mát, uống nước và gọi hỗ trợ y tế nếu có dấu hiệu bất thường."
        else:
            return None
    except (ValueError, TypeError):
        return None

def get_pm25_warning(pm25):
    """Generate warning message based on PM2.5 levels"""
    if pm25 is None:
        return None
    
    try:
        pm25_val = float(pm25)
        
        if 0 <= pm25_val <= 12:
            return "✅ **PM2.5: Tốt** — Không ảnh hưởng sức khỏe. Sinh hoạt bình thường."
        elif 13 <= pm25_val <= 35:
            return "⚠️ **PM2.5: Trung bình** — Nhạy cảm nhẹ. Người già, trẻ nhỏ nên hạn chế hoạt động ngoài trời lâu."
        elif 36 <= pm25_val <= 55:
            return "⚠️ **PM2.5: Không tốt cho nhóm nhạy cảm** — Tránh ra ngoài nếu có bệnh hô hấp."
        elif 56 <= pm25_val <= 150:
            return "⚠️ **PM2.5: Có hại cho sức khỏe** — Mọi người nên hạn chế ra ngoài. Đóng cửa, lọc không khí."
        elif 151 <= pm25_val <= 250:
            return "⚠️ **PM2.5: Rất có hại** — Cần ở trong nhà, đeo khẩu trang chuyên dụng (N95) nếu ra ngoài."
        elif pm25_val > 250:
            return "⚠️ **PM2.5: Nguy hiểm** — Cảnh báo khẩn cấp. Không ra ngoài. Nguy cơ tử vong nếu tiếp xúc lâu."
        else:
            return None
    except (ValueError, TypeError):
        return None

def get_pm10_warning(pm10):
    """Generate warning message based on PM10 levels"""
    if pm10 is None:
        return None
    
    try:
        pm10_val = float(pm10)
        
        if 0 <= pm10_val <= 54:
            return "✅ **PM10: Tốt** — Chất lượng không khí tốt, an toàn cho sức khỏe."
        elif 55 <= pm10_val <= 154:
            return "⚠️ **PM10: Trung bình** — Chất lượng không khí chấp nhận được, nhưng có thể gây ảnh hưởng cho một số người nhạy cảm."
        elif 155 <= pm10_val <= 254:
            return "⚠️ **PM10: Không tốt cho nhóm nhạy cảm** — Người có bệnh hô hấp nên hạn chế ra ngoài trời."
        elif 255 <= pm10_val <= 354:
            return "⚠️ **PM10: Có hại cho sức khỏe** — Mọi người nên hạn chế hoạt động ngoài trời và đeo khẩu trang."
        elif pm10_val >= 355:
            return "⚠️ **PM10: Rất có hại** — Tránh ra ngoài trời, đóng cửa sổ và sử dụng máy lọc không khí."
        else:
            return None
    except (ValueError, TypeError):
        return None

class WeatherAlertSystem:
    def __init__(self, socketio):
        """Initialize the weather alert system with a SocketIO instance"""
        self.socketio = socketio
        self.check_interval = int(os.environ.get("ALERT_CHECK_INTERVAL", "300"))  # Default: check every 5 minutes
        self.running = False
        self.thread = None
        self.last_check_time = {}  # Store last check time for each location
        
    def start(self):
        """Start the alert system in a background thread"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._alert_loop)
            self.thread.daemon = True
            self.thread.start()
            print("Weather alert system started")
    
    def stop(self):
        """Stop the alert system"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
            print("Weather alert system stopped")
    
    def _alert_loop(self):
        """Main loop for checking weather data and sending alerts"""
        while self.running:
            try:
                self._check_and_send_alerts()
            except Exception as e:
                print(f"Error in alert system: {e}")
            
            # Sleep for the specified interval
            time.sleep(self.check_interval)
    
    def _check_and_send_alerts(self):
        """Check for new weather data and send alerts if needed"""
        # Get latest weather data from InfluxDB
        weather_data = self._get_latest_weather_data()
        
        # Process each location's data
        alerts = []
        for location, data in weather_data.items():
            # Skip if we've already alerted for this data point
            if location in self.last_check_time and data['time'] <= self.last_check_time[location]:
                continue
            
            # Update last check time
            self.last_check_time[location] = data['time']
            
            location_alerts = []
            
            # Check temperature for warnings
            if 'temp_c' in data:
                warning = get_temperature_warning(data['temp_c'])
                if warning:
                    # Only send alerts for dangerous conditions
                    if "Nguy hiểm" in warning or "Cực kỳ nguy hiểm" in warning:
                        temp_alert = f"Nhiệt độ hiện tại {data['temp_c']}°C - {warning}"
                        location_alerts.append(temp_alert)
            
            # Check PM2.5 for warnings
            if 'pm2_5' in data:
                warning = get_pm25_warning(data['pm2_5'])
                if warning:
                    # Only send alerts for harmful conditions
                    if "Có hại" in warning or "Rất có hại" in warning or "Nguy hiểm" in warning:
                        pm25_alert = f"Chỉ số PM2.5 hiện tại {data['pm2_5']} μg/m³ - {warning}"
                        location_alerts.append(pm25_alert)
            
            # Check PM10 for warnings
            if 'pm10' in data:
                warning = get_pm10_warning(data['pm10'])
                if warning:
                    # Only send alerts for harmful conditions
                    if "Có hại" in warning or "Rất có hại" in warning:
                        pm10_alert = f"Chỉ số PM10 hiện tại {data['pm10']} μg/m³ - {warning}"
                        location_alerts.append(pm10_alert)
            
            # If we have any alerts for this location, add them
            if location_alerts:
                location_header = f"**{location}**:"
                location_alert_text = "\n- " + "\n- ".join(location_alerts)
                alerts.append(location_header + location_alert_text)
        
        # Send alerts if any
        if alerts:
            alert_text = "\n\n".join(alerts)
            full_message = f"**CẢNH BÁO THỜI TIẾT TỰ ĐỘNG**\n\n{alert_text}"
            
            # Send via SocketIO to all connected clients
            self.socketio.emit('weather_alert', {'message': full_message})
            print(f"Sent weather alerts: {full_message}")
    
    def _get_latest_weather_data(self):
        """Get the latest weather data for all locations from InfluxDB"""
        client = get_influxdb_client()
        query_api = client.query_api()
        
        # Dictionary to store weather data
        weather_data = {}
        
        # Fields to query
        fields = ["temp_c", "pm2_5", "pm10"]
        
        for field in fields:
            # Query to get the latest data for all locations for this field
            query = f"""
            from(bucket: "weather")
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "weather")
              |> filter(fn: (r) => r._field == "{field}")
              |> group(columns: ["location"])
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: 1)
              |> timeShift(duration: 7h, columns: ["_time"])
            """
            
            try:
                result = query_api.query(org=os.environ.get("INFLUXDB_ORG", "my-org"), query=query)
                
                # Process results
                for table in result:
                    for record in table.records:
                        location = record.values.get("location")
                        time = record.get_time()
                        value = record.get_value()
                        
                        # Initialize location data if not exists
                        if location not in weather_data:
                            weather_data[location] = {"time": time}
                        
                        # Update with the latest time if newer
                        if time > weather_data[location]["time"]:
                            weather_data[location]["time"] = time
                        
                        # Add field value
                        weather_data[location][field] = value
                        
            except Exception as e:
                print(f"Query error for {field} in alert system: {e}")
        
        client.close()
        return weather_data

# Initialize the alert system (will be called from app.py)
def init_alert_system(socketio):
    alert_system = WeatherAlertSystem(socketio)
    alert_system.start()
    return alert_system
