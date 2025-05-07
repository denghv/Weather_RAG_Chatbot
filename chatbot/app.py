import os
import json
import datetime
from flask import Flask, request, jsonify, render_template
from influxdb_client import InfluxDBClient
import openai
import pytz
import sys
from flask_socketio import SocketIO

# Add parent directory to path to import provinces
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from provinces import VIETNAM_PROVINCES
from location_mapping import get_english_location_name

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize OpenAI API key
openai.api_key = os.environ.get("OPENAI_API_KEY", "")

# InfluxDB connection
def get_influxdb_client():
    return InfluxDBClient(
        url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
        token=os.environ.get("INFLUXDB_TOKEN", "my-token"),
        org=os.environ.get("INFLUXDB_ORG", "my-org")
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ask', methods=['POST'])
def ask():
    user_question = request.json.get('question', '')
    
    if not user_question:
        return jsonify({"response": "Vui lòng nhập câu hỏi về thời tiết."})
    
    response = process_question(user_question)
    return jsonify({"response": response})

def extract_entities_with_chatgpt(question):
    """Use ChatGPT to extract location and time information from the question"""
    
    prompt = f"""
    Trích xuất thông tin về địa điểm và thời gian từ câu hỏi sau về thời tiết ở Việt Nam.
    Câu hỏi: "{question}"
    
    Các địa điểm hợp lệ là 63 tỉnh thành của Việt Nam: {', '.join(VIETNAM_PROVINCES)}
    
    Trả về một đối tượng JSON với cấu trúc ví dụ như sau:
    {{
        "locations": ["Địa điểm 1", "Địa điểm 2"], // Danh sách các địa điểm được đề cập trong câu hỏi
        "time_reference": "current" // Một trong các giá trị: current (hiện tại), today (hôm nay), yesterday (hôm qua), specific_date (ngày cụ thể)
    }}
    
    Nếu không tìm thấy địa điểm hợp lệ, trả về danh sách trống cho locations.
    Nếu không tìm thấy tham chiếu thời gian, giả định là "current".
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý hữu ích trích xuất thông tin địa điểm và thời gian từ các câu hỏi về thời tiết."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=200
        )
        
        result_text = response['choices'][0]['message']['content'].strip()
        
        # Extract JSON from the response
        try:
            # Find JSON in the response
            json_start = result_text.find('{')
            json_end = result_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = result_text[json_start:json_end]
                entities = json.loads(json_str)
                return entities
            else:
                return {"locations": [], "time_reference": "current"}
        except json.JSONDecodeError:
            return {"locations": [], "time_reference": "current"}
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return {"locations": [], "time_reference": "current"}

def generate_influxdb_query(entities):
    """Generate InfluxDB query based on extracted entities"""
    locations = entities.get("locations", [])
    time_reference = entities.get("time_reference", "current")
    
    # Default query for current weather
    query = """
    from(bucket: "weather")
    """
    
    # Add time range based on reference
    if time_reference == "current" or time_reference == "now":
        # Get data from the last 3 hours
        query += """
        |> range(start: -3h)
        """
    elif time_reference == "today":
        # Get records from today
        query += """
        |> range(start: today())
        """
    elif time_reference == "yesterday":
        # Get records from yesterday
        query += """
        |> range(start: -1d, stop: today())
        """
    else:
        # Default to last 24 hours
        query += """
        |> range(start: -24h)
        """
    
    # Filter for weather measurement
    query += """
    |> filter(fn: (r) => r._measurement == "weather")
    """
    
    # Add location filter if available
    if locations:
        location_filters = []
        for location in locations:
            # Convert Vietnamese location name to English
            english_location = get_english_location_name(location)
            print(f"Converting location: {location} -> {english_location}")
            location_filters.append(f'r.location == "{english_location}"')
        
        if location_filters:
            location_filter_str = " or ".join(location_filters)
            query += f"""
    |> filter(fn: (r) => {location_filter_str})
            """
    
    # Get the latest data for each location
    query += """
    |> group(columns: ["location", "_field"])
    |> sort(columns: ["_time"], desc: true)
    |> limit(n: 1)
    """
    
    # Convert to local timezone
    query += """
    |> timeShift(duration: 7h, columns: ["_time"])
    """
    
    return query

def execute_query(query):
    """Execute the InfluxDB query and return results"""
    client = get_influxdb_client()
    query_api = client.query_api()
    
    try:
        result = query_api.query(org=os.environ.get("INFLUXDB_ORG", "my-org"), query=query)
        
        # Process results
        weather_data = []
        for table in result:
            for record in table.records:
                weather_data.append({
                    "time": record.get_time(),
                    "location": record.values.get("location"),
                    "field": record.get_field(),
                    "value": record.get_value()
                })
        
        return weather_data
    except Exception as e:
        print(f"Query error: {e}")
        return []
    finally:
        client.close()

def format_weather_data(weather_data):
    """Format weather data into a structured dictionary"""
    if not weather_data:
        return {}
    
    # Group data by location
    grouped_data = {}
    for item in weather_data:
        location = item.get("location", "Unknown")
        time = item.get("time")
        field = item.get("field")
        value = item.get("value")
        
        if location not in grouped_data:
            grouped_data[location] = {"time": time, "data": {}}
        
        grouped_data[location]["data"][field] = value
    
    return grouped_data

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

def generate_response_with_chatgpt(question, weather_data):
    """Generate a natural language response using ChatGPT"""
    if not weather_data:
        return "Tôi không tìm thấy dữ liệu thời tiết phù hợp với câu hỏi của bạn. Vui lòng thử lại với một địa điểm cụ thể ở Việt Nam."
    
    # Format weather data for the prompt
    weather_info = ""
    warnings = []
    
    for location, info in weather_data.items():
        time_str = info["time"].strftime("%Y-%m-%d %H:%M:%S")
        data = info["data"]
        
        weather_info += f"Địa điểm: {location}, Thời gian: {time_str}\n"
        
        if "condition" in data:
            weather_info += f"- Điều kiện thời tiết: {data['condition']}\n"
        if "temp_c" in data:
            temp_c = data['temp_c']
            weather_info += f"- Nhiệt độ: {temp_c}°C\n"
            
            # Get temperature warning
            warning = get_temperature_warning(temp_c)
            if warning:
                warnings.append(f"**{location}**: {warning}")
                
        if "pm2_5" in data:
            pm25_val = data['pm2_5']
            weather_info += f"- Chỉ số PM2.5: {pm25_val} μg/m³\n"
            
            # Get PM2.5 warning
            warning = get_pm25_warning(pm25_val)
            if warning:
                warnings.append(f"**{location}**: {warning}")
                
        if "pm10" in data:
            pm10_val = data['pm10']
            weather_info += f"- Chỉ số PM10: {pm10_val} μg/m³\n"
            
            # Get PM10 warning
            warning = get_pm10_warning(pm10_val)
            if warning:
                warnings.append(f"**{location}**: {warning}")
                
        if "uv" in data:
            weather_info += f"- Chỉ số UV: {data['uv']}\n"
        
        weather_info += "\n"
    
    # Add warnings to the prompt
    warnings_text = "\n\n".join(warnings) if warnings else ""
    if warnings_text:
        warnings_section = f"\n\nCảnh báo thời tiết:\n{warnings_text}\n\n"
    else:
        warnings_section = ""
    
    prompt = f"""
    Dựa trên câu hỏi của người dùng và dữ liệu thời tiết được cung cấp, hãy tạo một câu trả lời tự nhiên và hữu ích.
    
    Câu hỏi: "{question}"
    
    Dữ liệu thời tiết:
    {weather_info}
    {warnings_section}
    Trả lời bằng tiếng Việt, cung cấp thông tin hữu ích và dễ hiểu. Nếu người dùng chỉ hỏi nhiệt độ thì trả lời nhiệt độ và lời khuyên liên quan, nếu hỏi về gì thì chỉ cần trả lời về tiêu chí đó không cần trả lời đầy đủ. Nếu có chỉ số chất lượng không khí, hãy giải thích ý nghĩa của nó (ví dụ: tốt, trung bình, kém, v.v.).
    
    Luôn đưa ra cảnh báo thời tiết nếu có, dựa trên nhiệt độ của các địa điểm.
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý thời tiết hữu ích, cung cấp thông tin thời tiết chính xác và lời khuyên hữu ích với kiểu thời tiết đó cho người dùng. Luôn đưa ra cảnh báo thời tiết nếu có, dựa trên nhiệt độ."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=800
        )
        
        response_text = response['choices'][0]['message']['content'].strip()
        
        # If no warnings in response but we have warnings, add them explicitly
        if warnings and not any(warning.split(":")[0].strip("*" ) in response_text for warning in warnings):
            response_text += "\n\n**Cảnh báo thời tiết:**\n" + "\n".join(warnings)
            
        return response_text
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        
        # Fallback response if API call fails
        fallback = "Xin lỗi, tôi không thể tạo câu trả lời chi tiết lúc này. Dưới đây là dữ liệu thời tiết:\n\n"
        fallback += weather_info
        
        # Add warnings to fallback response
        if warnings:
            fallback += "\n\n**Cảnh báo thời tiết:**\n" + "\n".join(warnings)
            
        return fallback

def process_question(question):
    """Process a user question and return a response"""
    # Extract entities from the question
    entities = extract_entities_with_chatgpt(question)
    print(f"Extracted entities: {entities}")
    
    # If no location is found, ask for clarification
    if not entities["locations"]:
        return "Vui lòng chỉ định một địa điểm cụ thể ở Việt Nam để tôi có thể cung cấp thông tin thời tiết."
    
    # Generate and execute query
    query = generate_influxdb_query(entities)
    print(f"Generated query: {query}")
    
    weather_data = execute_query(query)
    print(f"Query results: {weather_data}")
    
    # Format data
    formatted_data = format_weather_data(weather_data)
    print(f"Formatted data: {formatted_data}")
    
    # Generate response
    response = generate_response_with_chatgpt(question, formatted_data)
    return response

@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

# Initialize the alert system when the app starts
alert_system = None

if __name__ == '__main__':
    # Import weather_alerts module sau khi app đã được tạo để tránh circular import
    import weather_alerts
    
    # Initialize the alert system
    alert_system = weather_alerts.init_alert_system(socketio)
    
    # Run the app with Socket.IO
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
