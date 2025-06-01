import os
import json
import datetime
import uuid
from flask import Flask, request, jsonify, render_template, session
from influxdb_client import InfluxDBClient
import openai
import pytz
import sys
import requests
from flask_socketio import SocketIO

# Add parent directory to path to import provinces
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from provinces import VIETNAM_PROVINCES
from location_mapping import get_english_location_name

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24).hex())  # Secret key for sessions
socketio = SocketIO(app, cors_allowed_origins="*")

# Dictionary to store session IDs for clients
user_sessions = {}

# Initialize OpenAI API key
openai.api_key = os.environ.get("OPENAI_API_KEY", "")

# External knowledge API URL
EXTERNAL_KNOWLEDGE_API_URL = "https://n8n.vdx.vn/webhook/6b2af131-7902-44f6-98e2-1ee4203e2934/chat"

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
    client_id = request.json.get('clientId', request.remote_addr)  # Use client IP if no clientId provided
    
    if not user_question:
        return jsonify({"response": "Vui lòng nhập câu hỏi về thời tiết."})
    
    # Generate or retrieve session ID for this client
    if client_id not in user_sessions:
        # Create a new session ID for this client
        user_sessions[client_id] = str(uuid.uuid4())
        print(f"Created new session ID for client {client_id}: {user_sessions[client_id]}")
    
    session_id = user_sessions[client_id]
    print(f"Processing question with sessionId: {session_id} for client: {client_id}")
    
    response = process_question(user_question, session_id)
    return jsonify({"response": response, "sessionId": session_id})

def is_general_knowledge_question(question):
    """Determine if the question is about general knowledge rather than specific weather data"""
    prompt = f"""
    Phân tích câu hỏi sau và xác định xem nó là câu hỏi về dữ liệu thời tiết cụ thể hay là câu hỏi kiến thức chung về thời tiết/môi trường/sức khỏe.
    
    Câu hỏi: "{question}"
    
    Câu hỏi về dữ liệu thời tiết cụ thể là những câu hỏi yêu cầu thông tin về:
    - Nhiệt độ, độ ẩm, gió, mây, mưa, tia UV ở một địa điểm cụ thể
    - Chất lượng không khí, chỉ số bụi mịn (PM2.5, PM10) ở một địa điểm cụ thể
    - Dự báo thời tiết cho những ngày tới ở một địa điểm cụ thể
    
    Câu hỏi kiến thức chung là những câu hỏi về:
    - Lời khuyên về sức khỏe liên quan đến thời tiết (ví dụ: làm gì khi trời nóng, cách phòng tránh sốc nhiệt)
    - Tác động của ô nhiễm không khí đến sức khỏe
    - Giải thích về hiện tượng thời tiết
    - Các biện pháp phòng tránh tác hại của thời tiết xấu
    - Kiến thức chung về môi trường, biến đổi khí hậu
    - Tất cả những tri thức khác không liên quan tới thời tiết 
    
    Trả về một đối tượng JSON với cấu trúc như sau:
    {{
        "is_general_knowledge": true/false,  // true nếu là câu hỏi kiến thức chung, false nếu là câu hỏi về dữ liệu thời tiết cụ thể
        "reason": "Lý do ngắn gọn"  // Giải thích ngắn gọn lý do phân loại
    }}
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý hữu ích phân loại câu hỏi về thời tiết."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=300
        )
        
        result_text = response['choices'][0]['message']['content'].strip()
        
        # Extract JSON from the response
        try:
            # Find JSON in the response
            json_start = result_text.find('{')
            json_end = result_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = result_text[json_start:json_end]
                result = json.loads(json_str)
                return result
            else:
                # Default to treating as weather data question if parsing fails
                return {"is_general_knowledge": False, "reason": "Failed to parse response"}
        except json.JSONDecodeError:
            return {"is_general_knowledge": False, "reason": "JSON decode error"}
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return {"is_general_knowledge": False, "reason": f"API error: {str(e)}"}

def forward_to_external_api(question, session_id=None):
    """Forward the question to the external knowledge API and return the response"""
    try:
        # Create a payload with chatInput and sessionId (if provided)
        payload = {
            "chatInput": question
        }
        
        # Add sessionId to the payload if provided
        if session_id:
            payload["sessionId"] = session_id
        
        print(f"Sending to external API: {payload}")
        response = requests.post(EXTERNAL_KNOWLEDGE_API_URL, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        data = response.json()
        if 'output' in data:
            return data['output']
        else:
            return "Xin lỗi, tôi không thể trả lời câu hỏi này lúc này. Vui lòng thử lại sau."
    except Exception as e:
        print(f"Error calling external API: {e}")
        return f"Xin lỗi, có lỗi khi xử lý câu hỏi của bạn: {str(e)}"

def extract_entities_with_chatgpt(question):
    """Use ChatGPT to extract location, time information, and specific weather field from the question"""
    
    prompt = f"""
    Trích xuất thông tin về địa điểm, thời gian, dự báo và trường dữ liệu cụ thể từ câu hỏi sau về thời tiết ở Việt Nam.
    Câu hỏi: "{question}"
    
    Các địa điểm hợp lệ là 63 tỉnh thành của Việt Nam: {', '.join(VIETNAM_PROVINCES)}
    
    Các trường dữ liệu thời tiết có thể được hỏi:
    - temp_c: nhiệt độ (độ C), từ khóa: nhiệt độ, nóng, lạnh, bao nhiêu độ
    - pm2_5: chỉ số bụi mịn PM2.5, từ khóa: pm2.5, pm2_5, bụi mịn 2.5, chất lượng không khí
    - pm10: chỉ số bụi PM10, từ khóa: pm10, bụi mịn 10, chất lượng không khí
    - cloud: độ che phủ mây (%), từ khóa: mây, độ che phủ mây, trời nhiều mây, trời ít mây
    - humidity: độ ẩm (%), từ khóa: độ ẩm, ẩm ướt, khô ráo
    - uv: chỉ số tia cực tím, từ khóa: tia uv, tia cực tím, chỉ số uv
    
    Trả về một đối tượng JSON với cấu trúc ví dụ như sau:
    {{
        "locations": ["Địa điểm 1", "Địa điểm 2"], // Danh sách các địa điểm được đề cập trong câu hỏi
        "time_reference": "current", // Một trong các giá trị: current (hiện tại), today (hôm nay), yesterday (hôm qua), specific_date (ngày cụ thể), future (tương lai)
        "is_forecast": false, // true nếu câu hỏi liên quan đến dự báo thời tiết trong tương lai, false nếu hỏi về thời tiết hiện tại hoặc quá khứ
        "specific_fields": ["temp_c"] // Danh sách các trường dữ liệu cụ thể được hỏi đến, có thể là temp_c, pm2_5, pm10, hoặc rỗng nếu hỏi chung về thời tiết
    }}
    
    Nếu không tìm thấy địa điểm hợp lệ, trả về danh sách trống cho locations.
    Nếu không tìm thấy tham chiếu thời gian, giả định là "current".
    Nếu câu hỏi chứa các từ khóa như "dự báo", "dự đoán", "sẽ", "mai", "tuần tới", "ngày mai", "sắp tới", "sắp", "tới", "sẽ như thế nào", "thế nào", "ra sao", hoặc các từ ngữ tương tự về thời tiết trong tương lai, hãy đặt is_forecast = true và time_reference = "future".
    Nếu câu hỏi không đề cập đến trường dữ liệu cụ thể nào, trả về danh sách trống cho specific_fields.
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý hữu ích trích xuất thông tin địa điểm và thời gian từ các câu hỏi về thời tiết."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=500
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
    is_forecast = entities.get("is_forecast", False)
    specific_fields = entities.get("specific_fields", [])
    
    # Determine which bucket to use based on whether this is a forecast query
    bucket = "weather_forecast" if is_forecast else "weather"
    
    # Generate query with the appropriate bucket
    query = f"""
    from(bucket: "{bucket}")
    """
    
    # Add time range based on reference
    if is_forecast or time_reference == "future":
        # For forecast data, get the next 7 days
        query += """
        |> range(start: now(), stop: now() + 7d)
        """
    elif time_reference == "current" or time_reference == "now":
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
    
    # Filter for specific fields if requested
    if specific_fields:
        field_filters = []
        for field in specific_fields:
            field_filters.append(f'r._field == "{field}"')
        
        field_filter_str = " or ".join(field_filters)
        query += f"""
    |> filter(fn: (r) => {field_filter_str})
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
    
    # Group data by location and time
    grouped_data = {}
    for item in weather_data:
        location = item.get("location", "Unknown")
        time = item.get("time")
        field = item.get("field")
        value = item.get("value")
        
        # Format time as string
        time_str = time.strftime("%Y-%m-%d %H:%M:%S") if time else "Unknown"
        
        if location not in grouped_data:
            grouped_data[location] = {}
            
        if time_str not in grouped_data[location]:
            grouped_data[location][time_str] = {}
        
        # Store the field value
        grouped_data[location][time_str][field] = value
    
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

def get_uv_warning(uv):
    """Generate warning message based on UV index"""
    if uv is None:
        return None
    
    try:
        uv_val = float(uv)
        
        if 0 <= uv_val <= 2:
            return "✅ **UV: Thấp** — An toàn cho hầu hết mọi người. Có thể ra ngoài mà không cần bảo vệ đặc biệt."
        elif 3 <= uv_val <= 5:
            return "⚠️ **UV: Trung bình** — Nên tìm bóng râm vào giữa trưa. Sử dụng kem chống nắng SPF 30+ khi ra ngoài."
        elif 6 <= uv_val <= 7:
            return "⚠️ **UV: Cao** — Giảm thời gian ở ngoài từ 10h-16h. Đeo kính râm, mũ rộng vành và áo dài tay."
        elif 8 <= uv_val <= 10:
            return "⚠️ **UV: Rất cao** — Tránh ra ngoài vào giữa trưa. Bắt buộc sử dụng kem chống nắng, mũ và kính râm."
        elif uv_val >= 11:
            return "⚠️ **UV: Cực kỳ cao** — Nguy cơ tổn thương da cao. Hạn chế tối đa thời gian ngoài trời. Sử dụng đầy đủ các biện pháp bảo vệ."
        else:
            return None
    except (ValueError, TypeError):
        return None

def generate_response_with_chatgpt(question, weather_data):
    """Generate a natural language response using ChatGPT"""
    if not weather_data:
        return "Xin lỗi, tôi không tìm thấy dữ liệu thời tiết cho địa điểm và thời gian bạn yêu cầu."
    
    # Determine if this is forecast data by checking for forecast-specific fields
    is_forecast = False
    for location_data in weather_data.values():
        for time_data in location_data.values():
            if 'max_temp' in time_data or 'min_temp' in time_data or 'condition' in time_data:
                is_forecast = True
                break
    
    # Format weather data for the prompt
    weather_info = ""
    warnings = []
    
    for location, data in weather_data.items():
        weather_info += f"Thời tiết tại {location}:\n"
        
        for time, measurements in data.items():
            weather_info += f"  Thời gian: {time}\n"
            
            for key, value in measurements.items():
                if key == 'temp_c' or key == 'max_temp':
                    temp_label = 'Nhiệt độ' if key == 'temp_c' else 'Nhiệt độ cao nhất'
                    weather_info += f"  {temp_label}: {value}°C\n"
                    
                    # Add temperature warning if needed
                    warning = get_temperature_warning(value)
                    if warning:
                        warnings.append(f"*{location}*: {warning}")
                
                elif key == 'min_temp':
                    weather_info += f"  Nhiệt độ thấp nhất: {value}°C\n"
                        
                elif key == 'humidity':
                    weather_info += f"  Độ ẩm: {value}%\n"
                elif key == 'wind_kph':
                    weather_info += f"  Tốc độ gió: {value} km/h\n"
                elif key == 'precip_mm' or key == 'rainfall':
                    rain_label = 'Lượng mưa' if key == 'precip_mm' else 'Lượng mưa dự báo'
                    weather_info += f"  {rain_label}: {value} mm\n"
                elif key == 'cloud' or key == 'cloud_cover':
                    weather_info += f"  Độ che phủ mây: {value}%\n"
                elif key == 'condition':
                    weather_info += f"  Điều kiện thời tiết: {value}\n"
                elif key == 'pm2_5':
                    weather_info += f"  PM2.5: {value} µg/m³\n"
                    
                    # Add air quality warning if needed
                    warning = get_pm25_warning(value)
                    if warning:
                        warnings.append(f"*{location}*: {warning}")
                        
                elif key == 'pm10':
                    weather_info += f"  PM10: {value} µg/m³\n"
                    
                    # Add air quality warning if needed
                    warning = get_pm10_warning(value)
                    if warning:
                        warnings.append(f"*{location}*: {warning}")
                        
                elif key == 'humidity':
                    weather_info += f"  Độ ẩm: {value}%\n"
                        
                elif key == 'cloud':
                    weather_info += f"  Độ che phủ mây: {value}%\n"
                        
                elif key == 'uv':
                    weather_info += f"  Chỉ số UV: {value}\n"
                    
                    # Add UV warning if needed
                    warning = get_uv_warning(value)
                    if warning:
                        warnings.append(f"*{location}*: {warning}")
            
            weather_info += "\n"
    
    # Add warnings to the prompt if any
    warnings_text = ""
    if warnings:
        warnings_text = "\n".join(warnings)
    
    # Determine which specific fields were requested based on the data available
    available_fields = set()
    for location, data in weather_data.items():
        for time, measurements in data.items():
            for key in measurements.keys():
                available_fields.add(key)
    
    # Check if this is a general weather query or a specific field query
    entities = extract_entities_with_chatgpt(question)
    specific_fields = entities.get("specific_fields", [])
    is_general_query = len(specific_fields) == 0
    
    # Create a prompt based on whether this is a general query or specific query
    prompt = f"""
    Dựa trên câu hỏi của người dùng và dữ liệu thời tiết được cung cấp, hãy tạo một câu trả lời ngắn gọn, súc tích và chính xác.
    
    Câu hỏi: "{question}"
    
    Dữ liệu thời tiết:
    {weather_info}
    {warnings_text}
    
    Trả lời bằng tiếng Việt, chỉ cung cấp thông tin thời tiết được yêu cầu một cách ngắn gọn và trực tiếp.
    """
    
    if is_general_query:
        # For general queries, provide concise information about all available fields
        prompt += "\nCâu hỏi này là về thời tiết nói chung, hãy cung cấp thông tin ngắn gọn về tất cả các khía cạnh có trong dữ liệu."
        
        # No additional instructions for explanations or advice
    else:
        # For specific field queries, focus on the requested fields
        prompt += "\nQUAN TRỌNG: Câu hỏi này chỉ hỏi về một hoặc một số trường dữ liệu cụ thể. Chỉ trả lời về các trường dữ liệu được hỏi đến. Không đề cập đến các thông tin khác."
        
        # List the specific fields being requested without additional explanations
        if specific_fields:
            prompt += f"\nCác trường dữ liệu được yêu cầu: {', '.join(specific_fields)}"
    
    # Add warning instructions if relevant
    if warnings:
        prompt += "\nĐưa ra cảnh báo thời tiết ngắn gọn dựa trên dữ liệu được cung cấp."
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý thời tiết cung cấp thông tin thời tiết chính xác, ngắn gọn và đúng trọng tâm. Chỉ cung cấp dữ liệu thời tiết được yêu cầu mà không đưa ra lời khuyên hay giải thích thêm. Nếu có cảnh báo thời tiết, hãy đề cập ngắn gọn."},
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

def process_question(question, session_id=None):
    """Process a user question and return a response"""
    # First, determine if this is a general knowledge question
    classification = is_general_knowledge_question(question)
    print(f"Question classification: {classification}")
    
    # If it's a general knowledge question, forward it to the external API
    if classification.get("is_general_knowledge", False):
        print(f"Forwarding general knowledge question to external API: {question}")
        return forward_to_external_api(question, session_id)
    
    # Otherwise, process as a weather data question using the existing flow
    # Extract entities from the question
    entities = extract_entities_with_chatgpt(question)
    print(f"Extracted entities: {entities}")
    
    # If no location is found, ask for clarification
    if not entities["locations"]:
        return "Vui lòng chỉ định một địa điểm cụ thể ở Việt Nam để tôi có thể cung cấp thông tin thời tiết."
    
    # Check if this is a forecast query
    is_forecast = entities.get("is_forecast", False)
    
    if is_forecast:
        # Import forecast handler here to avoid circular imports
        from forecast_handler import get_forecast_data, format_forecast_response, is_date_in_valid_range
        import re
        from datetime import datetime, timedelta
        
        # Determine specific day for forecast based on the question
        specific_day = None
        specific_date = None
        time_reference = entities.get("time_reference", "")
        
        # Check for specific date patterns in the question (dd/mm or dd/mm/yyyy)
        date_patterns = [
            r'(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?',  # dd/mm or dd/mm/yyyy
            r'ngày\s+(\d{1,2})\s+tháng\s+(\d{1,2})(?:\s+năm\s+(\d{2,4}))?'  # ngày dd tháng mm (năm yyyy)
        ]
        
        current_date = datetime.now().date()
        found_specific_date = False
        
        for pattern in date_patterns:
            matches = re.findall(pattern, question)
            if matches:
                for match in matches:
                    try:
                        if len(match) >= 2:
                            day = int(match[0])
                            month = int(match[1])
                            year = int(match[2]) if len(match) > 2 and match[2] else current_date.year
                            
                            # Handle 2-digit year
                            if year < 100:
                                year += 2000
                                
                            specific_date = datetime(year, month, day).date()
                            found_specific_date = True
                            break
                    except (ValueError, IndexError):
                        continue
                if found_specific_date:
                    break
        
        # If specific date found, check if it's in valid range (today to 7 days ahead)
        if found_specific_date:
            if is_date_in_valid_range(specific_date):
                # Calculate days from today
                days_from_today = (specific_date - current_date).days
                if days_from_today == 1:
                    specific_day = 'tomorrow'
                else:
                    specific_day = f'{days_from_today}days'
            else:
                # Date is out of valid range
                locations_str = ", ".join(entities["locations"])
                return f"Xin lỗi, tôi chỉ có thể cung cấp thông tin về thời tiết cho {locations_str} từ ngày hôm nay và dự báo cho 7 ngày tới."
        else:
            # Check for relative day references if no specific date found
            if "ngày mai" in question.lower() or "tomorrow" in time_reference.lower():
                specific_day = 'tomorrow'
            elif any(term in question.lower() for term in ["2 ngày", "hai ngày", "2 hôm"]):
                specific_day = '2days'
            elif any(term in question.lower() for term in ["3 ngày", "ba ngày", "3 hôm"]):
                specific_day = '3days'
            elif any(term in question.lower() for term in ["4 ngày", "bốn ngày", "4 hôm"]):
                specific_day = '4days'
            elif any(term in question.lower() for term in ["5 ngày", "năm ngày", "5 hôm"]):
                specific_day = '5days'
            elif any(term in question.lower() for term in ["6 ngày", "sáu ngày", "6 hôm"]):
                specific_day = '6days'
            elif any(term in question.lower() for term in ["7 ngày", "bảy ngày", "một tuần", "7 hôm"]):
                specific_day = '7days'
            
            # Check for past time references
            if "hôm qua" in question.lower() or "yesterday" in time_reference.lower() or "tuần trước" in question.lower():
                locations_str = ", ".join(entities["locations"])
                return f"Xin lỗi, tôi chỉ có thể cung cấp thông tin về thời tiết cho {locations_str} từ ngày hôm nay và dự báo cho 7 ngày tới."
        
        print(f"Specific day for forecast: {specific_day}")
        print(f"Specific date requested: {specific_date}")
        
        # Get forecast data for the requested locations and specific day
        forecast_data = get_forecast_data(entities["locations"], specific_day=specific_day)
        print(f"Forecast data: {forecast_data}")
        
        # Format the forecast response
        response = format_forecast_response(forecast_data, question)
    else:
        # For current weather, use the existing flow
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
