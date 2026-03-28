from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def home():
    # Phản hồi bằng định dạng JSON để kiểm tra API
    return jsonify({
        "status": "success",
        "message": "Hello từ Flask! Dịch vụ đang chạy trên Python 3.12."
    })

if __name__ == '__main__':
    # host='0.0.0.0' cho phép các container khác trong mạng Docker có thể gọi API này
    app.run(host='0.0.0.0', port=5000)