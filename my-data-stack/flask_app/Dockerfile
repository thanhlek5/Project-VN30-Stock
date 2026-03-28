# Sử dụng hình ảnh Python 3.12 gọn nhẹ
FROM python:3.12-slim

# Chuyển đến thư mục làm việc bên trong container
WORKDIR /app

# Khai báo và cài đặt thư viện
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn vào container
COPY . .

# Mở cổng 5000 để giao tiếp
EXPOSE 5000

# Lệnh khởi chạy ứng dụng khi container bắt đầu
CMD ["python", "app.py"]