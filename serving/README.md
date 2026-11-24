# Serving layer

**`/serving/clickhouse/init`**: Khởi tạo các bảng khi hệ thống bắt đầu chạy, các bên vào bảng tương ứng với layer của mình để kiểm tra Schema

**`/serving/grafana`**: Cấu hình cho Grafana

**`/serving/ch_writer`**: Thư viện để ghi clickhouse, dùng cho các layer khác.

# Khởi tạo serving layer:


```bash
# 1. Nếu volume đã tồn tại: áp DDL thủ công
pip install -r libs/ch_writer/requirements.txt
cd libs && python -m ch_writer.ch_writer ../clickhouse/init

# 2. Smoke test thư viện ghi
python -m ch_writer.example_job_a
```
