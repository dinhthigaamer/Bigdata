# Serving layer

**`/serving/clickhouse/init`**: Khởi tạo các bảng khi hệ thống bắt đầu chạy, các bên vào bảng tương ứng với layer của mình để kiểm tra Schema

**`/serving/grafana`**: Cấu hình cho Grafana

**`/serving/ch_writer`**: Thư viện để ghi clickhouse, dùng cho các layer khác.

# Khởi tạo serving layer:

```bash
# 1. Nếu volume đã tồn tại: áp DDL thủ công
pip install -r libs/ch_writer/requirements.txt
cd libs && python -m ch_writer.ch_writer ../clickhouse/init

# 2. Load dữ liệu tĩnh vào bảng dim_station:
cd ../..
python -m serving.boostrap.load_dim_station
```



# Hướng Dẫn Nhanh Grafana & ClickHouse

## 1. Khởi chạy & Đăng nhập Grafana

1. Khởi động container tại thư mục dự án:
   ```bash
   docker compose up -d
   ```
2. Truy cập `http://localhost:3000` và đăng nhập bằng `GF_USER` / `GF_PASSWORD` trong file `.env` (mặc định: `admin` / `admin`).

---

## 2. Kết nối ClickHouse (Data Source)

1. Vào **Connections** > **Data sources** > **Add new data source** > chọn **ClickHouse**.
2. Điền thông số kết nối (môi trường Docker):
   * **Server address:** `clickhouse` *(tên service trong `docker-compose.yml`, không ghi `http://` hay port)*
   * **Server port:** `8123` *(hoặc `9000`)*
   * **Protocol:** `HTTP` *(nếu port `8123`)* hoặc `Native` *(nếu port `9000`)*
   * **Username:** `bigdata` *(hoặc `CH_USER` trong `.env`)*
   * **Password:** `bigdata123` *(hoặc `CH_PASSWORD` trong `.env`)*
   * **Default database:** Tên database dự án
3. Bấm **Save & test** $\rightarrow$ Thấy thông báo xanh *"Data source is working"* là xong.

---

## 3. Tạo Dashboard & Biểu đồ

1. Vào **Dashboards** > **New** > **New dashboard** > **+ Add visualization** > chọn **ClickHouse**.
2. Chuyển chế độ truy vấn sang **SQL Editor** (góc trên bên phải khung truy vấn) và nhập câu lệnh SQL:
   ```sql
   SELECT
       $__timeInterval(timestamp) AS time,
       entity_id,
       avg(metric_value) AS value
   FROM database_name.table_name
   WHERE $__timeFilter(timestamp)
   GROUP BY time, entity_id
   ORDER BY time ASC
   ```
3. Bấm **Run query**, chọn kiểu biểu đồ ở cột bên phải (**Time series**, **Geomap**, **Table**...), đặt **Title**, rồi bấm **Apply**.
4. Chọn khung thời gian và chu kỳ tự động làm mới (**Auto-refresh**: `5s`, `10s`) ở góc trên bên phải Dashboard $\rightarrow$ bấm biểu tượng **Save dashboard** (hình đĩa mềm) để lưu.
