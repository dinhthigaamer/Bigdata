# Hướng Dẫn Khởi Chạy Tự Động

### Bước 1: Build và khởi động toàn bộ hệ thống

Tại thư mục gốc dự án, chạy lệnh:

```bash
docker compose up -d --build
```

*(Nếu sử dụng file merge riêng, chạy: `docker compose -f docker-compose.merged.yml up -d --build`)*

**Quy trình tự động diễn ra khi khởi chạy:**

1. `kafka` và `clickhouse` khởi động và kiểm tra trạng thái sẵn sàng (`healthcheck`).
2. Container **`serving-init`** tự động chạy để áp dụng toàn bộ DDL trong `serving/clickhouse/init` và nạp dữ liệu danh mục trạm vào bảng `dim_station`.
3. Sau khi `serving-init` hoàn tất (`Exited 0`), **`spark-speed`** và **`grafana`** tự động khởi chạy với đầy đủ schema, kết nối datasource và dashboard sẵn sàng *(mặc định sử dụng tài khoản ClickHouse `bigdata` / `bigdata123`)*.

### Bước 2: Kiểm tra trạng thái các container

```bash
docker compose ps -a
```

* **Trạng thái chuẩn:** `kafka`, `clickhouse`, `spark-speed`, `grafana` ở trạng thái `Up` / `healthy`; riêng `serving-init` hiển thị `Exited (0)`.

### Bước 3: Truy cập giao diện Grafana

1. Mở trình duyệt web và truy cập địa chỉ: `http://localhost:3000`
2. Đăng nhập bằng tài khoản mặc định:
   * **Username:** `admin`
   * **Password:** `admin`
3. Sau khi đăng nhập, dashboard mặc định (đã kết nối sẵn nguồn dữ liệu ClickHouse) sẽ tự động hiển thị trên trang chủ, hoặc bạn có thể vào mục **Dashboards** ở thanh menu bên trái để xem các biểu đồ.