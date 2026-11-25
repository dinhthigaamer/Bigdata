"""
Drop bảng ClickHouse rồi khởi tạo lại từ các file DDL.

Ví dụ (chạy từ thư mục gốc btl_bigdata):
  python -m serving.boostrap.reset_db --tables dim_station
  python -m serving.boostrap.reset_db --tables dim_station station_latest
  python -m serving.boostrap.reset_db --all-tables
  python -m serving.boostrap.reset_db --drop-database
  python -m serving.boostrap.reset_db --tables dim_station --yes   # bỏ qua bước hỏi xác nhận
"""
import argparse
import sys
from pathlib import Path

import clickhouse_connect

from serving.libs.ch_writer.ch_writer import apply_ddl

HOST = "localhost"
PORT = 8123
USER = "bigdata"
PASSWORD = "bigdata123"
DATABASE = "bigdata"

# serving/boostrap/reset_db.py -> serving/clickhouse/init
DDL_DIR = Path(__file__).resolve().parents[1] / "clickhouse" / "init"


def get_client():
    # Không truyền database để vẫn kết nối được khi database chưa tồn tại
    return clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD
    )


def list_tables(client) -> list[str]:
    rows = client.query(
        "SELECT name FROM system.tables WHERE database = {db:String}",
        parameters={"db": DATABASE},
    ).result_rows
    return [r[0] for r in rows]


def main():
    ap = argparse.ArgumentParser(description="Drop bảng rồi chạy lại DDL")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--tables", nargs="+", help="Tên các bảng cần drop")
    g.add_argument("--all-tables", action="store_true",
                   help="Drop mọi bảng trong database")
    g.add_argument("--drop-database", action="store_true",
                   help="Drop cả database")
    ap.add_argument("--ddl-dir", default=str(DDL_DIR),
                    help="Thư mục chứa file .sql")
    ap.add_argument("--no-recreate", action="store_true",
                    help="Chỉ drop, không tạo lại")
    ap.add_argument("-y", "--yes", action="store_true",
                    help="Không hỏi xác nhận")
    args = ap.parse_args()

    client = get_client()

    # Xác định việc sẽ làm
    if args.drop_database:
        targets = [f"DATABASE {DATABASE}"]
    elif args.all_tables:
        targets = [f"TABLE {DATABASE}.{t}" for t in list_tables(client)]
    else:
        targets = [f"TABLE {DATABASE}.{t}" for t in args.tables]

    if not targets:
        print("Không có gì để drop.")
    else:
        print("Sẽ DROP:")
        for t in targets:
            print(f"  - {t}")

        if not args.yes and input("Tiếp tục? (gõ 'yes'): ").strip().lower() != "yes":
            print("Đã hủy.")
            sys.exit(0)

        for t in targets:
            client.command(f"DROP {t} SYNC")   # SYNC: xóa xong mới trả về
            print(f"đã drop {t}")

    if args.no_recreate:
        return

    print(f"Chạy lại DDL từ {args.ddl_dir}")
    apply_ddl(args.ddl_dir)

    print("Các bảng hiện có:", list_tables(get_client()))


if __name__ == "__main__":
    main()
