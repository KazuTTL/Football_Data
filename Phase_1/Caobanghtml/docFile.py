import duckdb

con = duckdb.connect(r'C:\Users\Tien Loc\Downloads\transfermarkt-datasets.duckdb')

print("--- Danh sách các bảng ---")
tables = con.execute("SHOW TABLES").fetchall()
if not tables:
    print("Database trống, chưa có bảng nào.")
else:
    for table in tables:
        print(f"Bảng: {table[0]}")

    # 3. Thử lấy dữ liệu từ một bảng (Ví dụ bảng 'players')
    # Thay 'players' bằng tên bảng bạn thấy ở bước trên
    print("\n--- Dữ liệu 5 dòng đầu tiên ---")
    df = con.execute("SELECT * FROM players LIMIT 5").df()
    print(df)

# Đóng kết nối
con.close()