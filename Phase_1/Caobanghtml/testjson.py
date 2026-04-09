import json

with open(r"C:\Users\Tien Loc\Downloads\sofascore_big5_clean_20260406_1033.json", "r", encoding="utf-8") as f:
    sofa_data = json.load(f)

# Lấy ra bản ghi đầu tiên trong mảng 'data'
first_item = sofa_data.get('data', [])[0]

# json.dumps với indent=4 giúp in JSON ra dưới dạng cây thư mục (Pretty Print)
print(json.dumps(first_item, indent=4, ensure_ascii=False))