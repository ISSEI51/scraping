import csv

input_file = "all_urls.csv"
output_file = "urls.csv"

filtered_rows = []

with open(input_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        url = row[0]  # 1列目にURLがある前提
        if url.count("/") == 3:
            filtered_rows.append([url])

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(filtered_rows)

print(f"抽出完了: {len(filtered_rows)} 件を {output_file} に保存しました")
