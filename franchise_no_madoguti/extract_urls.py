import csv

input_file = "all_urls.csv"
output_file = "urls.csv"

filtered = []

with open(input_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        url = row[0]  # URLが1列目にある前提
        if "detail" in url:
            filtered.append([url])  # URLのみ書き出す

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(filtered)

print(f"抽出完了: {len(filtered)} 件を {output_file} に保存しました。")
