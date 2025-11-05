# filename: scrape.py
import csv
import datetime
import re
import sys
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup, Tag

REQUEST_TIMEOUT = 30
DEFAULT_MAX_WORKERS = 10  # 並列数のデフォルト


def fetch_html(url: str) -> Tuple[str, str]:
    """
    指定URLのHTMLを取得して (最終URL, HTMLテキスト) を返す
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    # 文字化け対策
    if resp.encoding is None or resp.encoding.lower() in ("iso-8859-1", "latin-1"):
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.url, resp.text


def find_recruit_company_table(soup: BeautifulSoup) -> Optional[Tag]:
    """
    ページ内の「募集企業」セクション直下のテーブルを探して返す
    """
    heading_candidates = []
    for hn in ["h2", "h3", "h4", "h5", "p", "div"]:
        for el in soup.find_all(hn):
            text = el.get_text(strip=True)
            if "募集企業" in text:
                heading_candidates.append(el)

    for heading in heading_candidates:
        # 親要素内を優先的に検索
        parent = heading.parent
        if parent:
            table_in_parent = parent.find("table")
            if table_in_parent:
                return table_in_parent

        # 後続の兄弟を走査
        sib = heading
        while True:
            sib = sib.next_sibling
            if sib is None:
                break
            if isinstance(sib, Tag):
                if sib.name == "table":
                    return sib
                nested = sib.find("table") if hasattr(sib, "find") else None
                if nested:
                    return nested

    # フォールバック: キー/バリュー形式っぽいテーブル
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if not first_row:
            continue
        cells = first_row.find_all(["td", "th"])
        if len(cells) >= 2:
            header_texts = " ".join(c.get_text(strip=True) for c in cells[:2])
            if ("名称" in header_texts) or ("住所" in header_texts):
                return table
    return None


def parse_company_table(table: Tag) -> Dict[str, str]:
    """
    2列のキー/バリュー形式テーブルを辞書にして返す（日本語ラベルを正規化）
    """
    data: Dict[str, str] = {}
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        key = cells[0].get_text(separator=" ", strip=True)
        val = cells[1].get_text(separator=" ", strip=True)

        # 空白を正規化
        val = re.sub(r"\s+", " ", val).strip()

        key_map = {
            "名称": "名称",
            "住所": "住所",
            "TEL": "TEL",
            "電話": "TEL",
            "設立": "設立",
            "資本金": "資本金",
            "年商": "年商",
            "部署": "部署",
            "従業員": "従業員",
            "事業": "事業",
        }
        norm_key = key
        for k in key_map.keys():
            if k in norm_key:
                norm_key = key_map[k]
                break

        data[norm_key] = val

    # TELが無い場合は空欄を補完
    if "TEL" not in data:
        data["TEL"] = ""

    return data


def build_row(url: str, parsed: Dict[str, str]) -> Dict[str, str]:
    """
    出力行（指定カラム）を構築
    """
    # JST (UTC+9)
    now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    row = {
        "取得日時": now_jst.strftime("%Y/%m/%d %H:%M:%S"),
        "取得URL": url,
        "名称": parsed.get("名称", ""),
        "住所": parsed.get("住所", ""),
        "TEL": parsed.get("TEL", ""),
        "設立": parsed.get("設立", ""),
        "資本金": parsed.get("資本金", ""),
        "年商": parsed.get("年商", ""),
        "部署": parsed.get("部署", ""),
        "従業員": parsed.get("従業員", ""),
        "事業": parsed.get("事業", ""),
    }
    return row


def scrape_company_info_single(url: str) -> List[Dict[str, str]]:
    """
    単一ページから会社情報行を抽出して返す
    """
    final_url, html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    rows: List[Dict[str, str]] = []

    main_table = find_recruit_company_table(soup)
    if main_table:
        parsed = parse_company_table(main_table)
        rows.append(build_row(final_url, parsed))
        return rows

    # フォールバック: 名称/住所が含まれるテーブルを全て収集
    for table in soup.find_all("table"):
        parsed = parse_company_table(table)
        if parsed.get("名称") or parsed.get("住所"):
            rows.append(build_row(final_url, parsed))

    return rows


def read_urls_from_csv(path: str) -> List[str]:
    """
    all_urls.csvからURLリストを読み込む
    - 1列CSV（ヘッダ有無どちらでも可）
    - 複数列なら先頭列をURLとして扱う
    """
    urls: List[str] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            candidate = row[0].strip()
            if not candidate:
                continue
            # 先頭行がhttpで始まらなければヘッダとみなしてスキップ
            if len(urls) == 0 and not candidate.lower().startswith("http"):
                continue
            urls.append(candidate)
    return urls


def save_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    """
    固定カラム順でCSV保存（UTF-8 BOM; Excel対策）
    """
    fieldnames = [
        "取得日時",
        "取得URL",
        "名称",
        "住所",
        "TEL",
        "設立",
        "資本金",
        "年商",
        "部署",
        "従業員",
        "事業",
    ]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def process_url(url: str) -> List[Dict[str, str]]:
    """
    単一URLのラッパー（例外処理込み）。失敗時は空リストを返す
    """
    try:
        return scrape_company_info_single(url)
    except requests.HTTPError as e:
        print(f"[HTTPError] {url}: {e}")
    except requests.Timeout:
        print(f"[Timeout] {url}: request timed out")
    except Exception as e:
        print(f"[Error] {url}: {e}")
    return []


def main():
    if len(sys.argv) < 2:
        print("Usage: python scrape.py <all_urls.csv> [out.csv] [max_workers]")
        print("Example:")
        print("  python scrape.py all_urls.csv company_info_all.csv 16")
        sys.exit(1)

    in_csv = sys.argv[1]
    out_csv = sys.argv[2] if len(sys.argv) >= 3 else "company_info_all.csv"
    max_workers = int(sys.argv[3]) if len(sys.argv) >= 4 else DEFAULT_MAX_WORKERS

    urls = read_urls_from_csv(in_csv)
    if not urls:
        print("No URLs found in the input CSV.")
        sys.exit(1)

    all_rows: List[Dict[str, str]] = []

    # 並列スクレイピング
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(process_url, url): url for url in urls}
        for future in as_completed(future_map):
            url = future_map[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                # process_url内で未捕捉の例外が発生した場合のみここに来る
                print(f"[FutureError] {url}: {e}")

    save_csv(all_rows, out_csv)
    print(f"Processed {len(urls)} URL(s). Saved {len(all_rows)} row(s) to {out_csv}")


if __name__ == "__main__":
    main()
