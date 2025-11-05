# scrape.py
import csv
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 25
MAX_WORKERS = 8
RETRY_COUNT = 2
RETRY_BACKOFF_SEC = 2.0

KNOWN_FIELD_MAP = {
    "会社名": "名称",
    "社名": "名称",
    "所在地": "住所",
    "住所": "住所",
    "設立": "設立",
    "代表者": "代表者",
    "資本金": "資本金",
    "事業内容": "事業内容",
    "電話": "電話",
    "TEL": "電話",
    "メール": "メール",
    "E-mail": "メール",
}

REQUIRED_COLUMNS = ["取得日時", "取得URL", "名称", "住所"]


def now_jst_iso() -> str:
    return datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")


def fetch_html(url: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    HTMLを取得して返す。戻り値は (html, status_code, error_message)。
    - 成功: (html, 200, None)
    - 失敗: (None, status_code or None, error_message)
    """
    for attempt in range(RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
            )
            status = resp.status_code

            # 4xx はリトライせず即終了
            if 400 <= status < 500:
                return None, status, f"HTTP {status}"

            # 5xx はリトライ対象
            resp.raise_for_status()

            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text, status, None

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status and 400 <= status < 500:
                return None, status, f"HTTP {status}"
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
            else:
                return None, status, f"HTTP {status}" if status else str(e)

        except Exception as e:
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
            else:
                return None, None, str(e)

    return None, None, "unknown error"


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def parse_company_table(soup: BeautifulSoup) -> Dict[str, str]:
    """
    ページ内の「会社情報」見出しの直後テーブル、なければ全テーブルから
    2列（th/td, dt/dd）ペアを抽出し、既知のキーは正規化して返す。
    """
    heading = None
    for tag in soup.find_all(["h2", "h3"]):
        text = normalize_text(tag.get_text())
        if "会社情報" in text:
            heading = tag
            break

    tables: List = []
    if heading:
        nxt = heading.find_next(["table"])
        if nxt:
            tables.append(nxt)
    tables.extend(soup.find_all("table"))

    result: Dict[str, str] = {}
    for table in tables:
        for tr in table.find_all("tr"):
            th = tr.find(["th", "dt"])
            td = tr.find(["td", "dd"])
            if not th or not td:
                continue
            key_raw = normalize_text(th.get_text())
            val_raw = normalize_text(td.get_text())
            if not key_raw:
                continue

            val_raw = val_raw.replace("\r", "").replace("\n", " ").strip()

            normalized_key = None
            for k, colname in KNOWN_FIELD_MAP.items():
                if k in key_raw:
                    normalized_key = colname
                    break
            if normalized_key is None:
                normalized_key = key_raw

            prev = result.get(normalized_key, "")
            if len(val_raw) > len(prev):
                result[normalized_key] = val_raw

        # 名称・住所が両方そろったら十分と判断して早期終了
        if "名称" in result and "住所" in result:
            break

    return result


def extract_record(url: str) -> Optional[Dict[str, str]]:
    """
    1URLからレコードを抽出。
    - 名称が空の場合は None を返してスキップ
    - 失敗（404等）も None を返してスキップ
    """
    html, status, err = fetch_html(url)
    if html is None:
        print(
            f"[WARN] fetch failed, skip url={url} status={status} err={err}",
            file=sys.stderr,
        )
        return None

    try:
        soup = BeautifulSoup(html, "html.parser")
        table_data = parse_company_table(soup)

        name = table_data.get("名称", "").strip()
        address = table_data.get("住所", "").strip()

        if not name:
            print(f"[INFO] no company name, skip url={url}", file=sys.stderr)
            return None

        record: Dict[str, str] = {
            "取得日時": now_jst_iso(),
            "取得URL": url,
            "名称": name,
            "住所": address,
        }

        # その他カラム: 必須以外を全部取り込む
        for k, v in table_data.items():
            if k in REQUIRED_COLUMNS:
                continue
            record[k] = v

        return record

    except Exception:
        traceback.print_exc(file=sys.stderr)
        return None


def read_urls(csv_path: str) -> List[str]:
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            return urls
        # 先頭セルがURLでなければヘッダとみなしてスキップ
        start_idx = 0
        first_cell = rows[0][0] if rows[0] else ""
        if not first_cell.startswith("http"):
            start_idx = 1
        for row in rows[start_idx:]:
            if not row:
                continue
            url = row[0].strip()
            if url:
                urls.append(url)
    return urls


def write_csv(output_path: str, records: List[Dict[str, str]]) -> None:
    # ヘッダは必須 + その他カラム（出現順）で重複なく作成
    header: List[str] = list(REQUIRED_COLUMNS)
    seen = set(header)
    for rec in records:
        for k in rec.keys():
            if k not in seen:
                header.append(k)
                seen.add(k)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def main():
    input_csv = "all_urls.csv"
    output_csv = "company_info.csv"

    urls = read_urls(input_csv)
    if not urls:
        print(
            "all_urls.csv にURLが見つかりませんでした。1列目にURLを記載してください。",
            file=sys.stderr,
        )
        sys.exit(1)

    records: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(extract_record, url): url for url in urls}
        for future in as_completed(future_map):
            rec = future.result()
            if rec is not None:
                records.append(rec)

    # 安定ソート
    records.sort(key=lambda r: r.get("取得URL", ""))

    write_csv(output_csv, records)
    print(f"完了: {output_csv} に {len(records)} 件出力しました。（名称ありのみ）")


if __name__ == "__main__":
    main()
