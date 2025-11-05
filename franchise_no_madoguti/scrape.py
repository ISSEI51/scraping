# scrape_fc_mado_company_info.py

import concurrent.futures
import csv
import datetime
import os
import re
import sys
import time
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from tqdm import tqdm

# ユーザーエージェント（一般的なブラウザ文字列）
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 20  # 秒

CPU_COUNT = os.cpu_count() or 4
# I/Oバウンドなので並列数はCPUの4倍程度、上限32
MAX_WORKERS = min(32, CPU_COUNT * 4)

# 軽いレート制御（高速過ぎる連打を避ける）
REQUEST_INTERVAL_SEC = 0.2


# リトライ設定
def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(DEFAULT_HEADERS)
    return session


LABEL_PATTERNS: List[Tuple[str, str]] = [
    (r"会社情報|会社概要", "会社情報セクション"),
    (r"住所|所在地", "住所"),
    (r"設立|設立年月日", "設立"),
    (r"代表者|代表取締役|代表", "代表者"),
    (r"資本金", "資本金"),
    (r"事業内容|業務内容|事業", "事業内容"),
    (r"従業員|社員数|従業員数", "従業員"),
    (r"URL|公式サイト|サイト|ホームページ", "URL"),
    (r"電話|TEL|電話番号", "電話"),
    (r"所在地", "住所"),
]


def load_urls_from_csv(csv_path: str) -> List[str]:
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = row[0].strip()
            if not val:
                continue
            if val.lower() in {"url", "リンク", "urls"}:
                continue
            urls.append(val)
    return urls


_session = build_session()
_last_request_time = 0.0


def fetch(url: str) -> Optional[str]:
    global _last_request_time
    # レート制御
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_INTERVAL_SEC:
        time.sleep(REQUEST_INTERVAL_SEC - elapsed)
    try:
        r = _session.get(url, timeout=REQUEST_TIMEOUT)
        _last_request_time = time.time()
        if not (200 <= r.status_code < 300):
            return None
        # エンコーディング推定
        r.encoding = r.apparent_encoding or r.encoding
        return r.text
    except requests.RequestException:
        return None


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def label_to_key(label: str) -> str:
    lab = normalize_space(label)
    for pat, key in LABEL_PATTERNS:
        if re.search(pat, lab, flags=re.IGNORECASE):
            return key
    return lab


def extract_text(el) -> str:
    if el is None:
        return ""
    txt = el.get_text(separator=" ", strip=True)
    return normalize_space(txt)


def find_company_section(soup: BeautifulSoup) -> List[BeautifulSoup]:
    candidates: List[BeautifulSoup] = []

    for tag_name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
        for h in soup.find_all(tag_name):
            text = extract_text(h)
            if re.search(r"(会社情報|会社概要)", text):
                sib = h.find_next_sibling()
                if sib:
                    candidates.append(sib)
                if h.parent:
                    candidates.append(h.parent)

    for a in soup.find_all("a"):
        text = extract_text(a)
        if re.search(r"(会社情報|会社概要)", text):
            if a.parent:
                candidates.append(a.parent)
            sib = a.find_next_sibling()
            if sib:
                candidates.append(sib)

    for div in soup.find_all("div"):
        txt = extract_text(div)
        if re.search(r"(会社情報|会社概要)", txt) and len(txt) < 3000:
            candidates.append(div)

    uniq = []
    seen = set()
    for c in candidates:
        key = id(c)
        if key not in seen:
            seen.add(key)
            uniq.append(c)
    return uniq


def parse_label_value_text(raw: str) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    lines = [
        normalize_space(line) for line in raw.splitlines() if normalize_space(line)
    ]
    for line in lines:
        m = re.match(r"^(.{1,30}?)[：:]\s*(.+)$", line)
        if not m:
            m = re.match(r"^(.{1,30}?)\s*[-]\s*(.+)$", line)
        if m:
            label, value = m.group(1), m.group(2)
            results.append((label, value))
    return results


def extract_company_info_from_section(section: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}

    for dl in section.find_all("dl"):
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            key = label_to_key(extract_text(dt))
            val = extract_text(dd)
            if val:
                data[key] = val

    for table in section.find_all("table"):
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = label_to_key(extract_text(th))
                val = extract_text(td)
                if val:
                    data[key] = val

    txt = extract_text(section)
    for label, value in parse_label_value_text(txt):
        key = label_to_key(label)
        if value:
            data.setdefault(key, value)

    for strong in section.find_all(["strong", "b"]):
        label = extract_text(strong)
        next_text = ""
        if strong.next_sibling:
            if hasattr(strong.next_sibling, "get_text"):
                next_text = extract_text(strong.next_sibling)
            else:
                next_text = normalize_space(str(strong.next_sibling))
        if next_text:
            key = label_to_key(label)
            val = next_text
            if not re.search(r"[：:]", val) and len(val) <= 300:
                data.setdefault(key, val)

    if "事業内容" not in data:
        paragraphs = []
        for p in section.find_all("p"):
            txtp = normalize_space(p.get_text(" ", strip=True))
            if txtp and len(txtp) >= 30:
                paragraphs.append(txtp)
        if paragraphs:
            data["事業内容"] = " / ".join(paragraphs[:3])

    return data


def pick_name(soup: BeautifulSoup) -> Optional[str]:
    breadcrumbs = soup.find_all(["li", "span", "a"])
    crumb_candidates = []
    for el in breadcrumbs:
        txt = extract_text(el)
        if re.search(r"(株式会社|有限会社|組合|合同会社)", txt):
            crumb_candidates.append(txt)
    if crumb_candidates:
        return sorted(crumb_candidates, key=len)[0]

    for tag_name in ["h1", "h2"]:
        for h in soup.find_all(tag_name):
            txt = extract_text(h)
            if re.search(r"(株式会社|有限会社|合同会社)", txt):
                return txt

    if soup.title and soup.title.string:
        title = normalize_space(soup.title.string)
        m = re.search(r"(株式会社.+?)($|[|｜]|｜)", title)
        if m:
            return m.group(1)

    return None


def pick_address(info_map: Dict[str, str], soup: BeautifulSoup) -> Optional[str]:
    if "住所" in info_map and info_map["住所"]:
        return info_map["住所"]

    full_text = extract_text(soup)
    m = re.search(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県).{5,80}", full_text)
    if m:
        return normalize_space(m.group(0))

    return None


def scrape_one(url: str) -> Dict[str, str]:
    html = fetch(url)
    now_str = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    base: Dict[str, str] = {
        "取得日時": now_str,
        "取得URL": url,
        "名称": "",
        "住所": "",
    }
    if html is None:
        return base

    soup = BeautifulSoup(html, "lxml")
    sections = find_company_section(soup)

    info_map: Dict[str, str] = {}
    for sec in sections:
        extracted = extract_company_info_from_section(sec)
        for k, v in extracted.items():
            if k not in info_map and v:
                info_map[k] = v

    name = pick_name(soup)
    address = pick_address(info_map, soup)

    for k in ["会社名", "商号", "法人名", "名称"]:
        if k in info_map and not name:
            name = info_map[k]

    base["名称"] = name or ""
    base["住所"] = address or ""

    for k, v in info_map.items():
        if k in {"住所", "名称"}:
            continue
        base[k] = v

    if "URL" not in base:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http"):
                text = extract_text(a)
                if (
                    re.search(r"(公式|サイト|ホームページ|URL)", text)
                    or "fc-mado.com" not in href
                ):
                    base["URL"] = href
                    break

    return base


def to_dataframe(rows: List[Dict[str, str]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in ["取得日時", "取得URL", "名称", "住所"]:
        if col not in df.columns:
            df[col] = ""
    other_cols = [
        c for c in df.columns if c not in ["取得日時", "取得URL", "名称", "住所"]
    ]
    ordered = ["取得日時", "取得URL", "名称", "住所"] + sorted(other_cols)
    df = df[ordered]
    return df


def main():
    if len(sys.argv) < 2:
        csv_path = "urls.csv"
    else:
        csv_path = sys.argv[1]

    urls = load_urls_from_csv(csv_path)
    if not urls:
        print("all_urls.csv にURLがありません。1列目にURLを配置してください。")
        sys.exit(1)

    results: List[Dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_one, u): u for u in urls}
        for fut in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Scraping",
        ):
            try:
                row = fut.result()
                results.append(row)
            except Exception as e:
                url = futures[fut]
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                results.append(
                    {
                        "取得日時": now_str,
                        "取得URL": url,
                        "名称": "",
                        "住所": "",
                        "エラー": str(e),
                    }
                )

    df = to_dataframe(results)
    df.to_csv("company_info_output.csv", index=False, encoding="utf-8-sig")
    print("Saved company_info_output.csv ({} rows)".format(len(df)))


if __name__ == "__main__":
    main()
