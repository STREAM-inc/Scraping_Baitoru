import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime
import re

# サイトへの負荷を下げるための設定
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

REQUEST_TIMEOUT = 10       # 秒
MAX_RETRIES = 3            # 最大リトライ回数
BASE_BACKOFF_SEC = 3       # リトライ間隔の基準秒数

# 取得したいフィールド一覧
FIELDS = [
    "取得日時",
    "取得URL",
    "名称",
    "電話番号",
    "都道府県",
    "住所",
    "業種",
    "法人番号",
    "代表者",
    "資本金",
    "売上",
    "従業員数",
    "設立日",
    "事業内容",
    "HP",
]

# DOM セレクタで拾うフィールド（今回のHTMLでは name くらいに留める）
SELECTORS = {
    "name": [
        "h1",  # 実際の class 名に合わせて必要なら変更する
    ],
    # tel や industry などは、別の場所のHTMLを見てから必要に応じて追加
}


def fetch_html(url, max_retries=MAX_RETRIES):
    """
    指定したURLからHTMLを取得する。
    ネットワークエラーが起きても max_retries 回までは自動リトライする。
    """
    last_exc = None

    for i in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if i < max_retries - 1:
                wait_sec = BASE_BACKOFF_SEC * (i + 1)
                print(f"[WARN] fetch_html: error {exc} (retry {i + 1}/{max_retries})")
                time.sleep(wait_sec)

    # 全て失敗した場合
    raise last_exc


def safe_get_text_from_selectors(soup, selectors):
    """
    複数のCSSセレクタ候補を順番に試して、
    最初に見つかった要素のテキストを返す。
    見つからなければ None を返す。
    """
    if not selectors:
        return None

    for selector in selectors:
        try:
            el = soup.select_one(selector)
        except Exception:
            # 不正なセレクタなどで例外が出ても他のセレクタを試す
            continue

        if el is None:
            continue

        text = el.get_text(strip=True)
        if text:
            return text

    return None


def parse_analytics_data_from_head(soup):
    """
    head 内の <script> から analyticsData オブジェクトを粗くパースして
    prop61（会社名など）を取り出す。

    戻り値:
        dict: {"prop61": "株式会社XXX …", ...} のような辞書（見つからなければ空 dict）
    """
    result = {}

    # すべての <script> タグを走査する
    for script in soup.find_all("script"):
        text = script.get_text()
        if not text:
            continue

        if "var analyticsData" not in text:
            continue

        # 例:
        # "prop61" : "株式会社第一興商 飲食事業部",
        m = re.search(r'"prop61"\s*:\s*"([^"]*)"', text)
        if m:
            result["prop61"] = m.group(1).strip()

        break

    return result


def get_value_by_label(soup, label, prefer_link=False):
    """
    <dl>
      <dt><span>所在地</span></dt>
      <dd><p>千葉県千葉市…</p></dd>
    </dl>
    のような構造から、
    ラベル (例: "所在地") に対応する dd の中身を返す。

    prefer_link=True の場合、dd 内の <a href> を優先的に返す。
    見つからなければ None。
    """
    # span のテキストに label を含むものを探す
    span = soup.find("span", string=lambda t: t and label in t)
    if not span:
        return None

    # span -> 親の dt -> 次の兄弟 dd
    dt = span.find_parent("dt")
    if not dt:
        return None

    dd = dt.find_next_sibling("dd")
    if not dd:
        return None

    # URL など、リンクの href を取りたい場合
    if prefer_link:
        a = dd.find("a", href=True)
        if a and a["href"]:
            return a["href"].strip()

    # 通常は dd のテキスト全体を返す
    text = dd.get_text(separator=" ", strip=True)
    return text or None


# 日本の都道府県を住所文字列から抜き出す簡易関数
PREF_PATTERN = re.compile(r"(北海道|東京都|京都府|大阪府|..県)")


def extract_prefecture(address_text):
    """
    住所文字列から「都道府県」部分だけを抜き出す簡易版。
    見つからなければ None。
    """
    if not address_text:
        return None
    m = PREF_PATTERN.search(address_text)
    if m:
        return m.group(1)
    return None


def scrape_company_page(url):
    """
    単一の企業ページURLから企業情報を取得する。
    どれかの項目が無くても例外を出さず、None のまま返す。
    """
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    data = {}
    data["取得日時"] = datetime.now().isoformat(timespec="seconds")
    data["取得URL"] = url

    # 会社名（analyticsData → h1 の順で試す）
    analytics = parse_analytics_data_from_head(soup)
    raw_name = analytics.get("prop61")
    if raw_name:
        # 末尾の「のバイト/アルバイト/パートの求人情報」を消す簡易処理
        data["名称"] = raw_name.replace("のバイト/アルバイト/パートの求人情報", "")
    else:
        data["名称"] = safe_get_text_from_selectors(soup, SELECTORS.get("name", []))

    # 所在地（住所）
    address = get_value_by_label(soup, "所在地")
    data["住所"] = address

    # 都道府県（住所から抽出）
    data["都道府県"] = extract_prefecture(address)

    # 設立年
    data["設立日"] = get_value_by_label(soup, "設立年")

    # 資本金
    data["資本金"] = get_value_by_label(soup, "資本金")

    # 代表者名
    data["代表者"] = get_value_by_label(soup, "代表者名")

    # 従業員数
    data["従業員数"] = get_value_by_label(soup, "従業員数")

    # 事業内容
    data["事業内容"] = get_value_by_label(soup, "事業内容")

    # ホームページ（URL）
    data["HP"] = get_value_by_label(soup, "URL", prefer_link=True)

    # このHTML片には無いものは None にしておく
    data["電話番号"] = None
    data["業種"] = None
    data["法人番号"] = None
    data["売上"] = None

    return data


def scrape_many_company_pages(urls, sleep_sec=1.5):
    """
    複数の企業ページURLを順番にスクレイピングする。
    1件でエラーが起きても他のURLの処理を続行する。
    """
    results = []

    for url in urls:
        try:
            print(f"[INFO] scraping: {url}")
            row = scrape_company_page(url)
            results.append(row)
        except Exception as exc:
            print(f"[ERROR] failed to scrape {url}: {exc}")
        finally:
            time.sleep(sleep_sec)

    return results


def save_to_csv(rows, filepath):
    """
    スクレイピング結果のリストをCSVに保存する。
    足りないキーや None は空文字列に置き換える。
    """
    if not rows:
        print("[WARN] save_to_csv: rows is empty")
        return

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()

        for row in rows:
            safe_row = {
                field: (row.get(field, "") if row.get(field) is not None else "")
                for field in FIELDS
            }
            writer.writerow(safe_row)


if __name__ == "__main__":
    # fetch_info_baitoru.py が出力した company_urls.csv から企業ページURLを読み込む
    company_urls = []
    with open("company_urls.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("company_url")
            if url:
                company_urls.append(url)

    results = scrape_many_company_pages(company_urls)
    save_to_csv(results, "company_data.csv")
