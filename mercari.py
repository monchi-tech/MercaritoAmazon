import streamlit as st
import csv
import datetime
import time
import io
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import openpyxl
import base64

# ▼▼▼ 使う武器を変更！st.connectionの代わりに、もっと原始的な武器を使うぜ！ ▼▼▼
from supabase import create_client, Client

# ===================================================================
# config.py を信じて直接読み込む！
# ===================================================================
import config
# ===================================================================

# -------------------------------------------------------------------
# ヘルパー関数群（変更なし）
# -------------------------------------------------------------------
def convert_price_to_amazon(mercari_price_str):
    try:
        price_int = int("".join(filter(str.isdigit, mercari_price_str)))
    except (ValueError, TypeError): 
        return 0
    for threshold, amazon_price in config.PRICE_CONVERSION_TABLE:
        if price_int >= threshold: 
            return amazon_price
    return 0

def trigger_download(file_data, filename):
    b64 = base64.b64encode(file_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}" id="auto_download">ダウンロード</a>'
    html = f"""
    {href}
    <script>
        document.getElementById('auto_download').click();
    </script>
    """
    return html

def safe_get_element_text(driver, selector, default="不明"):
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )
        return element.text
    except (TimeoutException, NoSuchElementException):
        return default

def safe_get_element_attribute(driver, selector, attribute, default=""):
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )
        return element.get_attribute(attribute)
    except (TimeoutException, NoSuchElementException):
        return default

# -------------------------------------------------------------------
# スクレイピングのメイン処理「心臓部」（変更なし）
# -------------------------------------------------------------------
def run_mercari_scraper(keyword, max_pages, user_id, supabase_client):
    yield f"[ログ] Supabaseから '{user_id}' のデータを読み込みます..."
    try:
        ng_sellers_data = supabase_client.table("ng_sellers").select("seller_name").eq("user_id", user_id).execute()
        ng_sellers = {item['seller_name'] for item in ng_sellers_data.data}
        ng_words_data = supabase_client.table("ng_words").select("word").eq("user_id", user_id).execute()
        ng_words = {item['word'] for item in ng_words_data.data}
        processed_urls_data = supabase_client.table("processed_urls").select("url").eq("user_id", user_id).execute()
        processed_urls = {item['url'] for item in processed_urls_data.data}
        yield f"[ログ] NGセラー: {len(ng_sellers)}件, NGワード: {len(ng_words)}件, 処理済みURL: {len(processed_urls)}件"
    except Exception as e:
        yield f"[エラー] Supabaseからのデータ読み込みに失敗しました: {e}"
        return

    options = webdriver.ChromeOptions()
    options.add_argument('--headless') # 本番なのでコメントアウトを外す！
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=options)
    search_url = f"https://www.mercari.com/jp/search/?keyword=  {keyword.replace(' ', '%20')}"
    yield f"[ログ] 次のURLにアクセスします: {search_url}"
    driver.get(search_url)
    time.sleep(3)

    all_links = set()
    for page_num in range(1, max_pages + 1):
        yield f"\n--- {page_num} ページ目のURL収集を開始 ---"
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li[data-testid="item-cell"]')))
            yield "[ログ] ページをスクロールして全商品を読み込みます..."
            scroll_count = 0
            max_scrolls = 50 
            while scroll_count < max_scrolls:
                item_count_before = len(driver.find_elements(By.CSS_SELECTOR, 'li[data-testid="item-cell"]'))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2) 
                item_count_after = len(driver.find_elements(By.CSS_SELECTOR, 'li[data-testid="item-cell"]'))
                yield f"[ログ] スクロール中... ({item_count_after}件表示)"
                if item_count_before == item_count_after:
                    yield f"[ログ] ページの最下部に到達！全 {item_count_after} 件の商品を捕捉完了！"
                    break
                scroll_count += 1
            if scroll_count >= max_scrolls:
                yield "[警告] スクロール回数が上限に達しました。処理を続行します。"
                item_list_elements = driver.find_elements(By.CSS_SELECTOR, 'li[data-testid="item-cell"] a')
            for a_tag in item_list_elements:
                href = a_tag.get_attribute('href')
                if href and '/item/' in href:
                    all_links.add(href)
            yield f"→ 現在までのユニークURL合計: {len(all_links)} 件"
            
            if page_num < max_pages:
                next_button_selector = '[data-testid="pagination-next-button"] a'
                try:
                    next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, next_button_selector)))
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(3)
                except (TimeoutException, NoSuchElementException):
                    yield "[ログ] 次のページボタンが見つかりません。検索を終了します。"
                    break
            else:
                yield "[ログ] 最大検索ページ数に達しました。"
                break
        except (TimeoutException, NoSuchElementException) as e:
            yield f"[警告] ページ処理中にエラーが発生しました: {e}"
            break
    
    new_links_to_process = [url for url in all_links if url not in processed_urls]
    if not new_links_to_process:
        yield "\n新しい商品は見つかりませんでした。処理を終了します。"
        driver.quit()
        return
    
    yield f"\n--- 全URL取得完了。{len(new_links_to_process)} 件の新しい商品をチェックします ---"
    
    ok_items_to_db = []
    ng_items_to_db = []

    for i, url in enumerate(new_links_to_process, 1):
        yield f"\n[{i}/{len(new_links_to_process)}] チェック中... -> {url[:50]}..."
        try:
            driver.get(url)
            time.sleep(2)
            
            item_name = safe_get_element_text(driver, 'h1', "商品名不明")
            if item_name == "商品名不明": item_name = safe_get_element_text(driver, 'h1[class*="item-name"]', "商品名不明")
            seller_name = safe_get_element_text(driver, '[data-testid="seller-link"] p', "出品者不明")
            price_text = safe_get_element_text(driver, '[data-testid="price"]', "0")
            description = safe_get_element_text(driver, '[data-testid="description"]', "説明なし")
            main_image_url = safe_get_element_attribute(driver, '[data-testid="image-0"] img', 'src', "")
            
            is_ng, ng_reason = False, ""
            if item_name == "商品名不明": is_ng, ng_reason = True, "商品名取得不可"
            elif seller_name in ng_sellers: is_ng, ng_reason = True, f"NGセラー: {seller_name}"
            elif any(ng_word in (item_name + description) for ng_word in ng_words): is_ng, ng_reason = True, "NGワード"
            
            amazon_price = convert_price_to_amazon(price_text)
            if not is_ng and amazon_price <= 0: is_ng, ng_reason = True, f"価格範囲外: {price_text}"
            
            if is_ng:
                yield f"  [除外({ng_reason})]"
                ng_items_to_db.append({"user_id": user_id, "url": url, "status": "ng"})
                continue

            item_data = {'商品URL': url, '商品名': item_name, 'Amazon推奨価格': amazon_price, '商品説明': description, 'メイン画像URL': main_image_url}
            ok_items_to_db.append(item_data)
            yield "  [OK] 条件クリア！リストに追加しました。"
            
            if i % 10 == 0:
                driver.delete_all_cookies()
                driver.execute_script("window.localStorage.clear();")
                driver.execute_script("window.sessionStorage.clear();")
                
        except Exception as e:
            yield f"  [エラー] このページの処理中にエラーが発生: {str(e)}"
            ng_items_to_db.append({"user_id": user_id, "url": url, "status": "ng"})
            continue
    
    driver.quit()
    yield "\n\n===== 全ての処理が完了しました！ ====="
    
    try:
        if ok_items_to_db:
            ok_urls_to_insert = [{"user_id": user_id, "url": item['商品URL'], "status": "ok"} for item in ok_items_to_db]
            supabase_client.table("processed_urls").upsert(ok_urls_to_insert, on_conflict="user_id, url").execute()
            yield f"[ログ] OK処理済みURLを {len(ok_urls_to_insert)} 件Supabaseに保存しました。"
        if ng_items_to_db:
            supabase_client.table("processed_urls").upsert(ng_items_to_db, on_conflict="user_id, url").execute()
            yield f"[ログ] NG処理済みURLを {len(ng_items_to_db)} 件Supabaseに保存しました。"
    except Exception as e:
        yield f"[エラー] Supabaseへのデータ保存に失敗: {e}"
    
    yield ok_items_to_db

# -------------------------------------------------------------------
# StreamlitのUI部分【最終決戦バージョン】
# -------------------------------------------------------------------
st.set_page_config(page_title="メルカリお宝探しツール", layout="wide")

# ▼▼▼▼▼▼ ここが今回の改造の心臓部！ ▼▼▼▼▼▼

# ステップ１：まず、Secretsがちゃんと読み込めてるか、画面に表示させて確認する
st.header("デバッグ情報")
try:
    supabase_url = st.secrets["connections"]["supabase"]["url"]
    supabase_key = st.secrets["connections"]["supabase"]["key"]
    st.success("Secretsの読み込みに成功！")
    st.write(f"URL: {supabase_url}")
    # キーは全部表示すると危ないから、一部だけ表示する
    st.write(f"Key: {supabase_key[:5]}...") 
except Exception as e:
    st.error("Secretsの読み込みに失敗したぜ！StreamlitのSecrets設定をもう一回確認してくれ！")
    st.exception(e)
    st.stop() # Secretsが読めないなら、ここで終わりだ

# ステップ２：st.connectionを使わずに、直接Supabaseクライアントを作る
try:
    # 読み込んだURLとKeyを使って、直接クライアントを初期化する
    conn: Client = create_client(supabase_url, supabase_key)
    st.success("Supabaseクライアントの作成に成功！")
except Exception as e:
    st.error("Supabaseクライアントの作成に失敗！URLかKeyが間違ってる可能性が高いぜ！")
    st.exception(e)
    st.stop() # クライアントが作れないなら、ここで終わりだ

# ▲▲▲▲▲▲ ここまでが最終診断コード ▲▲▲▲▲▲

# -------------------------------------------------------------------
# 独自の認証機能
# -------------------------------------------------------------------
st.title("🔐 ログイン")

# セッション状態の初期化
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.user_id = None

# ログイン画面
if not st.session_state.authenticated:
    with st.form("login_form"):
        st.markdown("### メールアドレスでログイン")
        email = st.text_input("メールアドレス", placeholder="your.email@example.com")
        password = st.text_input("パスワード", type="password", placeholder="任意のパスワード")
        
        col1, col2 = st.columns(2)
        with col1:
            login_button = st.form_submit_button("ログイン", use_container_width=True, type="primary")
        with col2:
            signup_button = st.form_submit_button("新規登録", use_container_width=True)
        
        if login_button or signup_button:
            if email and password:
                # 簡易的な検証（本番環境では適切な認証を実装）
                if "@" in email and len(password) >= 4:
                    st.session_state.authenticated = True
                    st.session_state.user_id = email
                    st.success(f"ようこそ、{email}さん！")
                    st.rerun()
                else:
                    st.error("メールアドレスまたはパスワードが正しくありません")
            else:
                st.warning("メールアドレスとパスワードを入力してください")
    
    st.stop()  # ログインしていない場合はここで処理を停止

# ログイン済みの場合
user_id = st.session_state.user_id
st.success(f"ログイン中: {user_id}")

# ログアウトボタン
if st.button("ログアウト", key="logout_main"):
    st.session_state.authenticated = False
    st.session_state.user_id = None
    st.rerun()

st.divider()

# -------------------------------------------------------------------
# メインアプリケーション
# -------------------------------------------------------------------
st.title('メルカリお宝探しツール（Web版）')

# -------------------------------------------------------------------
# NGリスト管理機能（サイドバーに実装！）
# -------------------------------------------------------------------
with st.sidebar:
    st.header(f"⚙️ {user_id} の設定")
    
    # サイドバーにもログアウトボタン
    if st.button("ログアウト", key="logout_sidebar"):
        st.session_state.authenticated = False
        st.session_state.user_id = None
        st.rerun()
    
    st.divider()

    # --- NGセラー管理 ---
    st.subheader("🚫 NGセラーリスト")
    try:
        sellers = conn.table("ng_sellers").select("id, seller_name").eq("user_id", user_id).execute().data
        seller_df = pd.DataFrame(sellers).set_index("id")
        st.dataframe(seller_df, use_container_width=True)

        with st.form("add_seller_form", clear_on_submit=True):
            new_seller = st.text_input("追加するNGセラー名")
            if st.form_submit_button("追加"):
                if new_seller:
                    conn.table("ng_sellers").insert({"user_id": user_id, "seller_name": new_seller}).execute()
                    st.toast(f"「{new_seller}」を追加したぜ！")
                    st.rerun()

        if not seller_df.empty:
            seller_to_delete = st.selectbox("削除するNGセラーを選択", options=seller_df.index, format_func=lambda x: seller_df.loc[x, "seller_name"], index=None)
            if st.button("削除", type="primary"):
                if seller_to_delete is not None:
                    deleted_name = seller_df.loc[seller_to_delete, "seller_name"]
                    conn.table("ng_sellers").delete().eq("id", int(seller_to_delete)).execute()
                    st.toast(f"「{deleted_name}」を削除したぜ！")
                    st.rerun()

    except Exception as e:
        st.error(f"NGセラーの読み込みに失敗: {e}")

    # --- NGワード管理 ---
    st.subheader("🤫 NGワードリスト")
    try:
        words = conn.table("ng_words").select("id, word").eq("user_id", user_id).execute().data
        word_df = pd.DataFrame(words).set_index("id")
        st.dataframe(word_df, use_container_width=True)

        with st.form("add_word_form", clear_on_submit=True):
            new_word = st.text_input("追加するNGワード")
            if st.form_submit_button("追加"):
                if new_word:
                    conn.table("ng_words").insert({"user_id": user_id, "word": new_word}).execute()
                    st.toast(f"「{new_word}」を追加したぜ！")
                    st.rerun()

        if not word_df.empty:
            word_to_delete = st.selectbox("削除するNGワードを選択", options=word_df.index, format_func=lambda x: word_df.loc[x, "word"], index=None)
            if st.button("削除", type="primary", key="delete_word"):
                if word_to_delete is not None:
                    deleted_word = word_df.loc[word_to_delete, "word"]
                    conn.table("ng_words").delete().eq("id", int(word_to_delete)).execute()
                    st.toast(f"「{deleted_word}」を削除したぜ！")
                    st.rerun()
    except Exception as e:
        st.error(f"NGワードの読み込みに失敗: {e}")

# -------------------------------------------------------------------
# メイン画面（スクレイピング実行）
# -------------------------------------------------------------------
st.markdown("メルカリから指定したキーワードで商品を検索し、条件に合うものだけをリストアップします。")

with st.form("search_form"):
    st.info(f"`config.py` の設定をデフォルト値として使用しています。")
    keyword = st.text_input('1. 検索キーワードを入力してください', value=config.SEARCH_KEYWORD)
    max_pages = st.number_input('2. 何ページまで検索しますか？', min_value=1, max_value=20, value=config.MAX_PAGES)
    submitted = st.form_submit_button("お宝探し スタート！")

if 'running' not in st.session_state: st.session_state.running = False
if 'results' not in st.session_state: st.session_state.results = []

if submitted and not st.session_state.running:
    if not keyword:
        st.warning('キーワードが入力されていません。')
    else:
        st.session_state.running = True
        st.session_state.results = []
        log_area = st.empty()
        progress_bar = st.progress(0, "処理を開始します...")
        final_results = []
        log_text = ""
        
        for result in run_mercari_scraper(keyword, max_pages, user_id, conn):
            if isinstance(result, str):
                log_text += result + "\n"
                log_area.text_area("実行ログ", log_text, height=300, key=f"log_{time.time()}")
                try:
                    if "チェック中" in result:
                        progress_str = result.split('[')[1].split(']')[0]
                        current, total = map(int, progress_str.split('/'))
                        progress_bar.progress(current/total, f"商品詳細をチェック中... ({current}/{total})")
                    elif "ページ目のURL収集を開始" in result:
                        progress_bar.progress(0, result.strip())
                except: pass
            elif isinstance(result, list):
                final_results = result
        
        st.session_state.results = final_results
        st.session_state.running = False
        progress_bar.progress(1.0, "処理が完了しました！")
        st.rerun()

# -------------------------------------------------------------------
# 結果表示とExcelダウンロード部分
# -------------------------------------------------------------------
if st.session_state.results:
    st.subheader('🎉 検索結果')
    df_raw = pd.DataFrame(st.session_state.results)
    st.dataframe(df_raw)
    
    if not df_raw.empty:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "AmazonUpload"

        master_header_en = ['feed_product_type', 'item_sku', 'brand_name', 'item_name', 'external_product_id', 'external_product_id_type', 'manufacturer', 'recommended_browse_nodes', 'is_adult_product', 'quantity', 'standard_price', 'main_image_url', 'target_gender', 'other_image_url1', 'other_image_url2', 'other_image_url3', 'other_image_url4', 'other_image_url5', 'other_image_url6', 'other_image_url7', 'other_image_url8', 'swatch_image_url', 'parent_child', 'variation_theme', 'parent_sku', 'relationship_type', 'update_delete', 'part_number', 'product_description', 'care_instructions1', 'care_instructions2', 'care_instructions3', 'language_value', 'model', 'edition', 'bullet_point1', 'bullet_point2', 'bullet_point3', 'bullet_point4', 'bullet_point5', 'generic_keywords', 'mfg_minimum_unit_of_measure', 'mfg_maximum_unit_of_measure', 'assembly_instructions', 'is_assembly_required', 'assembly_time_unit_of_measure', 'assembly_time', 'folded_size', 'material_type', 'special_features1', 'special_features2', 'special_features3', 'special_features4', 'special_features5', 'size_name', 'color_name', 'color_map', 'number_of_pieces', 'engine_type', 'recommended_uses_for_product', 'collection_name', 'genre', 'battery_description', 'lithium_battery_voltage', 'catalog_number', 'platinum_keywords1', 'platinum_keywords2', 'platinum_keywords3', 'platinum_keywords4', 'platinum_keywords5', 'style_name', 'lithium_battery_voltage_unit_of_measure', 'target_audience_keywords1', 'target_audience_keywords2', 'target_audience_keywords3', 'target_audience_keywords4', 'target_audience_keywords5', 'shaft_style_type', 'controller_type', 'mfg_minimum', 'mfg_maximum', 'subject_character', 'material_composition', 'scale_name', 'rail_gauge', 'remote_control_technology', 'frequency_bands_supported', 'educational_objective', 'website_shipping_weight', 'website_shipping_weight_unit_of_measure', 'minimum_weight_recommendation', 'minimum_weight_recommendation_unit_of_measure', 'maximum_weight_recommendation', 'maximum_weight_recommendation_unit_of_measure', 'minimum_height_recommendation', 'minimum_height_recommendation_unit_of_measure', 'maximum_height_recommendation', 'maximum_height_recommendation_unit_of_measure', 'size_map', 'handle_height', 'handle_height_unit_of_measure', 'seat_width', 'seat_width_unit_of_measure', 'item_height', 'item_length', 'item_width', 'item_display_weight', 'item_display_weight_unit_of_measure', 'item_display_length', 'item_display_length_unit_of_measure', 'item_dimensions_unit_of_measure', 'fulfillment_center_id', 'package_length', 'package_width', 'package_height', 'package_weight', 'package_weight_unit_of_measure', 'package_dimensions_unit_of_measure', 'legal_disclaimer_description', 'safety_warning', 'warranty_description', 'country_string', 'country_of_origin', 'specific_uses_for_product', 'are_batteries_included', 'batteries_required', 'battery_type1', 'battery_type2', 'battery_type3', 'number_of_batteries1', 'number_of_batteries2', 'number_of_batteries3', 'lithium_battery_energy_content', 'number_of_lithium_ion_cells', 'number_of_lithium_metal_cells', 'lithium_battery_weight', 'lithium_battery_packaging', 'item_weight', 'item_weight_unit_of_measure', 'battery_cell_composition', 'battery_weight', 'battery_weight_unit_of_measure', 'lithium_battery_energy_content_unit_of_measure', 'lithium_battery_weight_unit_of_measure', 'supplier_declared_dg_hz_regulation1', 'supplier_declared_dg_hz_regulation2', 'supplier_declared_dg_hz_regulation3', 'supplier_declared_dg_hz_regulation4', 'supplier_declared_dg_hz_regulation5', 'hazmat_united_nations_regulatory_id', 'safety_data_sheet_url', 'item_volume', 'item_volume_unit_of_measure', 'flash_point', 'ghs_classification_class1', 'ghs_classification_class2', 'ghs_classification_class3', 'fulfillment_latency', 'condition_type', 'condition_note', 'product_site_launch_date', 'merchant_release_date', 'restock_date', 'optional_payment_type_exclusion', 'delivery_schedule_group_id', 'sale_price', 'sale_from_date', 'sale_end_date', 'item_package_quantity', 'list_price', 'number_of_items', 'offering_can_be_giftwrapped', 'offering_can_be_gift_messaged', 'max_order_quantity', 'is_discontinued_by_manufacturer', 'offering_end_date', 'product_tax_code', 'merchant_shipping_group_name', 'is_expiration_dated_product', 'distribution_designation', 'offering_start_date', 'standard_price_points_percent', 'sale_price_points_percent', 'business_price', 'quantity_price_type', 'quantity_lower_bound1', 'quantity_price1', 'quantity_lower_bound2', 'quantity_price2', 'quantity_lower_bound3', 'quantity_price3', 'quantity_lower_bound4', 'quantity_price4', 'quantity_lower_bound5', 'quantity_price5', 'progressive_discount_type', 'progressive_discount_lower_bound1', 'progressive_discount_value1', 'progressive_discount_lower_bound2', 'progressive_discount_value2', 'progressive_discount_lower_bound3', 'progressive_discount_value3', 'pricing_action']
        header_jp_map = {'feed_product_type': '商品タイプ', 'item_sku': '出品者SKU', 'brand_name': 'ブランド名', 'item_name': '商品名', 'external_product_id': '商品コード(JANコード等)', 'external_product_id_type': '商品コードのタイプ', 'manufacturer': 'メーカー名', 'recommended_browse_nodes': '推奨ブラウズノード', 'is_adult_product': 'アダルト商品', 'quantity': '在庫数', 'standard_price': '商品の販売価格', 'main_image_url': '商品メイン画像URL', 'update_delete': 'アップデート・削除', 'part_number': 'メーカー型番', 'product_description': '商品説明文', 'model': '型番', 'edition': '版', 'bullet_point1': '商品の仕様', 'care_instructions1': 'お取り扱い上の注意', 'fulfillment_latency': '出荷作業日数', 'condition_type': 'コンディション', 'condition_note': '商品のコンディション説明'}
        magic_header = ['TemplateType=fptcustom', 'Version=2021.1025', 'TemplateSignature=SE9CQkIFUw==', 'settings=contentLanguageTag=ja_JP&feedType=113&headerLanguageTag=ja_JP&metadataVersion=MatprodVkxBUHJvZC0xMTQ0&primaryMarketplaceId=amzn1.mp.o.A1VC38T7YXB528&templateIdentifier=02a2bf65-f3d4-4c17-a875-ac4db5407f03×tamp=2021-10-25T16%3A47%3A43.222Z', '上3行はAmazon.comのみで使用します。上3行は変更または削除しないでください。']
        magic_header.extend([''] * (len(master_header_en) - len(magic_header)))
        jp_header_row = [header_jp_map.get(col, '') for col in master_header_en]

        data_rows = []
        for index, raw_row in df_raw.iterrows():
            row_data = {}
            row_data['feed_product_type'] = 'Hobbies'
            row_data['item_sku'] = "m_" + raw_row['商品URL'].split('/item/')[1].replace('/', '')
            row_data['brand_name'] = 'ノーブランド品'
            row_data['item_name'] = raw_row['商品名']
            row_data['manufacturer'] = 'ノーブランド品'
            row_data['recommended_browse_nodes'] = '3113755051'
            row_data['is_adult_product'] = 'FALSE'
            row_data['quantity'] = 1
            row_data['standard_price'] = raw_row['Amazon推奨価格']
            row_data['main_image_url'] = raw_row['メイン画像URL']
            row_data['update_delete'] = 'Update'
            row_data['part_number'] = 'NON'
            row_data['product_description'] = raw_row['商品名']
            row_data['model'] = 'NON'
            row_data['edition'] = 'NON'
            row_data['bullet_point1'] = raw_row['商品名']
            row_data['care_instructions1'] = ''
            row_data['fulfillment_latency'] = '7'
            row_data['condition_type'] = '新品'
            row_data['condition_note'] = '新品'
            data_rows.append(row_data)

        df_final = pd.DataFrame(data_rows, columns=master_header_en)
        df_final = df_final.fillna('')

        ws.append(magic_header)
        ws.append(jp_header_row)
        ws.append(master_header_en)
        for index, row in df_final.iterrows(): ws.append(row.tolist())
        output_excel = io.BytesIO()
        wb.save(output_excel)
        excel_data = output_excel.getvalue()
        output_excel.close()
        filename = f'amazon_upload_final_v5_{datetime.datetime.now().strftime("%Y%m%d%H%M")}.xlsx'
        st.download_button(label="【最終形態】Amazonアップロード用Excelをダウンロード", data=excel_data, file_name=filename, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        st.balloons()
        
elif st.session_state.get('running') == False and st.session_state.get('results') is not None:
    st.info("条件に合う商品はありませんでした。または、まだ検索を実行していません。")