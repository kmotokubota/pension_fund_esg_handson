# =========================================================
# グローバル年金基金 サステナビリティレポート分析
# =========================================================

import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# =========================================================
# ヘルパー関数
# =========================================================
def clean_ai_response(response):
    """AI_COMPLETEの出力からエスケープシーケンスとクォートを削除"""
    if isinstance(response, str):
        if response.startswith('"') and response.endswith('"'):
            response = response[1:-1]
        response = response.replace('\\n', '\n')
        response = response.replace('\\t', '\t')
        response = response.replace('\\"', '"')
        response = response.replace('\\\\', '\\')
    return response

# =========================================================
# ページ設定
# =========================================================
st.set_page_config(
    page_title="グローバル年金基金分析",
    page_icon="◆",
    layout="wide"
)

# =========================================================
# カスタムCSS（Figma風デザイン）
# =========================================================
st.markdown("""
<style>
    /* メインカラー設定 */
    :root {
        --primary-purple: #7B61FF;
        --primary-purple-light: #A78BFA;
        --bg-light: #FAFAFA;
        --text-dark: #1E1E1E;
        --text-gray: #6B7280;
        --border-color: #E5E7EB;
    }
    
    /* ヘッダースタイル */
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: var(--text-dark);
        margin-bottom: 8px;
        line-height: 1.2;
    }
    
    .sub-header {
        font-size: 1rem;
        color: var(--text-gray);
        margin-bottom: 20px;
        line-height: 1.5;
    }
    
    /* サイドバー */
    [data-testid="stSidebar"] {
        background-color: var(--bg-light);
    }
    
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 {
        font-size: 14px;
        font-weight: 600;
        color: var(--text-dark);
    }
    
    /* タブスタイル */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid var(--border-color);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-size: 14px;
        font-weight: 500;
        color: var(--text-gray);
        padding: 12px 16px;
    }
    
    .stTabs [aria-selected="true"] {
        color: var(--primary-purple) !important;
        border-bottom-color: var(--primary-purple) !important;
    }
    
    /* ボタンスタイル */
    .stButton > button[kind="primary"] {
        background-color: var(--primary-purple);
        border: none;
        font-weight: 500;
    }
    
    .stButton > button[kind="primary"]:hover {
        background-color: var(--primary-purple-light);
    }
    
    /* Expanderスタイル */
    .streamlit-expanderHeader {
        font-size: 14px;
        font-weight: 500;
    }
    
    /* 区切り線 */
    hr {
        border-color: var(--border-color);
        margin: 24px 0;
    }
    
    /* セクションヘッダー */
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--text-dark);
        margin: 16px 0 8px 0;
        line-height: 1.3;
    }
</style>
""", unsafe_allow_html=True)

# =========================================================
# Snowflake接続
# =========================================================
session = get_active_session()
root = Root(session)

current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
current_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]

# =========================================================
# 設定値
# =========================================================
CORTEX_SEARCH_DATABASE = "DEMO_DB"
CORTEX_SEARCH_SCHEMA = "DEMO_SUSTAINABILITY"
CORTEX_SEARCH_SERVICE = "GLOBAL_PF_SUSTAINABILITY_REPORT"
CORTEX_SEARCH_VIEW = "COMBINED_GLOBAL_SUSTAINABILITY_VIEW"
DOCUMENT_STAGE = "DOCUMENT_STAGE"

# =========================================================
# セッション状態の初期化
# =========================================================
if 'selected_reports' not in st.session_state:
    st.session_state.selected_reports = []
if 'summary_results' not in st.session_state:
    st.session_state.summary_results = {}
if 'trend_analysis' not in st.session_state:
    st.session_state.trend_analysis = None
if 'gap_analysis' not in st.session_state:
    st.session_state.gap_analysis = None
if 'file_list_refresh_key' not in st.session_state:
    st.session_state.file_list_refresh_key = 0

# =========================================================
# データ取得関数
# =========================================================
def get_file_list():
    """利用可能なPDFファイルのリストを取得"""
    # refresh_keyを使ってキャッシュを無効化できるようにする
    refresh_key = st.session_state.get('file_list_refresh_key', 0)
    return _get_file_list_cached(refresh_key)

@st.cache_data
def _get_file_list_cached(refresh_key):
    """キャッシュ付きのファイルリスト取得関数"""
    try:
        query = f"""
        SELECT DISTINCT FILE_NAME, SOURCE_REPORT
        FROM {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_VIEW}
        ORDER BY SOURCE_REPORT, FILE_NAME
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"ファイル一覧の取得に失敗しました: {str(e)}")
        return pd.DataFrame()

def refresh_file_list():
    """ファイルリストのキャッシュをリフレッシュ"""
    st.session_state.file_list_refresh_key += 1

def get_full_report_text(file_name, limit=100):
    """指定されたレポートの全テキストを取得"""
    try:
        query = f"""
        SELECT CHUNK_TEXT, PAGE_INDEX
        FROM {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_VIEW}
        WHERE FILE_NAME = '{file_name}'
        ORDER BY PAGE_INDEX, CHUNK_INDEX_ON_PAGE
        LIMIT {limit}
        """
        df = session.sql(query).to_pandas()
        
        if len(df) > 0:
            full_text = "\n\n".join(df['CHUNK_TEXT'].tolist())
            return full_text
        return ""
    except Exception as e:
        st.error(f"レポートテキストの取得に失敗しました: {str(e)}")
        return ""

# =========================================================
# AI分析関数
# =========================================================
def summarize_report(file_name, report_text):
    """AI_COMPLETEを使用してレポートをサマライズ"""
    try:
        max_chars = 10000
        if len(report_text) > max_chars:
            report_text = report_text[:max_chars] + "..."
        
        prompt = f"""あなたは年金基金のサステナビリティレポートを分析する専門家です。
以下のレポートの内容を日本語で要約してください。

【レポート名】
{file_name}

【レポート内容】
{report_text}

【要約の条件】
以下の観点で構造化して要約してください：

## 1. エグゼクティブサマリー（200字程度）
レポート全体の概要を簡潔に説明

## 2. 重要な取り組み・イニシアティブ
- 主要な取り組み内容
- 特徴的な施策

## 3. ESG投資戦略
- 環境（E）への取り組み
- 社会（S）への取り組み
- ガバナンス（G）への取り組み

## 4. 数値目標・実績
具体的な数値目標や達成実績があれば記載

## 5. 特筆すべき点
他の年金基金と比較して特徴的な点や先進的な取り組み
"""
        
        ai_query = f"""
        SELECT AI_COMPLETE(
            'claude-sonnet-4-5',
            '{prompt.replace("'", "''")}'
        ) AS response
        """
        
        result = session.sql(ai_query).collect()
        raw_response = result[0]['RESPONSE']
        
        return clean_ai_response(raw_response)
        
    except Exception as e:
        st.error(f"サマライズに失敗しました: {str(e)}")
        return f"エラー: {str(e)}"

def analyze_trends(selected_files_data):
    """複数レポートからトレンドを分析"""
    try:
        summaries = []
        for file_name, summary in selected_files_data.items():
            summaries.append(f"【{file_name}】\n{summary}")
        
        combined_summaries = "\n\n---\n\n".join(summaries)
        
        prompt = f"""年金基金のサステナビリティトレンドを分析し、以下のレポート要約から共通トレンドを抽出してください。

入力データ
{combined_summaries}

出力仕様（厳守）
次の構造でMarkdown出力。前置き・締めは不要。各項目は300字程度で簡潔に記載。

## 1. 共通する重点テーマ
複数レポートで共通するテーマや優先事項（3-5項目、具体例を含む）

## 2. ESG投資のトレンド
### 気候変動対応
具体的な戦略、投資事例、目標値

### ダイバーシティ＆インクルージョン
各基金の取り組み内容と特徴

### スチュワードシップ活動
エンゲージメント手法、議決権行使方針

## 3. 目標設定のトレンド
### ネットゼロ目標
具体的な目標年、中間目標、進捗状況

### その他の数値目標
ESG投資額、CO2削減目標など具体的数値

## 4. 開示・報告の特徴
報告フレームワーク（TCFD、ISSB等）の採用状況と開示の詳細度

## 5. 先進的な取り組み
革新的または注目すべき取り組み事例（投資先企業名や具体的金額を含む、2-3項目）

## 6. 日本への示唆
GPIFをはじめとする日本の年金基金が学べる具体的な点（3-4項目）

全6項目を必ず完成させてください。
"""
        
        ai_query = f"""
        SELECT AI_COMPLETE(
            'claude-sonnet-4-5',
            '{prompt.replace("'", "''")}'
        ) AS response
        """
        
        result = session.sql(ai_query).collect()
        raw_response = result[0]['RESPONSE']
        
        return clean_ai_response(raw_response)
        
    except Exception as e:
        st.error(f"トレンド分析に失敗しました: {str(e)}")
        return f"エラー: {str(e)}"

def analyze_gap(gpif_summary, global_summaries):
    """GPIFレポートと海外年金基金レポートのGAP分析"""
    try:
        global_text = "\n\n---\n\n".join([
            f"【{name}】\n{summary}"
            for name, summary in global_summaries.items()
        ])
        
        prompt = f"""GPIFと海外年金基金のサステナビリティレポートを比較し、GAP分析を行ってください。

入力データ
【GPIF】
{gpif_summary}

【海外年金基金】
{global_text}

出力仕様（厳守）
次の構造でMarkdown出力。前置き・締めは不要。各項目は200字以内で簡潔に記載。

## 1. GPIFの強み
海外と比較して優れている点（3項目、箇条書き）

## 2. 改善の機会
海外から学べる取り組み（3項目、具体名を含む箇条書き）

## 3. 開示・コミュニケーションのGAP
### レポート構成の違い
情報開示の詳細度、コミュニケーションの違い（2項目）

### 報告フレームワークの違い
TCFD、ISSB等の採用状況（1-2項目）

## 4. ESG投資戦略のGAP
### 気候変動対応の違い
ネットゼロ戦略、トランジション・ファイナンス（2項目）

### エンゲージメント手法の違い
アクティブオーナーシップ、議決権行使（2項目）

### インパクト測定の違い
効果測定の手法、KPI設定（1-2項目）

## 5. ガバナンス体制のGAP
組織体制、専門人材の配置、外部運用機関との関係性（2-3項目）

## 6. 具体的な推奨事項
GPIFが取り組むべき優先事項（3項目、実行可能性を含む箇条書き）

全6項目を必ず完成させてください。
"""
        
        ai_query = f"""
        SELECT AI_COMPLETE(
            'claude-sonnet-4-5',
            '{prompt.replace("'", "''")}'
        ) AS response
        """
        
        result = session.sql(ai_query).collect()
        raw_response = result[0]['RESPONSE']
        
        return clean_ai_response(raw_response)
        
    except Exception as e:
        st.error(f"GAP分析に失敗しました: {str(e)}")
        return f"エラー: {str(e)}"

# =========================================================
# UI
# =========================================================
st.title("グローバル年金基金 サステナビリティレポート分析")
st.caption("世界の主要年金基金のサステナビリティレポートを分析し、トレンドの抽出やGPIFとの比較分析を行います")

# サイドバー
with st.sidebar:
    st.markdown("### 分析設定")
    
    files_df = get_file_list()
    
    if len(files_df) > 0:
        st.markdown("**レポート選択**")
        
        gpif_files = files_df[files_df['FILE_NAME'].str.contains('gpif', case=False, na=False)]
        global_files = files_df[~files_df['FILE_NAME'].str.contains('gpif', case=False, na=False)]
        
        st.markdown("GPIFレポート")
        if len(gpif_files) > 0:
            gpif_file = st.selectbox(
                "GPIFレポートを選択",
                options=gpif_files['FILE_NAME'].tolist(),
                label_visibility="collapsed"
            )
            st.session_state.gpif_file = gpif_file
        else:
            st.warning("GPIFレポートが見つかりません")
            st.session_state.gpif_file = None
        
        st.markdown("---")
        
        st.markdown("海外年金基金レポート")
        if len(global_files) > 0:
            selected_global_files = st.multiselect(
                "分析対象のレポートを選択",
                options=global_files['FILE_NAME'].tolist(),
                default=global_files['FILE_NAME'].tolist()[:3] if len(global_files) >= 3 else global_files['FILE_NAME'].tolist(),
                label_visibility="collapsed"
            )
            st.session_state.selected_reports = selected_global_files
            
            st.caption(f"選択中: {len(selected_global_files)}件")
        else:
            st.error("海外レポートが見つかりません")
    else:
        st.error("レポートが見つかりません")
    
    st.markdown("---")
    st.caption(f"データソース: {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}")

# =========================================================
# タブ構成
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "レポートサマリー",
    "トレンド分析", 
    "GAP分析",
    "レポート追加"
])

# ========================================
# タブ1: レポートサマリー
# ========================================
with tab1:
    st.header("レポートサマリー")
    st.caption("選択した各レポートの内容を自動的にサマライズします")
    st.markdown("---")
    
    include_gpif = st.checkbox(
        "GPIFレポートもサマライズする",
        value=False
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("サマライズ実行", type="primary", use_container_width=True):
            if len(st.session_state.selected_reports) == 0 and not (include_gpif and st.session_state.gpif_file):
                st.warning("サイドバーからレポートを選択してください")
            else:
                st.session_state.summary_results = {}
                
                if include_gpif and st.session_state.gpif_file:
                    with st.spinner(f"{st.session_state.gpif_file} を分析中..."):
                        gpif_text = get_full_report_text(st.session_state.gpif_file)
                        if gpif_text:
                            gpif_summary = summarize_report(st.session_state.gpif_file, gpif_text)
                            st.session_state.summary_results[st.session_state.gpif_file] = gpif_summary
                
                if len(st.session_state.selected_reports) > 0:
                    progress_bar = st.progress(0)
                    for idx, file_name in enumerate(st.session_state.selected_reports):
                        with st.spinner(f"{file_name} を分析中... ({idx+1}/{len(st.session_state.selected_reports)})"):
                            report_text = get_full_report_text(file_name)
                            if report_text:
                                summary = summarize_report(file_name, report_text)
                                st.session_state.summary_results[file_name] = summary
                            progress_bar.progress((idx + 1) / len(st.session_state.selected_reports))
                    
                    progress_bar.empty()
                st.success("サマライズ完了")
    
    if st.session_state.summary_results:
        st.markdown("---")
        st.markdown("**サマライズ結果**")
        
        if st.session_state.gpif_file and st.session_state.gpif_file in st.session_state.summary_results:
            with st.expander(f"GPIF: {st.session_state.gpif_file}", expanded=True):
                st.markdown(st.session_state.summary_results[st.session_state.gpif_file])
        
        for file_name, summary in st.session_state.summary_results.items():
            if file_name != st.session_state.gpif_file:
                with st.expander(file_name, expanded=False):
                    st.markdown(summary)

# ========================================
# タブ2: トレンド分析
# ========================================
with tab2:
    st.header("グローバルトレンド分析")
    st.caption("選択した複数の海外年金基金レポートから、共通するトレンドや特徴を抽出します")
    st.markdown("---")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("トレンド分析を実行", type="primary", use_container_width=True):
            if len(st.session_state.summary_results) < 2:
                st.warning("まず「レポートサマリー」タブで2件以上のレポートをサマライズしてください")
            else:
                global_summaries = {
                    name: summary 
                    for name, summary in st.session_state.summary_results.items()
                    if name != st.session_state.gpif_file
                }
                
                if len(global_summaries) < 2:
                    st.warning("海外レポートが2件以上必要です")
                else:
                    with st.spinner("トレンド分析を実行中...（1-2分かかる場合があります）"):
                        trend_result = analyze_trends(global_summaries)
                        st.session_state.trend_analysis = trend_result
                    
                    st.success("トレンド分析完了")
    
    if st.session_state.trend_analysis:
        st.markdown("---")
        st.markdown("**分析結果**")
        st.markdown(st.session_state.trend_analysis)
        
        st.markdown("---")
        st.download_button(
            label="分析結果をダウンロード",
            data=st.session_state.trend_analysis,
            file_name=f"trend_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain"
        )

# ========================================
# タブ3: GAP分析
# ========================================
with tab3:
    st.header("GPIF vs グローバル年金基金 GAP分析")
    st.caption("GPIFのサステナビリティレポートと海外主要年金基金のレポートを比較し、強み、改善機会、具体的な推奨事項を提示します")
    st.markdown("---")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("GAP分析を実行", type="primary", use_container_width=True):
            if not st.session_state.gpif_file:
                st.warning("GPIFレポートが選択されていません")
            elif st.session_state.gpif_file not in st.session_state.summary_results:
                st.warning("まず「レポートサマリー」タブでGPIFレポートをサマライズしてください")
            elif len(st.session_state.summary_results) < 2:
                st.warning("比較対象となる海外レポートが必要です")
            else:
                gpif_summary = st.session_state.summary_results[st.session_state.gpif_file]
                global_summaries = {
                    name: summary 
                    for name, summary in st.session_state.summary_results.items()
                    if name != st.session_state.gpif_file
                }
                
                with st.spinner("GAP分析を実行中...（1-2分かかる場合があります）"):
                    gap_result = analyze_gap(gpif_summary, global_summaries)
                    st.session_state.gap_analysis = gap_result
                
                st.success("GAP分析完了")
    
    if st.session_state.gap_analysis:
        st.markdown("---")
        st.markdown("**分析結果**")
        st.markdown(st.session_state.gap_analysis)
        
        st.markdown("---")
        st.download_button(
            label="分析結果をダウンロード",
            data=st.session_state.gap_analysis,
            file_name=f"gap_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain"
        )

# ========================================
# タブ4: レポート追加
# ========================================
with tab4:
    st.header("新規レポート追加")
    st.caption("新しい海外年金基金のサステナビリティレポート（PDF）をアップロードして、分析対象に追加します")
    st.markdown("---")
    
    st.markdown("""
    **処理の流れ**
    1. PDFファイルをアップロード
    2. ステージに保存
    3. AI_PARSE_DOCUMENTでテキスト抽出
    4. チャンク化してデータベースに格納
    """)
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader(
        "PDFファイルを選択",
        type=['pdf'],
        label_visibility="collapsed"
    )
    
    if uploaded_file is not None:
        st.caption(f"選択されたファイル: {uploaded_file.name} ({uploaded_file.size / 1024 / 1024:.2f} MB)")
        
        if st.button("レポートを追加", type="primary"):
            try:
                with st.spinner("ステップ1/4: ファイルをステージにアップロード中..."):
                    stage_path = f"global_pf_esg_report/{uploaded_file.name}"
                    
                    temp_path = f"/tmp/{uploaded_file.name}"
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    stage_location = f"@{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.document_stage/global_pf_esg_report/"
                    session.file.put(
                        temp_path,
                        stage_location,
                        auto_compress=False,
                        overwrite=True
                    )
                    st.success("ファイルアップロード完了")
                
                with st.spinner("ステップ2/4: テキスト抽出中..."):
                    escaped_stage_path = stage_path.replace("'", "''")
                    
                    check_table_sql = f"""
                    SELECT column_name 
                    FROM {CORTEX_SEARCH_DATABASE}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_schema = '{CORTEX_SEARCH_SCHEMA}' 
                    AND table_name = 'GLOBAL_PF_SUSTAINABILITY_REPORT'
                    ORDER BY ordinal_position
                    """
                    columns_result = session.sql(check_table_sql).collect()
                    table_columns = [row['COLUMN_NAME'] for row in columns_result]
                    
                    if len(table_columns) == 4:
                        direct_insert_sql = f"""
                        INSERT INTO {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.global_pf_sustainability_report 
                        (relative_path, scoped_file_url, raw_text_dict, raw_text)
                        SELECT
                            '{escaped_stage_path}' AS relative_path,
                            GET_PRESIGNED_URL('@{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.document_stage', '{escaped_stage_path}') AS scoped_file_url,
                            AI_PARSE_DOCUMENT(
                                TO_FILE('@{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.document_stage', '{escaped_stage_path}'),
                                {{'mode': 'LAYOUT', 'page_split': true}}
                            ) AS raw_text_dict,
                            NULL AS raw_text
                        """
                    else:
                        direct_insert_sql = f"""
                        INSERT INTO {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.global_pf_sustainability_report 
                        (relative_path, scoped_file_url, raw_text_dict)
                        SELECT
                            '{escaped_stage_path}' AS relative_path,
                            GET_PRESIGNED_URL('@{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.document_stage', '{escaped_stage_path}') AS scoped_file_url,
                            AI_PARSE_DOCUMENT(
                                TO_FILE('@{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.document_stage', '{escaped_stage_path}'),
                                {{'mode': 'LAYOUT', 'page_split': true}}
                            ) AS raw_text_dict
                        """
                    
                    session.sql(direct_insert_sql).collect()
                    st.success("テキスト抽出完了")
                
                with st.spinner("ステップ3/4: チャンク化中..."):
                    chunk_sql = f"""
                    INSERT INTO {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.global_pf_sustainability_report_chunk 
                    (relative_path, scoped_file_url, file_name, page_index, chunk_index_on_page, chunk_text, chunk_id)
                    SELECT
                        t.relative_path,
                        t.scoped_file_url,
                        SPLIT_PART(t.relative_path, '/', -1) AS file_name,
                        p.value:index::INT AS page_index, 
                        c.index::INT AS chunk_index_on_page,
                        c.value::STRING AS chunk_text,
                        MD5_HEX(t.relative_path || ':' || p.value:index::INT || ':' || c.index::INT)::STRING AS chunk_id
                    FROM 
                        {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.global_pf_sustainability_report t,
                        LATERAL FLATTEN(input => t.raw_text_dict:pages) p,
                        LATERAL FLATTEN(
                            INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                                p.value:content::STRING,
                                'markdown',
                                1000,
                                100
                            )
                        ) c
                    WHERE t.relative_path = '{escaped_stage_path}'
                    """
                    
                    try:
                        session.sql(chunk_sql).collect()
                        
                        chunk_count_sql = f"""
                        SELECT COUNT(*) as count 
                        FROM {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.global_pf_sustainability_report_chunk
                        WHERE relative_path = '{escaped_stage_path}'
                        """
                        chunk_count_result = session.sql(chunk_count_sql).collect()
                        chunk_count = chunk_count_result[0]['COUNT']
                        st.success(f"チャンク化完了（{chunk_count}チャンク生成）")
                        
                        if chunk_count == 0:
                            st.warning("チャンクが0件です。raw_text_dictの構造を確認してください。")
                    except Exception as e:
                        st.error(f"チャンク化エラー: {str(e)}")
                        chunk_count = 0
                
                with st.spinner("ステップ4/4: データを反映中..."):
                    escaped_filename = uploaded_file.name.replace("'", "''")
                    
                    view_check_sql = f"""
                    SELECT COUNT(*) as count 
                    FROM {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.COMBINED_GLOBAL_SUSTAINABILITY_VIEW
                    WHERE file_name = '{escaped_filename}'
                    """
                    view_result = session.sql(view_check_sql).collect()
                    view_count = view_result[0]['COUNT']
                    st.success(f"データ反映完了（ビュー内に{view_count}チャンク確認）")
                
                st.markdown("---")
                st.success(f"""
                **レポート追加が完了しました**
                
                - ファイル名: {uploaded_file.name}
                - 生成チャンク数: {chunk_count}
                
                サイドバーのレポートリストに自動的に追加されます。
                """)
                
                # キャッシュをリフレッシュしてからリロード
                refresh_file_list()
                
                if st.button("ページをリロード", key="reload_after_upload"):
                    st.rerun()
                    
            except Exception as e:
                st.error(f"エラーが発生しました: {str(e)}")
                st.markdown("""
                **トラブルシューティング:**
                - ステージへのアクセス権限を確認してください
                - ファイル名に特殊文字が含まれていないか確認してください
                - 同じファイル名のレポートが既に存在しないか確認してください
                """)

# フッター
st.markdown("---")
st.caption("GPIF グローバル年金基金 サステナビリティレポート分析システム")
