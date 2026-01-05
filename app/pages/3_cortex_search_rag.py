# =========================================================
# Cortex Search RAG チャットアプリ（コスト最適化版）
# - Cortex Agent不使用
# - Cortex Search + Cortex Complete を直接使用
# =========================================================

from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# =========================================================
# ページ設定
# =========================================================
st.set_page_config(
    page_title="Cortex Search RAG",
    page_icon="🔍",
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
    
    /* チャットメッセージ */
    [data-testid="stChatMessage"] {
        border-radius: 12px;
    }
    
    /* 設定カード */
    .settings-card {
        background-color: var(--bg-light);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Cortex Complete をSQL経由で呼び出す関数
def cortex_complete(session, model: str, prompt: str) -> str:
    """Cortex CompleteをSQL経由で実行（SiS互換）"""
    # プロンプト内のシングルクォートをエスケープ
    escaped_prompt = prompt.replace("'", "''")
    
    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{escaped_prompt}'
        ) AS response
    """
    
    result = session.sql(sql).collect()
    return result[0]['RESPONSE'] if result else ""

# =====================================================
# 設定
# =====================================================
DEFAULT_DATABASE = "DEMO_DB"
DEFAULT_SCHEMA = "DEMO_SUSTAINABILITY"

# 利用可能なモデル
MODELS = [
    "claude-4-sonnet",
    "claude-3-7-sonnet",
    "claude-3-5-sonnet",
    "llama4-maverick",
    "llama4-scout",
]

# Cortex Search Services（固定リスト - 動的取得も可能）
SEARCH_SERVICES = [
    {
        "name": "スチュワードシップ評価用",
        "fq_name": "DEMO_DB.DEMO_SUSTAINABILITY.SUSTAINABILITY_REPORT",
        "db": "DEMO_DB",
        "schema": "DEMO_SUSTAINABILITY",
        "short_name": "SUSTAINABILITY_REPORT",
        "search_column": "chunk_text",
        "columns": ["chunk_text", "file_name", "relative_path", "scoped_file_url", "page_index"],
    },
    {
        "name": "グローバル年金分析用",
        "fq_name": "DEMO_DB.DEMO_SUSTAINABILITY.GLOBAL_PF_SUSTAINABILITY_REPORT",
        "db": "DEMO_DB",
        "schema": "DEMO_SUSTAINABILITY",
        "short_name": "GLOBAL_PF_SUSTAINABILITY_REPORT",
        "search_column": "chunk_text",
        "columns": ["chunk_text", "file_name", "relative_path", "scoped_file_url", "page_index", "source_report"],
    },
]

# Snowflake接続
session = get_active_session()
root = Root(session)


# =====================================================
# ユーティリティ関数
# =====================================================

def get_cortex_search_service(service_config: Dict[str, Any]):
    """Cortex Search Serviceオブジェクトを取得"""
    return root.databases[service_config["db"]].schemas[service_config["schema"]].cortex_search_services[service_config["short_name"]]


@st.cache_data(ttl=300)
def get_available_files(service_short_name: str) -> List[str]:
    """選択されたサービスで利用可能なファイル名一覧を取得"""
    try:
        if service_short_name == "SUSTAINABILITY_REPORT":
            sql = """
            SELECT DISTINCT file_name 
            FROM DEMO_DB.DEMO_SUSTAINABILITY.COMBINED_SUSTAINABILITY_CHUNKS_VIEW
            ORDER BY file_name
            """
        else:
            sql = """
            SELECT DISTINCT file_name 
            FROM DEMO_DB.DEMO_SUSTAINABILITY.COMBINED_GLOBAL_SUSTAINABILITY_VIEW
            ORDER BY file_name
            """
        result = session.sql(sql).collect()
        return [row['FILE_NAME'] for row in result if row['FILE_NAME']]
    except Exception:
        return []


def query_cortex_search(
    query: str,
    service_config: Dict[str, Any],
    num_results: int = 5,
    filter_obj: Optional[Dict[str, Any]] = None
) -> tuple[str, List[Dict[str, Any]]]:
    """Cortex Searchを実行してコンテキストを取得"""
    
    svc = get_cortex_search_service(service_config)
    search_col = service_config.get("search_column", "chunk_text")
    request_columns = service_config.get("columns", ["chunk_text", "file_name", "relative_path"])
    
    # 検索実行
    kwargs = {
        "query": query,
        "columns": request_columns,
        "limit": num_results,
    }
    if filter_obj:
        kwargs["filter"] = filter_obj
    
    doc = svc.search(**kwargs)
    results = doc.results
    
    # コンテキスト構築
    context_rows = []
    context_lines = []
    
    for i, r in enumerate(results, start=1):
        content = r.get(search_col) or r.get(search_col.lower()) or r.get(search_col.upper()) or ""
        file_name = r.get("file_name") or r.get("FILE_NAME") or ""
        relative_path = r.get("relative_path") or r.get("RELATIVE_PATH") or ""
        file_url = r.get("scoped_file_url") or r.get("SCOPED_FILE_URL") or r.get("file_url") or ""
        page_index = r.get("page_index") or r.get("PAGE_INDEX") or ""
        
        context_rows.append({
            "idx": i,
            "file_name": file_name,
            "relative_path": relative_path,
            "file_url": file_url,
            "page_index": page_index,
            "chunk": content,
        })
        
        # LLMに渡すコンテキストテキスト
        source_info = f"[ファイル: {file_name}"
        if page_index:
            source_info += f", ページ: {page_index}"
        source_info += "]"
        context_lines.append(f"--- ドキュメント {i} {source_info} ---\n{content}\n")
    
    context_text = "\n".join(context_lines)
    return context_text, context_rows


def build_history_text(chat_history: List[Dict[str, Any]], k: int) -> str:
    """過去の会話履歴をテキストに変換"""
    if k <= 0 or not chat_history:
        return ""
    turns = chat_history[-k:]
    messages = []
    for t in turns:
        messages.append(f"ユーザー: {t.get('question', '')}")
        messages.append(f"アシスタント: {t.get('answer', '')}")
    return "\n".join(messages)


def build_prompt(history_text: str, context_text: str, user_query: str, service_name: str) -> str:
    """LLMに送るプロンプトを構築"""
    
    system = f"""あなたは{service_name}の専門アシスタントです。
以下の検索結果（コンテキスト）と会話履歴のみを根拠に、正確かつ簡潔に日本語で回答してください。

【回答ルール】
1. 必ず出典（ファイル名、ページ番号）を明記してください
2. コンテキストに記載がない情報は「資料からは確認できませんでした」と回答してください
3. 推測や憶測は避け、事実に基づいて回答してください
4. 複数の情報がある場合は、箇条書きで整理してください

【フォーマットルール（重要）】
- 箇条書きには必ず「- 」（ハイフン+スペース）を使用してください
- 各箇条書き項目は必ず改行で区切ってください
- セクション見出しには「##」や「###」を使用してください
- 段落間には空行を入れてください
- 「・」や「•」は使用せず、マークダウン形式の「- 」を使ってください

【出力例】
## セクション見出し

- 項目1の内容
- 項目2の内容
- 項目3の内容

### サブセクション

- サブ項目1
- サブ項目2"""

    parts = [
        f"【システム指示】\n{system}",
        f"【過去の会話】\n{history_text or '(なし)'}",
        f"【検索結果（参照ドキュメント）】\n{context_text or '(該当なし)'}",
        f"【ユーザーの質問】\n{user_query}",
    ]
    return "\n\n".join(parts)


def stream_text(container, full_text: str, step: int = 80):
    """テキストを段階的に表示（疑似ストリーミング）"""
    buf = ""
    for i in range(0, len(full_text), step):
        buf += full_text[i:i+step]
        container.markdown(buf)
        time.sleep(0.02)


# =====================================================
# サイドバー
# =====================================================

def init_sidebar():
    st.sidebar.markdown("## ⚙️ 設定")
    
    # --- Cortex Search Service選択 ---
    st.sidebar.markdown("### 🔍 検索サービス")
    
    service_options = {s["name"]: s for s in SEARCH_SERVICES}
    selected_name = st.sidebar.selectbox(
        "Cortex Search Service",
        options=list(service_options.keys()),
        index=0,
    )
    st.session_state.selected_service = service_options[selected_name]
    st.sidebar.caption(f"📍 {st.session_state.selected_service['fq_name']}")
    
    st.sidebar.divider()
    
    # --- モデル選択 ---
    st.sidebar.markdown("### 🤖 LLMモデル")
    
    if "selected_model" not in st.session_state:
        st.session_state.selected_model = MODELS[0]
    
    st.session_state.selected_model = st.sidebar.selectbox(
        "回答生成モデル",
        MODELS,
        index=MODELS.index(st.session_state.selected_model),
    )
    
    st.sidebar.divider()
    
    # --- 検索パラメータ ---
    st.sidebar.markdown("### 📊 検索パラメータ")
    
    if "num_retrieved_chunks" not in st.session_state:
        st.session_state.num_retrieved_chunks = 5
    
    st.session_state.num_retrieved_chunks = st.sidebar.slider(
        "参照チャンク数",
        min_value=1,
        max_value=15,
        value=st.session_state.num_retrieved_chunks,
        help="検索で取得するドキュメントチャンクの数"
    )
    
    if "history_k" not in st.session_state:
        st.session_state.history_k = 3
    
    st.session_state.history_k = st.sidebar.slider(
        "過去履歴の参照数",
        min_value=0,
        max_value=10,
        value=st.session_state.history_k,
        help="LLMに渡す過去の会話ターン数"
    )
    
    st.sidebar.divider()
    
    # --- フィルタ（オプション） ---
    st.sidebar.markdown("### 📁 フィルタ")
    
    # ファイル名一覧を取得
    available_files = get_available_files(st.session_state.selected_service["short_name"])
    
    if available_files:
        file_options = ["すべてのファイル"] + available_files
        
        if "filter_file_name" not in st.session_state:
            st.session_state.filter_file_name = "すべてのファイル"
        
        selected_file = st.sidebar.selectbox(
            "ファイル名でフィルタ",
            options=file_options,
            index=file_options.index(st.session_state.filter_file_name) if st.session_state.filter_file_name in file_options else 0,
        )
        st.session_state.filter_file_name = selected_file
        st.session_state.filter_enabled = (selected_file != "すべてのファイル")
    else:
        st.sidebar.caption("ファイルが見つかりません")
    
    st.sidebar.divider()
    
    # --- 履歴管理 ---
    st.sidebar.markdown("### 📝 履歴管理")
    
    if "rag_chat_history" not in st.session_state:
        st.session_state.rag_chat_history = []
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🗑️ 履歴クリア", use_container_width=True):
            st.session_state.rag_chat_history = []
            st.rerun()
    
    with col2:
        st.sidebar.caption(f"履歴: {len(st.session_state.rag_chat_history)}件")
    
    # --- 情報表示 ---
    st.sidebar.divider()
    st.sidebar.caption("💡 **Cortex Search + Complete**")
    st.sidebar.caption("Cortex Agentを使用しないコスト最適化版です。")


# =====================================================
# チャット表示
# =====================================================

def render_context_expander(context_rows: List[Dict[str, Any]]):
    """参照コンテキストをエクスパンダで表示"""
    if not context_rows:
        return
    
    with st.expander(f"📚 参照ドキュメント ({len(context_rows)}件)", expanded=False):
        for r in context_rows:
            st.markdown(f"**#{r['idx']} - {r['file_name']}**")
            if r.get("page_index"):
                st.caption(f"📄 ページ: {r['page_index']}")
            
            # チャンク内容（折りたたみ）
            with st.container():
                chunk_preview = r["chunk"][:300] + "..." if len(r["chunk"]) > 300 else r["chunk"]
                st.text(chunk_preview)
            
            if r.get("file_url"):
                st.markdown(f"[📎 ファイルを開く]({r['file_url']})")
            
            st.divider()


def render_chat_history():
    """過去のチャット履歴を表示"""
    for turn in st.session_state.get("rag_chat_history", []):
        # ユーザーメッセージ
        with st.chat_message("user"):
            st.markdown(turn.get("question", ""))
        
        # アシスタントメッセージ
        with st.chat_message("assistant"):
            st.markdown(turn.get("answer", ""))
            
            # 参照コンテキスト
            ctx = turn.get("contexts") or []
            if ctx:
                render_context_expander(ctx)


# =====================================================
# メイン
# =====================================================

def main():
    # ヘッダー
    st.title("🔍 Cortex Search RAG")
    st.markdown('<p class="sub-header">Cortex Search + Cortex Complete によるコスト最適化RAG検索</p>', unsafe_allow_html=True)
    
    # サイドバー初期化
    init_sidebar()
    
    service = st.session_state.get("selected_service")
    if not service:
        st.info("👈 左のサイドバーで検索サービスを選択してください。")
        return
    
    # 現在の設定を表示（コンパクト表示）
    with st.expander("⚙️ 現在の設定", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"**検索サービス**<br><span style='color: #6B7280;'>{service['name']}</span>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"**LLMモデル**<br><span style='color: #6B7280;'>{st.session_state.selected_model}</span>", unsafe_allow_html=True)
        with col3:
            filter_text = st.session_state.filter_file_name if st.session_state.get("filter_enabled") and st.session_state.get("filter_file_name") else "なし"
            st.markdown(f"**フィルタ**<br><span style='color: #6B7280;'>{filter_text}</span>", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # チャット履歴を表示
    render_chat_history()
    
    # チャット入力
    user_query = st.chat_input("質問を入力してください...")
    
    if user_query:
        # ユーザーメッセージを即座に表示
        with st.chat_message("user"):
            st.markdown(user_query)
        
        # フィルタ構築
        filter_obj = None
        if st.session_state.get("filter_enabled") and st.session_state.get("filter_file_name"):
            filter_file = st.session_state.filter_file_name
            if filter_file and filter_file != "すべてのファイル":
                # Cortex Searchのフィルタ（@eq演算子で完全一致）
                filter_obj = {"@eq": {"file_name": filter_file}}
        
        # アシスタント応答
        with st.chat_message("assistant"):
            with st.spinner("検索中..."):
                # 1) Cortex Searchで検索
                context_text, context_rows = query_cortex_search(
                    query=user_query,
                    service_config=service,
                    num_results=st.session_state.num_retrieved_chunks,
                    filter_obj=filter_obj,
                )
            
            with st.spinner("回答生成中..."):
                # 2) 履歴テキスト構築
                history_text = build_history_text(
                    st.session_state.get("rag_chat_history", []),
                    st.session_state.history_k
                )
                
                # 3) プロンプト構築
                prompt = build_prompt(
                    history_text=history_text,
                    context_text=context_text,
                    user_query=user_query,
                    service_name=service["name"],
                )
                
                # 4) LLM呼び出し（SQL経由）
                placeholder = st.empty()
                try:
                    answer = cortex_complete(
                        session=session,
                        model=st.session_state.selected_model,
                        prompt=prompt
                    )
                except Exception as e:
                    answer = f"❌ エラーが発生しました: {str(e)}"
                
                # 疑似ストリーミング表示
                stream_text(placeholder, answer)
            
            # 参照コンテキスト表示
            render_context_expander(context_rows)
        
        # 履歴に保存
        turn = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "question": user_query,
            "answer": answer,
            "model": st.session_state.selected_model,
            "contexts": context_rows,
        }
        st.session_state.rag_chat_history.append(turn)


if __name__ == "__main__":
    main()

