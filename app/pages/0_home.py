# =========================================================
# ホームページ
# =========================================================
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Snowflakeセッションの取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# メインページコンテンツ
# =========================================================
st.title("🏛️ 年金基金 サステナビリティ分析システム")
st.markdown("### Snowflake Cortex AI を活用したサステナビリティレポート分析")

st.markdown("---")

# アプリケーション概要
st.markdown("""
### 📋 このアプリケーションについて

本アプリケーションは、**Snowflake Cortex AI** の各種機能を活用し、
年金基金および運用機関のサステナビリティレポートを分析するシステムです。

| 使用技術 | 説明 |
|------|------|
| **AI_COMPLETE** | テキスト生成・要約・分析 |
| **Cortex Search** | セマンティック検索 |
| **Cortex Agent** | 自然言語での問い合わせ処理 |
| **AI_PARSE_DOCUMENT** | PDFからのテキスト抽出 |
""")

st.markdown("---")

# ページ構成
st.markdown("### 📚 ページ構成")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    #### 📊 グローバル年金分析
    
    海外主要年金基金のサステナビリティレポートを
    AI_COMPLETEで分析します。
    
    **🛠️ 使用技術:** `AI_COMPLETE`
    
    **📌 主な機能:**
    - レポートの要約・サマライズ
    - 複数レポートからのトレンド抽出
    - GPIFとのGAP分析
    - 新規レポートのアップロード
    
    **💡 ユースケース:**
    - グローバルESGトレンドの把握
    - ベンチマーク比較分析
    """)

with col2:
    st.markdown("""
    #### 🤖 スチュワードシップ原則評価
    
    GPIFスチュワードシップ活動原則に基づき、
    運用機関のレポートをCortex Agentで評価します。
    
    **🛠️ 使用技術:** `Cortex Agent`
    
    **📌 主な機能:**
    - 自然言語での検索・問い合わせ
    - 5原則への対応度評価
    - 総合評価レポート生成
    
    **💡 ユースケース:**
    - 運用機関の原則対応状況確認
    - 複数機関の比較分析
    """)

with col3:
    st.markdown("""
    #### 🔍 Cortex Search RAG
    
    Cortex Agentを使用せず、Cortex Search + 
    AI_COMPLETEで直接RAG検索を行います。
    
    **🛠️ 使用技術:** `Cortex Search` + `AI_COMPLETE`
    
    **📌 主な機能:**
    - セマンティック検索
    - LLMモデル選択可能
    - 会話履歴機能
    
    **💡 ユースケース:**
    - コスト最適化されたRAG検索
    - シンプルな文書検索
    """)

st.markdown("---")

# GPIFスチュワードシップ活動原則の概要
st.markdown("### 📜 GPIFスチュワードシップ活動原則（5原則）")

principles = [
    ("原則1", "運用受託機関におけるコーポレート・ガバナンス体制"),
    ("原則2", "運用受託機関における利益相反管理"),
    ("原則3", "エンゲージメントを含むスチュワードシップ活動方針"),
    ("原則4", "投資におけるESGなどのサステナビリティの考慮"),
    ("原則5", "議決権行使")
]

cols = st.columns(5)
for idx, (key, title) in enumerate(principles):
    with cols[idx]:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            height: 120px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        ">
            <p style="color: #ffffff; font-weight: bold; margin: 0; font-size: 14px;">{key}</p>
            <p style="color: #e0e0e0; font-size: 11px; margin-top: 8px; line-height: 1.3;">{title}</p>
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")
st.info("👈 **使い方**: サイドバーから各ページに移動してください。")

# フッター
st.markdown("---")
st.caption("© 2025 年金基金 サステナビリティ分析システム | Powered by Snowflake Cortex AI")

