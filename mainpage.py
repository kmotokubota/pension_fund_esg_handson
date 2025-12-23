# =========================================================
# GPIF向け Snowflake Cortex ハンズオン
# GPIFスチュワードシップ活動分析アプリケーション
# =========================================================
# Created for GPIF Handson
# 最終更新: 2025/12/18
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
import streamlit as st
from snowflake.snowpark.context import get_active_session

# =========================================================
# ページ設定とセッション初期化
# =========================================================
st.set_page_config(
    page_title="GPIF スチュワードシップ分析",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッションの取得
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# メインページコンテンツ
# =========================================================
def render_home_page():
    """ホームページを表示"""
    st.title("🏛️ GPIF スチュワードシップ活動 分析システム")
    st.markdown("### Snowflake Cortex AI を活用したサステナビリティレポート分析")
    
    st.markdown("---")
    
    # 基本情報を2列で表示
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 📋 このアプリケーションについて
        
        本アプリケーションは、**Snowflake Cortex AI** の各種機能を活用し、
        GPIFおよび運用機関のサステナビリティレポートを分析するシステムです。
        
        ---
        
        #### 🔹 主な機能
        
        **1. グローバル年金基金サステナビリティ分析**
        - 海外主要年金基金のレポート要約
        - トレンド分析・共通項抽出
        - GPIFとのGAP分析
        
        **2. スチュワードシップ原則 対応度評価**
        - GPIFスチュワードシップ活動原則（5原則）に基づく評価
        - Cortex Agentを活用した自然言語検索
        - 運用機関別の対応状況分析
        """)
        
    with col2:
        st.markdown("""
        ### 🛠️ 使用するSnowflake Cortex機能
        
        | 機能 | 説明 |
        |------|------|
        | **AI_COMPLETE** | テキスト生成・要約・分析 |
        | **Cortex Search** | セマンティック検索 |
        | **Cortex Agent** | 自然言語での問い合わせ処理 |
        | **AI_PARSE_DOCUMENT** | PDFからのテキスト抽出 |
        
        ---
        
        ### 🎯 対象データ
        
        - **GPIFサステナビリティレポート**
        - **海外年金基金レポート**
          - CalPERS / CalSTRS (米国)
          - CPP Investments (カナダ)
          - Norges Bank (ノルウェー)
          - Temasek (シンガポール) 等
        - **国内運用機関レポート**
          - AMOne / SMTAM / りそな / MUTB
        """)

    st.markdown("---")
    
    # ワークショップ手順
    st.markdown("### 📚 ページ構成")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### 📊 ページ1: グローバル年金分析
        
        海外主要年金基金のサステナビリティレポートを分析し、
        グローバルトレンドとGPIFとの比較を行います。
        
        **主な機能:**
        - レポートのサマライズ
        - 複数レポートからのトレンド抽出
        - GPIFとのGAP分析
        - 新規レポートの追加
        """)
    
    with col2:
        st.markdown("""
        #### 🤖 ページ2: スチュワードシップ原則評価
        
        GPIFスチュワードシップ活動原則に基づき、
        運用機関のレポートを評価します。
        
        **主な機能:**
        - 自然言語での検索・問い合わせ
        - 原則別の対応度評価
        - 総合評価レポート生成
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

# =========================================================
# メインアプリケーション
# =========================================================
def main():
    """メインアプリケーション"""
    
    # メインページを表示
    render_home_page()
    
    # フッター
    st.markdown("---")
    st.caption("© 2025 GPIF スチュワードシップ活動分析システム | Powered by Snowflake Cortex AI")

if __name__ == "__main__":
    main()

