# =========================================================
# 年金基金 ESG/サステナビリティ分析 ハンズオン
# Snowflake Cortex AI を活用したサステナビリティレポート分析
# =========================================================
# 最終更新: 2025/01
# =========================================================

# =========================================================
# 必要なライブラリのインポート
# =========================================================
import streamlit as st
from snowflake.snowpark.context import get_active_session

# =========================================================
# ページ設定
# =========================================================
st.set_page_config(
    page_title="年金基金 ESG分析",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# ナビゲーション設定（日本語サイドバー + 英語ファイル名）
# =========================================================
home_page = st.Page("pages/0_home.py", title="ホーム", icon="🏠", default=True)
page1 = st.Page("pages/1_global_analysis.py", title="グローバル年金分析", icon="📊")
page2 = st.Page("pages/2_stewardship_evaluation.py", title="スチュワードシップ原則評価", icon="🤖")
page3 = st.Page("pages/3_cortex_search_rag.py", title="Cortex Search RAG", icon="🔍")

pg = st.navigation([home_page, page1, page2, page3])
pg.run()
