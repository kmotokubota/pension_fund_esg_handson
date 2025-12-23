# =========================================================
# GPIFスチュワードシップ活動原則 対応度評価アプリ (Cortex Agent版)
# =========================================================

import streamlit as st
import pandas as pd
import json
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import _snowflake

# =========================================================
# ページ設定
# =========================================================
st.set_page_config(
    page_title="スチュワードシップ原則評価",
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

pd.set_option("max_colwidth", None)
pd.set_option('display.max_columns', None)

# =========================================================
# 設定値
# =========================================================
AGENT_DATABASE = "SNOWFLAKE_INTELLIGENCE"
AGENT_SCHEMA = "AGENTS"
AGENT_NAME = "GPIF_STEWARDSHIP_AGENT"
AGENT_FULL_NAME = f"{AGENT_DATABASE}.{AGENT_SCHEMA}.{AGENT_NAME}"

API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000

DATA_DATABASE = "DEMO_DB"
DATA_SCHEMA = "DEMO_SUSTAINABILITY"
DATA_VIEW = "COMBINED_SUSTAINABILITY_CHUNKS_VIEW"

CORTEX_SEARCH_DATABASE = "DEMO_DB"
CORTEX_SEARCH_SCHEMA = "DEMO_SUSTAINABILITY"
CORTEX_SEARCH_SERVICE = "SUSTAINABILITY_REPORT"
CORTEX_SEARCH_TOOL_NAME = "sustainability_report_search_tool"
CORTEX_SEARCH_MAX_RESULTS = 12
CORTEX_SEARCH_ID_COLUMN = "SCOPED_FILE_URL"
CORTEX_SEARCH_TITLE_COLUMN = "RELATIVE_PATH"

# =========================================================
# GPIFのスチュワードシップ活動原則（5つの原則）
# =========================================================
GPIF_PRINCIPLES = {
    "原則1": {
        "title": "運用受託機関におけるコーポレート・ガバナンス体制",
        "description": """○運用受託機関は、日本版スチュワードシップ・コードを受け入れること。
○運用受託機関は、自らのコーポレート・ガバナンス体制を整えること。特に、運用機関としての独立性、透明性を高めるため、独立性の高い社外取締役を導入する等、監督の仕組みを整えること。
○運用受託機関は、スチュワードシップ責任を実効的に果たすための組織・体制の構築、人材育成を行うこと。
○運用受託機関は、役職員の報酬体系がどのように受益者の利益に合致しているか説明を行うこと。"""
    },
    "原則2": {
        "title": "運用受託機関における利益相反管理",
        "description": """○運用受託機関は、受益者の利益を第一として行動するために、適切に利益相反（企業グループに所属する場合には、グループ内における利益相反を含む。）を管理すること。管理に当たっては、利益相反の種類を資本関係、取引関係等に類型化した上で、管理方針を策定し、公表すること。
○運用受託機関は、独立性の高い第三者委員会の設置等、利益相反を防止するための体制・仕組みを構築し、公表すること。第三者委員会の構成は、独立性、経験等も十分考慮して検討すること。
○運用受託機関は、自社又は親会社、グループ会社等の利害関係先に対して議決権行使を行う場合、第三者委員会等による行使判断や妥当性の検討、議決権行使助言会社の推奨の適用等、恣意性を排除し、ガバナンスのベストプラクティスを追求する仕組みを整え、公表すること。"""
    },
    "原則3": {
        "title": "エンゲージメントを含むスチュワードシップ活動方針",
        "description": """○運用受託機関は、エンゲージメントを含むスチュワードシップ活動を実施するに当たり、スチュワードシップ活動方針を策定し、公表すること。
○運用受託機関は、エンゲージメントを含むスチュワードシップ活動についてはショートターミズムに陥らないよう、長期の視点からリスク調整後のリターン向上に資する内容、質を重視して取り組むこと。また、実効的な活動が行えるよう、アクションプランの策定等も検討すること。
○運用受託機関は、エンゲージメントを含むスチュワードシップ活動と運用の連携を図ること。
○運用受託機関は、インデックス構成が投資パフォーマンスを大きく左右する要素であることを踏まえ、インデックス会社が実施するコンサルテーションの機会を活用する等、受益者の利益のため、積極的にエンゲージメントを行うこと。
○運用受託機関は、市場全体の持続的成長の観点から、企業やインデックス会社にとどまらず関係者と幅広くエンゲージメントを行うこと。
○運用受託機関は、コーポレート・ガバナンスに関する報告書、統合報告書等に記載の非財務情報も十分に活用し企業とエンゲージメントを行うこと。
○運用受託機関は、各国のコーポレートガバナンス・コード又はそれに準ずるものの各原則において、企業が「実施しない理由」を説明している項目について、企業の考えを十分にヒアリングすること。
○特に、株式のパッシブ運用を行う運用受託機関は、市場全体の持続的成長を目指す観点から、エンゲージメントの戦略を立案し、実効性のある取組みを実践すること。
○運用受託機関は、エンゲージメント代行会社を利用する場合、採用に当たり、組織体制、人員等についてデューディリジェンスを実施するとともに、採用後にはサービス内容についてモニタリング・評価を継続的に行い、必要に応じてエンゲージメントを行うこと。"""
    },
    "原則4": {
        "title": "投資におけるESGなどのサステナビリティの考慮",
        "description": """○投資においてESG（環境・社会・ガバナンス）などのサステナビリティを適切に考慮することは、運用資産の長期的な投資収益拡大の観点から、企業価値の向上や投資先及び市場全体の持続的成長に資すると考えられることから、運用受託機関は、セクターにおける重要性、投資先の実情等を踏まえて、ESGなどのサステナビリティ課題に取り組むこと。
○運用受託機関は、重大なESGなどのサステナビリティ課題について、投資家として考える目標を示し、積極的にエンゲージメントを行うこと。
○運用受託機関は、PRI（責任投資原則）への署名を行うこと。また、ESGなどのサステナビリティに関する様々なイニシアティブに積極的に参加すること。"""
    },
    "原則5": {
        "title": "議決権行使",
        "description": """○運用受託機関は、議決権の行使について、GPIFから委託されたものであることを十分認識し、受託者責任の観点から専ら受益者の利益のために議決権を行使すること。
○運用受託機関は、企業価値向上を促すエンゲージメントの一環として、別に定める議決権行使原則のとおり、議決権を行使すること。
○運用受託機関は、議決権行使において議決権行使助言会社を利用する場合、採用に当たり、組織体制、人員等についてデューディリジェンスを実施するとともに、採用後にはその助言内容についてモニタリング・評価を継続的に行い、必要に応じてエンゲージメントを行うこと（利益相反管理を目的とする場合は除く。）。"""
    }
}

# 対象運用機関
ASSET_MANAGERS = {
    "AMOne": "アセットマネジメントOne",
    "SMTAM": "三井住友トラスト・アセットマネジメント",
    "りそな": "りそなアセットマネジメント",
    "MUTB": "三菱UFJ信託銀行"
}

# =========================================================
# セッション状態の初期化
# =========================================================
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'agent_session_id' not in st.session_state:
    st.session_state.agent_session_id = None
if 'selected_company' not in st.session_state:
    st.session_state.selected_company = "全社"
if 'evaluation_results' not in st.session_state:
    st.session_state.evaluation_results = None
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

# =========================================================
# Agent API関数
# =========================================================
def build_agent_payload(user_message: str, session_id: str | None = None) -> dict:
    """Agent呼び出し用のpayloadを生成"""
    target_company = st.session_state.get('selected_company', "全社")
    company_context = ""
    search_filter = ""
    
    if target_company and target_company != "全社":
        company_display = ASSET_MANAGERS.get(target_company, target_company)
        company_context = (
            f"\n【重要】対象運用機関: {company_display}（ファイル名に含まれるコード: {target_company.lower()}）\n"
            f"**必ず{company_display}のレポート（ファイル名に'{target_company.lower()}'を含むもの）からのみ情報を取得してください。**\n"
            f"**他の運用機関（SMTAM、MUTB、りそななど）の情報は絶対に使用しないでください。**\n"
        )
        search_filter = f"ファイル名に'{target_company.lower()}'を含む"

    selected_file = st.session_state.get('selected_file')
    file_context = ""
    if selected_file:
        file_context = (
            f"\n【重要】対象レポート: {selected_file}\n"
            f"**必ずこのレポート（{selected_file}）からのみ情報を取得してください。**\n"
            "**他のレポートの情報は絶対に使用しないでください。**\n"
        )

    guidance_text = (
        "以下のルールに厳密に従って回答してください。\n"
        "1. 必ずCortex Search (sustainability_report_search_tool) を使って関連情報を検索すること。\n"
        "2. 検索結果の引用に基づいて回答すること。\n"
        "3. 引用する際はファイル名とページ番号を必ず明示すること。\n"
        "4. **検索時は、指定された運用機関またはレポートに関連する情報のみを取得すること。**\n"
        "5. **評価の際は、具体的な数値や詳細な記載がなくても、原則の趣旨に沿った取り組みや方針が示されていれば「対応している」と判断すること。**\n"
    )
    
    if search_filter:
        guidance_text += f"6. **検索条件: {search_filter}**\n"

    message_text = (
        guidance_text
        + company_context
        + file_context
        + "\n【質問】\n"
        + user_message
    )

    payload = {
        "agent": AGENT_FULL_NAME,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": message_text
                    }
                ]
            }
        ],
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": CORTEX_SEARCH_TOOL_NAME
                }
            }
        ],
        "tool_resources": {
            CORTEX_SEARCH_TOOL_NAME: {
                "name": f"{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_SERVICE}",
                "max_results": CORTEX_SEARCH_MAX_RESULTS,
                "id_column": CORTEX_SEARCH_ID_COLUMN,
                "title_column": CORTEX_SEARCH_TITLE_COLUMN
            }
        }
    }

    if session_id:
        payload["session_id"] = session_id

    return payload

def call_agent_api(message: str, session_id: str = None):
    """Cortex Agent APIを呼び出してメッセージを送信"""
    try:
        request_body = build_agent_payload(message, session_id)
        
        if st.session_state.debug_mode:
            st.sidebar.markdown("### 送信するPayload")
            st.sidebar.json(request_body)

        response = _snowflake.send_snow_api_request(
            "POST",
            API_ENDPOINT,
            {},
            {},
            request_body,
            {},
            API_TIMEOUT
        )
        
        if response:
            if isinstance(response, dict) and 'status' in response:
                if response['status'] != 200:
                    if st.session_state.debug_mode:
                        st.sidebar.error(f"APIエラー: ステータスコード {response['status']}")
                    return None
                
                content = response.get('content', '')
                
                if isinstance(content, str):
                    try:
                        response_json = json.loads(content)
                    except json.JSONDecodeError:
                        if st.session_state.debug_mode:
                            st.sidebar.error("JSON解析失敗")
                        return None
                else:
                    response_json = content
            else:
                if isinstance(response, (dict, list)):
                    response_json = response
                elif isinstance(response, str):
                    try:
                        response_json = json.loads(response)
                    except json.JSONDecodeError:
                        return None
                else:
                    response_json = response
            
            return response_json
        else:
            return None
            
    except Exception as e:
        if st.session_state.debug_mode:
            st.sidebar.error(f"API呼び出しエラー: {str(e)}")
        return None

def send_message_to_agent(message: str):
    """Cortex Agentにメッセージを送信して応答を取得"""
    try:
        response = call_agent_api(message, st.session_state.agent_session_id)
        
        if not response:
            return None
        
        session_id = None
        content_text = ""
        citations = []
        
        # イベントストリーム形式のレスポンスを処理
        if isinstance(response, list):
            for event in response:
                if not isinstance(event, dict):
                    continue
                
                event_type = event.get('event', '')
                event_data = event.get('data', {})
                
                if isinstance(event_data, str):
                    try:
                        event_data = json.loads(event_data)
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if event_type == 'response.text':
                    if isinstance(event_data, dict) and 'text' in event_data:
                        content_text = event_data['text']
                
                elif event_type == 'response.text.annotation':
                    if isinstance(event_data, dict) and 'annotation' in event_data:
                        annotation = event_data['annotation']
                        if annotation.get('type') == 'cortex_search_citation':
                            citation = {
                                'doc_id': annotation.get('doc_id', ''),
                                'doc_title': annotation.get('doc_title', ''),
                                'text': annotation.get('text', ''),
                                'index': annotation.get('index', 0),
                                'search_result_id': annotation.get('search_result_id', '')
                            }
                            citations.append(citation)
                
                elif event_type == 'response':
                    if isinstance(event_data, dict):
                        if 'content' in event_data and isinstance(event_data['content'], list):
                            for content_item in event_data['content']:
                                if isinstance(content_item, dict):
                                    if content_item.get('type') == 'citations':
                                        additional_citations = content_item.get('citations', [])
                                        citations.extend(additional_citations)
                        
                        if 'citations' in event_data:
                            additional_citations = event_data['citations']
                            citations.extend(additional_citations)
                
                # セッションIDの取得
                if 'session_id' in event:
                    session_id = event['session_id']
                elif isinstance(event_data, dict) and 'session_id' in event_data:
                    session_id = event_data['session_id']
        
        # messageフィールドがある場合
        elif isinstance(response, dict) and 'message' in response:
            message_content = response['message']
            
            if 'content' in message_content:
                if isinstance(message_content['content'], str):
                    content_text = message_content['content']
                elif isinstance(message_content['content'], list):
                    for content_item in message_content['content']:
                        if isinstance(content_item, dict) and content_item.get('type') == 'text':
                            content_text += content_item.get('text', '')
            
            if 'citations' in message_content:
                citations = message_content['citations']
            elif 'citations' in response:
                citations = response['citations']
            
            session_id = response.get('session_id')
        
        # セッションIDの保存
        if session_id and not st.session_state.agent_session_id:
            st.session_state.agent_session_id = session_id
        
        if not content_text:
            return None
        
        return {
            'content': content_text,
            'citations': citations,
            'session_id': session_id
        }
        
    except Exception as e:
        if st.session_state.debug_mode:
            st.sidebar.error(f"エラー: {str(e)}")
        return None

def create_agent_session():
    """Agentセッションをリセット"""
    st.session_state.agent_session_id = None
    return None

def evaluate_principle_with_agent(principle_key, principle_data, company_name):
    """特定の原則に対する評価をAgentで実行"""
    principle_details = principle_data.get('description', '')
    
    if company_name == "全社":
        query = f"""
{principle_key}: {principle_data['title']}

【原則の詳細】
{principle_details}

上記のGPIFスチュワードシップ活動原則について、各運用機関（AMOne、SMTAM、りそな、MUTB）の対応状況を分析してください。

**【重要】以下のフォーマットで回答してください：**

## 評価結果

### 原則の各項目に対する各社の対応状況

原則の詳細に記載されている各項目（○で始まる項目）ごとに、以下の形式で評価してください：

---

**○ [GPIFの要求事項をそのまま記載]**

**AMOne:**
- [取り組み内容・方針を簡潔に要約]
- 評価: ✅ 対応している / ⚠️ 部分的に対応 / ❌ 情報なし

**SMTAM:**
- [取り組み内容・方針を簡潔に要約]
- 評価: ✅ 対応している / ⚠️ 部分的に対応 / ❌ 情報なし

**りそな:**
- [取り組み内容・方針を簡潔に要約]
- 評価: ✅ 対応している / ⚠️ 部分的に対応 / ❌ 情報なし

**MUTB:**
- [取り組み内容・方針を簡潔に要約]
- 評価: ✅ 対応している / ⚠️ 部分的に対応 / ❌ 情報なし

---

（次の項目も同様に繰り返す）

### 総合評価

[各社の対応状況を比較した総合的な評価コメント]

---

**評価基準:**
- ✅ 対応している: 原則の趣旨に沿った取り組み・方針・姿勢が確認できる場合（具体的な数値や詳細がなくても可）
- ⚠️ 部分的に対応: 一部の要素のみ対応、または間接的な言及にとどまる場合
- ❌ 情報なし: レポート内に関連する記載が全く見つからない場合

**注意事項:**
- 検索結果に基づいて回答すること
- 原則の趣旨に沿った内容であれば、詳細な記載がなくても「✅ 対応している」と判断して良い
"""
    else:
        query = f"""
{principle_key}: {principle_data['title']}

【原則の詳細】
{principle_details}

上記のGPIFスチュワードシップ活動原則について、{ASSET_MANAGERS.get(company_name, company_name)}の対応状況を分析してください。

**【重要】以下のフォーマットで回答してください：**

## 評価結果

### 原則の各項目に対する対応状況

原則の詳細に記載されている各項目（○で始まる項目）ごとに、以下の形式で評価してください：

---

**○ [GPIFの要求事項をそのまま記載]**

**対応状況:**
- [取り組み内容・方針・姿勢を簡潔に要約（2-3文程度）]
- [検索結果から得られた関連情報]

**評価:** ✅ 対応している / ⚠️ 部分的に対応 / ❌ 情報なし

---

（次の項目も同様に繰り返す）

### 総合評価

[原則全体に対する総合的な評価コメント（3-4文程度）]

---

**評価基準:**
- ✅ 対応している: 原則の趣旨に沿った取り組み・方針・姿勢が確認できる場合（具体的な数値や詳細がなくても可）
- ⚠️ 部分的に対応: 一部の要素のみ対応、または間接的な言及にとどまる場合
- ❌ 情報なし: レポート内に関連する記載が全く見つからない場合

**注意事項:**
- 検索結果に基づいて回答すること
- 原則の趣旨に沿った内容であれば、詳細な記載がなくても「✅ 対応している」と判断して良い
"""
    
    with st.spinner(f'{principle_key}の評価中...'):
        response = send_message_to_agent(query)
        
        if response:
            return {
                'principle': principle_key,
                'title': principle_data['title'],
                'query': query,
                'response': response.get('content', '応答を取得できませんでした'),
                'citations': response.get('citations', [])
            }
        else:
            return {
                'principle': principle_key,
                'title': principle_data['title'],
                'query': query,
                'response': 'エラー: 応答を取得できませんでした',
                'citations': []
            }

# =========================================================
# UI
# =========================================================
st.title("GPIFスチュワードシップ活動原則 対応度評価")
st.caption("Snowflake Cortex Agentを使用して、GPIFスチュワードシップ活動原則に基づき、運用機関のサステナビリティレポートを解読し、各原則への対応状況を評価します")

# サイドバー
with st.sidebar:
    st.markdown("### 評価設定")
    
    st.markdown("**対象運用機関**")
    selected_company = st.selectbox(
        "評価対象を選択してください",
        options=["全社"] + list(ASSET_MANAGERS.keys()),
        label_visibility="collapsed"
    )
    
    st.session_state.selected_company = selected_company
    
    if selected_company == "全社":
        st.caption("全運用機関を対象に横断分析します")
    else:
        st.caption(f"選択中: {ASSET_MANAGERS[selected_company]}")
    
    st.markdown("---")
    
    st.markdown("**GPIF 5つの原則**")
    for key, principle in GPIF_PRINCIPLES.items():
        with st.expander(f"{key}: {principle['title']}"):
            description_lines = principle['description'].strip().split('\n')
            for line in description_lines:
                if line.strip():
                    st.markdown(line)
    
    st.markdown("---")
    
    st.markdown("**デバッグモード**")
    debug_mode = st.checkbox(
        "デバッグ情報を表示",
        value=st.session_state.debug_mode
    )
    if debug_mode != st.session_state.debug_mode:
        st.session_state.debug_mode = debug_mode
        st.rerun()
    
    st.markdown("---")
    
    st.markdown("**セッション管理**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("セッション更新", use_container_width=True):
            create_agent_session()
            st.session_state.chat_history = []
            st.success("新しいセッションを作成しました")
            st.rerun()
    
    with col2:
        if st.button("履歴クリア", use_container_width=True):
            st.session_state.chat_history = []
            st.success("チャット履歴をクリアしました")
            st.rerun()
    
    if st.session_state.agent_session_id:
        st.caption(f"セッションID: {st.session_state.agent_session_id[:8]}...")
    
    st.markdown("---")
    st.caption(f"Agent: {AGENT_NAME}")
    st.caption(f"データソース: {DATA_DATABASE}.{DATA_SCHEMA}")

# =========================================================
# タブ構成
# =========================================================
tab1, tab2, tab3 = st.tabs(["自然言語検索", "原則別評価", "総合レポート"])

# ========================================
# タブ1: 自然言語検索
# ========================================
with tab1:
    col1, col2 = st.columns([4, 1])
    with col1:
        st.header("自然言語での検索・問い合わせ")
    with col2:
        if st.button("履歴クリア", key="clear_tab1", use_container_width=True):
            st.session_state.chat_history = []
            st.success("クリアしました")
            st.rerun()
    
    st.caption("""
    自由な形式で質問してください。Cortex Agentが適切な情報を検索し、分析結果を提供します。
    
    質問例:
    - 「AMOneのスチュワードシップ責任に関する方針について教えてください」
    - 「各社の利益相反管理の具体的な取り組みを比較してください」
    - 「SMTAMの投資先企業との対話の実績を教えてください」
    """)
    
    st.markdown("---")
    
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.chat_history:
            with st.chat_message(message['role']):
                st.markdown(message['content'])
                
                if 'citations' in message and message['citations']:
                    with st.expander(f"参照資料 ({len(message['citations'])}件)"):
                        for idx, citation in enumerate(message['citations'], 1):
                            title = (citation.get('doc_title') or 
                                   citation.get('title') or 
                                   citation.get('file_name') or 'N/A')
                            text = (citation.get('text') or 
                                  citation.get('content') or 'N/A')
                            
                            st.markdown(f"**[{idx}] {title}**")
                            st.caption(f"> {text[:300]}..." if len(str(text)) > 300 else text)
                            st.markdown("---")
    
    user_query = st.chat_input("質問を入力してください...")
    
    if user_query:
        st.session_state.chat_history.append({
            'role': 'user',
            'content': user_query
        })
        
        with st.chat_message("user"):
            st.markdown(user_query)
        
        with st.chat_message("assistant"):
            with st.spinner("Cortex Agentが分析中..."):
                response = send_message_to_agent(user_query)
            
            if response:
                response_content = response.get('content', '応答を取得できませんでした')
                citations = response.get('citations', [])
                
                st.markdown(response_content)
                
                if citations:
                    with st.expander(f"参照資料 ({len(citations)}件)"):
                        for idx, citation in enumerate(citations, 1):
                            title = (citation.get('doc_title') or 
                                   citation.get('title') or 
                                   citation.get('file_name') or 'N/A')
                            text = (citation.get('text') or 
                                  citation.get('content') or 'N/A')
                            
                            st.markdown(f"**[{idx}] {title}**")
                            st.caption(f"> {text[:300]}..." if len(str(text)) > 300 else text)
                            st.markdown("---")
                
                st.session_state.chat_history.append({
                    'role': 'assistant',
                    'content': response_content,
                    'citations': citations
                })
            else:
                error_msg = "申し訳ございません。応答の取得に失敗しました。"
                st.error(error_msg)
                st.session_state.chat_history.append({
                    'role': 'assistant',
                    'content': error_msg,
                    'citations': []
                })
        
        st.rerun()

# ========================================
# タブ2: 原則別評価
# ========================================
with tab2:
    st.header("GPIFスチュワードシップ原則 対応度評価")
    
    st.caption(f"評価対象: {ASSET_MANAGERS.get(st.session_state.selected_company, st.session_state.selected_company) if st.session_state.selected_company != '全社' else '全運用機関'}")
    
    st.markdown("---")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("全原則を一括評価", type="primary", use_container_width=True):
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (key, principle) in enumerate(GPIF_PRINCIPLES.items()):
                status_text.text(f"{key}を評価中...")
                result = evaluate_principle_with_agent(
                    key, 
                    principle, 
                    st.session_state.selected_company
                )
                results.append(result)
                progress_bar.progress((idx + 1) / len(GPIF_PRINCIPLES))
            
            st.session_state.evaluation_results = results
            status_text.text("評価完了")
            progress_bar.empty()
            st.rerun()
    
    st.markdown("---")
    
    st.markdown("**原則ごとの詳細評価**")
    
    for key, principle in GPIF_PRINCIPLES.items():
        with st.expander(f"{key}: {principle['title']}", expanded=False):
            st.markdown("**説明:**")
            description_lines = principle['description'].strip().split('\n')
            for line in description_lines:
                if line.strip():
                    st.markdown(line)
            st.markdown("---")
            
            if st.button(f"この原則を評価", key=f"eval_{key}"):
                result = evaluate_principle_with_agent(
                    key, 
                    principle, 
                    st.session_state.selected_company
                )
                
                if st.session_state.evaluation_results is None:
                    st.session_state.evaluation_results = [result]
                else:
                    updated = False
                    for i, existing_result in enumerate(st.session_state.evaluation_results):
                        if existing_result['principle'] == key:
                            st.session_state.evaluation_results[i] = result
                            updated = True
                            break
                    
                    if not updated:
                        st.session_state.evaluation_results.append(result)
                
                st.markdown("**評価結果**")
                st.markdown(result['response'])
                
                if result['citations']:
                    with st.expander(f"参照資料 ({len(result['citations'])}件)", expanded=False):
                        for idx, citation in enumerate(result['citations'], 1):
                            title = citation.get('doc_title') or citation.get('title') or citation.get('file_name') or 'N/A'
                            text = citation.get('text') or citation.get('content') or ''
                            
                            if '/' in str(title):
                                display_title = title.split('/')[-1]
                            else:
                                display_title = title
                            
                            st.markdown(f"**[{idx}] {display_title}**")
                            
                            if text and len(str(text)) > 200:
                                st.caption(str(text)[:200] + "...")
                            elif text:
                                st.caption(text)
                            
                            if idx < len(result['citations']):
                                st.markdown("---")
                
                st.success(f"{key}の評価結果を総合レポートに保存しました")

# ========================================
# タブ3: 総合レポート
# ========================================
with tab3:
    st.header("総合評価レポート")
    
    if st.session_state.evaluation_results is None:
        st.info("まず「原則別評価」タブで評価を実行してください")
    else:
        st.caption(f"評価対象: {ASSET_MANAGERS.get(st.session_state.selected_company, st.session_state.selected_company) if st.session_state.selected_company != '全社' else '全運用機関'}")
        st.caption(f"評価日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
        
        st.markdown("---")
        
        st.markdown("**評価サマリー**")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("評価原則数", f"{len(GPIF_PRINCIPLES)}原則")
        with col2:
            total_citations = sum([len(r.get('citations', [])) for r in st.session_state.evaluation_results])
            st.metric("参照資料数", f"{total_citations}件")
        with col3:
            avg_citations = total_citations / len(GPIF_PRINCIPLES) if len(GPIF_PRINCIPLES) > 0 else 0
            st.metric("原則あたり平均参照数", f"{avg_citations:.1f}件")
        
        st.markdown("---")
        
        st.markdown("**原則別評価結果**")
        
        for result in st.session_state.evaluation_results:
            st.markdown(f"### {result['principle']}: {result['title']}")
            
            st.markdown("**評価**")
            st.markdown(result['response'])
            
            citations = result.get('citations', [])
            if citations:
                with st.expander(f"参照資料 ({len(citations)}件)", expanded=False):
                    for idx, citation in enumerate(citations, 1):
                        title = citation.get('doc_title') or citation.get('title') or citation.get('file_name') or 'N/A'
                        text = citation.get('text') or citation.get('content') or ''
                        
                        if '/' in str(title):
                            display_title = title.split('/')[-1]
                        else:
                            display_title = title
                        
                        st.markdown(f"**[{idx}] {display_title}**")
                        
                        if text and len(str(text)) > 200:
                            st.caption(str(text)[:200] + "...")
                        elif text:
                            st.caption(text)
                        
                        if idx < len(citations):
                            st.markdown("---")
            
            st.markdown("---")
        
        st.markdown("**レポートのエクスポート**")
        
        report_text = f"""
GPIFスチュワードシップ活動原則 対応度評価レポート (Cortex Agent版)
評価日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}
評価対象: {ASSET_MANAGERS.get(st.session_state.selected_company, st.session_state.selected_company) if st.session_state.selected_company != '全社' else '全運用機関'}

{'='*80}

"""
        
        for result in st.session_state.evaluation_results:
            report_text += f"""
{result['principle']}: {result['title']}
{'-'*80}

【評価結果】
{result['response']}

【参照資料数】
{len(result.get('citations', []))}件

{'='*80}

"""
        
        st.download_button(
            label="評価レポートをダウンロード",
            data=report_text,
            file_name=f"gpif_agent_evaluation_{st.session_state.selected_company}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain",
            type="primary"
        )

# フッター
st.markdown("---")
st.caption("GPIF スチュワードシップ活動原則 対応度評価システム")
