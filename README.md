# 年金基金 ESG/サステナビリティ分析 ハンズオン

Snowflake Cortex AI を活用した年金基金サステナビリティレポート分析のハンズオン資料です。

## 概要

このハンズオンでは、以下の Snowflake Cortex AI 機能を学習します：

- **AI_PARSE_DOCUMENT**: PDFファイルからテキストを抽出
- **SPLIT_TEXT_RECURSIVE_CHARACTER**: テキストのチャンク化
- **Cortex Search**: ベクトル検索によるRAG（検索拡張生成）
- **Cortex Agent**: 対話型AIエージェント
- **AI_COMPLETE**: LLMによるテキスト生成

## ディレクトリ構成

```
pension_fund_esg_handson/
├── README.md
├── setup.sql                    # セットアップSQL
├── environment.yml              # Python依存パッケージ
├── mainpage.py                  # Streamlitメインページ
├── pages/
│   ├── _1_グローバル年金分析.py    # グローバル年金基金分析アプリ
│   └── _2_スチュワードシップ原則評価.py  # スチュワードシップ原則評価アプリ
├── am_esg_report/               # 運用機関サステナビリティレポート
└── global_pf_esg_report/        # 海外年金基金サステナビリティレポート
```

## ハンズオンの流れ（3時間）

### セッション1: 環境構築（60分）
1. データベース・スキーマ・ウェアハウスの作成
2. クロスリージョン推論の有効化
3. Git統合・API統合の設定
4. PDFファイルのステージへの取り込み

### セッション2: データ準備（60分）
1. AI_PARSE_DOCUMENTによるPDFテキスト抽出
2. SPLIT_TEXT_RECURSIVE_CHARACTERによるチャンク化
3. 統合ビューの作成

### セッション3: Cortex AI活用（60分）
1. Cortex Search Serviceの作成
2. Cortex Agentの作成
3. Streamlitアプリでの動作確認

## 前提条件

- Snowflakeアカウント（ACCOUNTADMIN権限推奨）
- クロスリージョン推論が利用可能であること

## クイックスタート

### 1. Git統合によるセットアップ

Snowflakeワークシートで以下を実行：

```sql
USE ROLE ACCOUNTADMIN;

-- API統合の作成
CREATE OR REPLACE API INTEGRATION git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/kmotokubota/')
    ENABLED = TRUE;

-- Gitリポジトリの登録
CREATE OR REPLACE GIT REPOSITORY pension_fund_esg_handson
    API_INTEGRATION = git_api_integration
    ORIGIN = 'https://github.com/kmotokubota/pension_fund_esg_handson.git';

-- setup.sqlの実行
-- (Snowsightでsetup.sqlの内容をコピー＆実行)
```

### 2. Streamlitアプリの実行

Snowsight > Streamlit > Create Streamlit App から新規アプリを作成し、`mainpage.py` の内容をコピーして実行します。

## 関連リンク

- [Snowflake Cortex AI ドキュメント](https://docs.snowflake.com/en/guides-overview-ai-features)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Cortex Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agent)

## ライセンス

This repository is for demonstration purposes.

