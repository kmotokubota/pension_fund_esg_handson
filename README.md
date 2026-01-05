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
├── setup.sql                           # セットアップSQL
├── app/
│   ├── mainpage.py                     # Streamlitメインページ
│   ├── environment.yml                 # Python依存パッケージ
│   └── pages/
│       ├── 1_global_analysis.py        # グローバル年金基金分析
│       ├── 2_stewardship_evaluation.py # スチュワードシップ原則評価
│       └── 3_cortex_search_rag.py      # Cortex Search RAG
├── handson/
│   └── handson.ipynb                   # ハンズオン用Notebook
├── data/                               # 初期データ
│   ├── am_esg_report/                  # 運用機関ESGレポート
│   ├── global_pf_esg_report/           # 海外年金基金レポート
│   ├── gpif_esg_report/                # GPIFレポート
│   └── stewardship_principles/         # スチュワードシップ原則
└── additional_data/                    # 追加データ
    ├── am_esg_report/                  # 追加運用機関レポート
    └── global_pf_esg_report/           # 追加海外年金基金レポート
```

## ハンズオンの流れ

### 1. Git統合によるセットアップ
Snowflakeワークシートでsetup.sqlの実行 (Snowsightでsetup.sqlの内容をコピー＆実行)

### 2. Notebookの実行
Notebook内のセルを実行し、PDF資料の構造化やCortex Search、Cortex Agentの作成を実行します。

### 3. Streamlitアプリの実行
Snowsight > Streamlit > Create Streamlit App から新規アプリを作成し、`mainpage.py` の内容をコピーして実行します。

### 4. Snowflake Intelligenceの実行

## ライセンス

This repository is for demonstration purposes.

