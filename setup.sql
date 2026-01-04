-- =========================================================
-- 年金基金 ESG/サステナビリティ分析 ハンズオン
-- セットアップSQL
-- =========================================================
-- 作成日: 2025/12/23
-- 対象: 3時間ハンズオン
-- =========================================================

-- =========================================================
-- セッション1: 環境構築
-- =========================================================

-- ---------------------------------------------------------
-- Step 1-1: 基本設定
-- ---------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- データベースとスキーマの作成
CREATE DATABASE IF NOT EXISTS DEMO_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_DB.DEMO_SUSTAINABILITY;

USE DATABASE DEMO_DB;
USE SCHEMA DEMO_SUSTAINABILITY;

-- ウェアハウスの作成（必要に応じて）
CREATE WAREHOUSE IF NOT EXISTS HANDSON_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE HANDSON_WH;

-- ---------------------------------------------------------
-- Step 1-2: クロスリージョン推論の有効化
-- ---------------------------------------------------------
-- Cortex AI機能を使用するために必要な設定
-- 日本リージョン（AP_NORTHEAST_1）では一部のLLMモデルが利用できないため、
-- クロスリージョン推論を有効にすることで、他リージョンのモデルを利用可能にします

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ---------------------------------------------------------
-- Step 1-3: ドキュメント格納用ステージの作成
-- ---------------------------------------------------------
-- PDFファイルをアップロードするための内部ステージを作成
CREATE OR REPLACE STAGE document_stage
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- ステージ確認
LIST @document_stage;

-- ---------------------------------------------------------
-- Step 1-4: Git連携 - API統合の作成
-- ---------------------------------------------------------
-- GitHubリポジトリと連携するためのAPI統合を作成
-- これにより、GitHubからPDFファイルを直接Snowflakeに取り込むことができます

CREATE OR REPLACE API INTEGRATION git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/kmotokubota/')
    ENABLED = TRUE;

-- ---------------------------------------------------------
-- Step 1-5: Git連携 - Gitリポジトリの作成
-- ---------------------------------------------------------
-- ハンズオン用のGitHubリポジトリを登録
-- リポジトリURL: https://github.com/kmotokubota/pension_fund_esg_handson

CREATE OR REPLACE GIT REPOSITORY pension_fund_esg_handson
    API_INTEGRATION = git_api_integration
    ORIGIN = 'https://github.com/kmotokubota/pension_fund_esg_handson.git';

-- Gitリポジトリの確認
SHOW GIT REPOSITORIES;

-- リポジトリの内容を確認（ブランチ一覧）
SHOW GIT BRANCHES IN GIT REPOSITORY pension_fund_esg_handson;

-- リポジトリのファイル一覧を確認
LS @pension_fund_esg_handson/branches/main/;
LS @pension_fund_esg_handson/branches/main/data/;
LS @pension_fund_esg_handson/branches/main/data/am_esg_report/;
LS @pension_fund_esg_handson/branches/main/data/global_pf_esg_report/;
LS @pension_fund_esg_handson/branches/main/data/gpif_esg_report/;
LS @pension_fund_esg_handson/branches/main/data/stewardship_principles/;

-- ---------------------------------------------------------
-- Step 1-6: GitからステージへPDFファイルをコピー
-- ---------------------------------------------------------
-- GitHubリポジトリからPDFファイルを内部ステージにコピー

-- 運用機関サステナビリティレポートをコピー
COPY FILES 
    INTO @document_stage/am_esg_report/
    FROM @pension_fund_esg_handson/branches/main/data/am_esg_report/
    PATTERN = '.*\.pdf';

-- 海外年金基金サステナビリティレポートをコピー
COPY FILES 
    INTO @document_stage/global_pf_esg_report/
    FROM @pension_fund_esg_handson/branches/main/data/global_pf_esg_report/
    PATTERN = '.*\.pdf';

-- GPIF ESGレポートをコピー（国内年金基金サステナビリティレポート）
COPY FILES 
    INTO @document_stage/gpif_esg_report/
    FROM @pension_fund_esg_handson/branches/main/data/gpif_esg_report/
    PATTERN = '.*\.pdf';

-- スチュワードシップ活動原則をコピー
COPY FILES 
    INTO @document_stage/stewardship_principles/
    FROM @pension_fund_esg_handson/branches/main/data/stewardship_principles/
    PATTERN = '.*\.pdf';

-- ディレクトリメタデータを更新
ALTER STAGE document_stage REFRESH;

-- コピー結果の確認
LIST @document_stage/am_esg_report/;
LIST @document_stage/global_pf_esg_report/;
LIST @document_stage/gpif_esg_report/;
LIST @document_stage/stewardship_principles/;

-- GitHubリポジトリのディレクトリ構造（参考）
-- pension_fund_esg_handson/
-- ├── data/
-- │   ├── am_esg_report/           -- 運用機関サステナビリティレポート
-- │   ├── global_pf_esg_report/    -- 海外年金基金サステナビリティレポート
-- │   ├── gpif_esg_report/         -- GPIF（国内年金基金）サステナビリティレポート
-- │   └── stewardship_principles/  -- スチュワードシップ活動原則
-- ├── handson/
-- │   └── handson.ipynb            -- ハンズオン用Notebook
-- └── app/
--     ├── mainpage.py              -- Streamlitメインページ
--     ├── environment.yml          -- 依存パッケージ
--     └── pages/                   -- Streamlitサブページ

-- Snowflakeステージ内のディレクトリ構造（コピー後）
-- document_stage/
-- ├── am_esg_report/           -- 運用機関サステナビリティレポート
-- │   ├── amone_sustainability_report_j2024.pdf
-- │   ├── mutb_stewardship_2025.pdf
-- │   ├── resona_am_sus_report2024-2025.pdf
-- │   └── smtam_SustainabilityReport_20242025_A3.pdf
-- ├── global_pf_esg_report/    -- 海外年金基金サステナビリティレポート
-- │   ├── 2023 Annual Report of the Thrift Savings Plan.pdf
-- │   ├── 2023 National Pension Service Sustainability Report.pdf
-- │   ├── CalPERS' Sustainable Investments 2030 Strategy.pdf
-- │   ├── CalSTRS Sustainability Report 2023-24.pdf
-- │   ├── CPP investments 2023 Report on Sustainable Investing.pdf
-- │   ├── norges bank investment management_responsible-investment-2023.pdf
-- │   └── Temasek-Sustainability-Report-2025.pdf
-- ├── gpif_esg_report/         -- GPIF（国内年金基金）サステナビリティレポート
-- │   └── gpif_Sustainability_Investment_Report_2024_E_02.pdf
-- └── stewardship_principles/  -- スチュワードシップ活動原則
--     └── gpif_20250331_stewardship_activity_principle.pdf


-- =========================================================
-- セッション2: Notebook & Streamlitの作成
-- =========================================================
-- セッション2以降の処理はNotebook上で実行します

-- ---------------------------------------------------------
-- Step 2-1: Notebook用コンピュートプールの作成
-- ---------------------------------------------------------
-- ウェアハウスより起動が早く、コスト効率が良いCPUコンテナランタイム

CREATE COMPUTE POOL IF NOT EXISTS ESG_HOL_COMPUTE_POOL
    MIN_NODES = 1
    MAX_NODES = 10
    INSTANCE_FAMILY = CPU_X64_M
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'ESG HOL - CPU Notebook Runtime';

-- コンピュートプールの確認
SHOW COMPUTE POOLS LIKE 'ESG_HOL_COMPUTE_POOL';

-- ---------------------------------------------------------
-- Step 2-2: Notebookの作成
-- ---------------------------------------------------------
-- GitHubリポジトリからNotebookを作成
-- AIサービス（AI_PARSE_DOCUMENT, Cortex Search等）を使用するため
-- コンピュートプール（CPUランタイム）で実行

CREATE OR REPLACE NOTEBOOK handson
    FROM @pension_fund_esg_handson/branches/main/handson/
    MAIN_FILE = 'handson.ipynb'
    QUERY_WAREHOUSE = HANDSON_WH
    RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME'
    COMPUTE_POOL = ESG_HOL_COMPUTE_POOL
    IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 7200;

-- Notebookの確認
SHOW NOTEBOOKS LIKE 'HANDSON';

-- ---------------------------------------------------------
-- Step 2-3: Streamlit in Snowflakeの作成
-- ---------------------------------------------------------
-- GitHubリポジトリからStreamlitアプリを作成

CREATE OR REPLACE STREAMLIT pension_fund_esg
    FROM @pension_fund_esg_handson/branches/main/app
    MAIN_FILE = 'mainpage.py'
    QUERY_WAREHOUSE = HANDSON_WH;

-- Streamlitアプリの確認
SHOW STREAMLITS LIKE 'PENSION_FUND_ESG';


-- =========================================================
-- 次のステップ
-- =========================================================
-- 1. 上記のNotebook「handson」を開いてセッション2以降を実行
--    - AI_PARSE_DOCUMENTによるPDF解析
--    - テキストのチャンク化
--    - Cortex Search Serviceの作成
--    - Cortex Agentの作成
--
-- 2. Streamlitアプリ「pension_fund_esg」を開いて動作確認
--    - グローバル年金分析
--    - スチュワードシップ原則評価
--    - Cortex Search RAG
