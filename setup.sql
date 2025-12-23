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
    WAREHOUSE_SIZE = 'MEDIUM'
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

-- 設定確認
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

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

-- API統合の確認
SHOW API INTEGRATIONS;
DESC API INTEGRATION git_api_integration;

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
LS @pension_fund_esg_handson/branches/main/am_esg_report/;
LS @pension_fund_esg_handson/branches/main/global_pf_esg_report/;

-- ---------------------------------------------------------
-- Step 1-6: GitからステージへPDFファイルをコピー
-- ---------------------------------------------------------
-- GitHubリポジトリからPDFファイルを内部ステージにコピー

-- 運用機関サステナビリティレポートをコピー
COPY FILES 
    INTO @document_stage/am_esg_report/
    FROM @pension_fund_esg_handson/branches/main/am_esg_report/
    PATTERN = '.*\.pdf';

-- 海外年金基金サステナビリティレポートをコピー
COPY FILES 
    INTO @document_stage/global_pf_esg_report/
    FROM @pension_fund_esg_handson/branches/main/global_pf_esg_report/
    PATTERN = '.*\.pdf';

-- スチュワードシップ活動原則をコピー（もしあれば）
-- COPY FILES 
--     INTO @document_stage/stewardship_principles/
--     FROM @pension_fund_esg_handson/branches/main/stewardship_principles/
--     PATTERN = '.*\.pdf';

-- ディレクトリメタデータを更新
ALTER STAGE document_stage REFRESH;

-- コピー結果の確認
LIST @document_stage/am_esg_report/;
LIST @document_stage/global_pf_esg_report/;

-- ステージ内のディレクトリ構造（参考）
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
-- └── stewardship_principles/  -- スチュワードシップ活動原則
--     └── stewardship_activity_principle.pdf


-- =========================================================
-- セッション2: データ準備（AI_PARSE_DOCUMENT & チャンク化）
-- =========================================================

-- ---------------------------------------------------------
-- Step 2-1: 運用機関サステナビリティレポートの処理
-- ---------------------------------------------------------

-- PDFからテキストを抽出（AI_PARSE_DOCUMENT使用）
CREATE OR REPLACE TABLE am_sustainability_report AS
SELECT
    relative_path,
    GET_PRESIGNED_URL('@demo_db.demo_sustainability.document_stage', relative_path) AS scoped_file_url,
    AI_PARSE_DOCUMENT(
        TO_FILE('@demo_db.demo_sustainability.document_stage', relative_path), 
        {'mode': 'LAYOUT', 'page_split': true}
    ) AS raw_text_dict
FROM DIRECTORY('@demo_db.demo_sustainability.document_stage')
WHERE relative_path LIKE 'am_esg_report/%.pdf';

-- 確認
SELECT * FROM am_sustainability_report;

-- チャンク化（SPLIT_TEXT_RECURSIVE_CHARACTER使用）
CREATE OR REPLACE TABLE am_sustainability_report_chunk AS
SELECT
    t.relative_path,
    t.scoped_file_url,
    SPLIT_PART(t.relative_path, '/', -1) AS file_name,
    p.value:index::INT AS page_index, 
    c.index::INT AS chunk_index_on_page,
    c.value::STRING AS chunk_text,
    MD5_HEX(t.relative_path || ':' || p.value:index::INT || ':' || c.index::INT)::STRING AS chunk_id
FROM 
    am_sustainability_report t,
    LATERAL FLATTEN(input => t.raw_text_dict:pages) p,
    LATERAL FLATTEN(
        INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            p.value:content::STRING,
            'markdown',
            1000,  -- 最大チャンクサイズ
            100    -- オーバーラップサイズ
        )
    ) c;

-- 確認
SELECT * FROM am_sustainability_report_chunk;
SELECT COUNT(*) AS total_chunks FROM am_sustainability_report_chunk;

-- ---------------------------------------------------------
-- Step 2-2: スチュワードシップ活動原則の処理
-- ---------------------------------------------------------

-- PDFからテキストを抽出
CREATE OR REPLACE TABLE stewardship_principles_2025 AS
SELECT
    relative_path,
    GET_PRESIGNED_URL('@demo_db.demo_sustainability.document_stage', relative_path) AS scoped_file_url,
    AI_PARSE_DOCUMENT(
        TO_FILE('@demo_db.demo_sustainability.document_stage', relative_path), 
        {'mode': 'LAYOUT', 'page_split': false}
    ) AS raw_text_dict,
    raw_text_dict:content::string AS raw_text
FROM DIRECTORY('@demo_db.demo_sustainability.document_stage')
WHERE SPLIT_PART(relative_path, '/', -1) = 'stewardship_activity_principle.pdf';

-- 確認
SELECT * FROM stewardship_principles_2025;

-- チャンク化（セクション分類付き）
CREATE OR REPLACE TABLE stewardship_principles_2025_chunk AS
SELECT
    relative_path,
    scoped_file_url,
    SPLIT_PART(relative_path, '/', -1) AS file_name,
    c.index::INT AS chunk_index,
    c.value::STRING AS chunk_text,
    MD5_HEX(relative_path || ':' || c.index)::STRING AS chunk_id,
    CASE 
        WHEN c.value::STRING LIKE '%（1）%' THEN 'コーポレート・ガバナンス体制'
        WHEN c.value::STRING LIKE '%（2）%' THEN '利益相反管理'
        WHEN c.value::STRING LIKE '%（3）%' THEN 'エンゲージメント活動'
        WHEN c.value::STRING LIKE '%（4）%' THEN 'ESG・サステナビリティ'
        WHEN c.value::STRING LIKE '%（5）%' THEN '議決権行使'
        ELSE '概要・前文'
    END AS section_category
FROM stewardship_principles_2025 t,
LATERAL FLATTEN(
    INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        t.raw_text,
        'markdown',
        1000,
        100
    )
) c;

-- 確認
SELECT * FROM stewardship_principles_2025_chunk;

-- ---------------------------------------------------------
-- Step 2-3: 国内年金基金サステナビリティレポートの処理
-- ---------------------------------------------------------

CREATE OR REPLACE TABLE domestic_pf_sustainability_report AS
SELECT
    relative_path,
    GET_PRESIGNED_URL('@demo_db.demo_sustainability.document_stage', relative_path) AS scoped_file_url,
    AI_PARSE_DOCUMENT(
        TO_FILE('@demo_db.demo_sustainability.document_stage', relative_path), 
        {'mode': 'LAYOUT', 'page_split': true}
    ) AS raw_text_dict
FROM DIRECTORY('@demo_db.demo_sustainability.document_stage')
WHERE relative_path LIKE '%Sustainability_Investment_Report%.pdf%';

-- 確認
SELECT * FROM domestic_pf_sustainability_report;

-- チャンク化
CREATE OR REPLACE TABLE domestic_pf_sustainability_report_chunk AS
SELECT
    t.relative_path,
    t.scoped_file_url,
    SPLIT_PART(t.relative_path, '/', -1) AS file_name,
    p.value:index::INT AS page_index, 
    c.index::INT AS chunk_index_on_page,
    c.value::STRING AS chunk_text,
    MD5_HEX(t.relative_path || ':' || p.value:index::INT || ':' || c.index::INT)::STRING AS chunk_id
FROM 
    domestic_pf_sustainability_report t,
    LATERAL FLATTEN(input => t.raw_text_dict:pages) p,
    LATERAL FLATTEN(
        INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            p.value:content::STRING,
            'markdown',
            1000,
            100
        )
    ) c;

-- 確認
SELECT * FROM domestic_pf_sustainability_report_chunk;

-- ---------------------------------------------------------
-- Step 2-4: 海外年金基金サステナビリティレポートの処理
-- ---------------------------------------------------------

CREATE OR REPLACE TABLE global_pf_sustainability_report AS
SELECT
    relative_path,
    GET_PRESIGNED_URL('@demo_db.demo_sustainability.document_stage', relative_path) AS scoped_file_url,
    AI_PARSE_DOCUMENT(
        TO_FILE('@demo_db.demo_sustainability.document_stage', relative_path), 
        {'mode': 'LAYOUT', 'page_split': true}
    ) AS raw_text_dict
FROM DIRECTORY('@demo_db.demo_sustainability.document_stage')
WHERE relative_path LIKE 'global_pf_esg_report/%.pdf';

-- 確認
SELECT * FROM global_pf_sustainability_report;

-- チャンク化
CREATE OR REPLACE TABLE global_pf_sustainability_report_chunk AS
SELECT
    t.relative_path,
    t.scoped_file_url,
    SPLIT_PART(t.relative_path, '/', -1) AS file_name,
    p.value:index::INT AS page_index, 
    c.index::INT AS chunk_index_on_page,
    c.value::STRING AS chunk_text,
    MD5_HEX(t.relative_path || ':' || p.value:index::INT || ':' || c.index::INT)::STRING AS chunk_id
FROM 
    global_pf_sustainability_report t,
    LATERAL FLATTEN(input => t.raw_text_dict:pages) p,
    LATERAL FLATTEN(
        INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            p.value:content::STRING,
            'markdown',
            1000,
            100
        )
    ) c;

-- 確認
SELECT * FROM global_pf_sustainability_report_chunk;

-- ---------------------------------------------------------
-- Step 2-5: 統合ビューの作成
-- ---------------------------------------------------------

-- 運用機関 + スチュワードシップ原則の統合ビュー（Cortex Search用）
CREATE OR REPLACE VIEW combined_sustainability_chunks_view AS
-- 1. スチュワードシップ原則のチャンクテーブル
SELECT 
    'STEWARDSHIP' AS source_table,
    relative_path,
    scoped_file_url,
    file_name,
    chunk_text,
    chunk_id,
    section_category,
    NULL AS page_index,
    chunk_index AS chunk_index_in_file,
    NULL AS chunk_index_on_page
FROM 
    stewardship_principles_2025_chunk
UNION ALL
-- 2. AMのチャンクテーブル
SELECT 
    'AM' AS source_table,
    relative_path,
    scoped_file_url,
    file_name,
    chunk_text,
    chunk_id,
    NULL AS section_category,
    page_index,
    NULL AS chunk_index_in_file,
    chunk_index_on_page
FROM 
    am_sustainability_report_chunk;

-- 確認
SELECT * FROM combined_sustainability_chunks_view LIMIT 10;

-- グローバル年金基金 + 国内年金基金サステナビリティレポートの統合ビュー（グローバル分析用）
CREATE OR REPLACE VIEW combined_global_sustainability_view AS
-- 1. 国内年金基金 サステナビリティレポート
SELECT 
    'Domestic_PF_Sustainability' AS source_report,
    relative_path,
    scoped_file_url,
    file_name,
    page_index,
    chunk_index_on_page,
    chunk_text,
    chunk_id
FROM 
    domestic_pf_sustainability_report_chunk
UNION ALL
-- 2. Global PF サステナビリティレポート
SELECT 
    'Global_PF_Sustainability' AS source_report,
    relative_path,
    scoped_file_url,
    file_name,
    page_index,
    chunk_index_on_page,
    chunk_text,
    chunk_id
FROM 
    global_pf_sustainability_report_chunk;

-- 確認
SELECT * FROM combined_global_sustainability_view LIMIT 10;


-- =========================================================
-- セッション3-A: Cortex Search の設定
-- =========================================================

-- ---------------------------------------------------------
-- Step 3-1: 運用機関レポート用 Cortex Search Service
-- ---------------------------------------------------------
-- スチュワードシップ原則評価用の検索サービス

CREATE OR REPLACE CORTEX SEARCH SERVICE sustainability_report
    ON chunk_text
    ATTRIBUTES relative_path, scoped_file_url, file_name, page_index, chunk_index_on_page
    WAREHOUSE = HANDSON_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        chunk_text,
        relative_path,
        scoped_file_url,
        file_name,
        page_index,
        chunk_index_on_page
    FROM combined_sustainability_chunks_view
);

-- Cortex Search Service 確認
SHOW CORTEX SEARCH SERVICES;

-- 検索テスト
-- SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
--     'DEMO_DB.DEMO_SUSTAINABILITY.SUSTAINABILITY_REPORT',
--     '{
--         "query": "スチュワードシップコードの受入れ",
--         "columns": ["chunk_text", "file_name"],
--         "limit": 3
--     }'
-- );

-- ---------------------------------------------------------
-- Step 3-2: グローバル年金基金レポート用 Cortex Search Service
-- ---------------------------------------------------------
-- グローバル年金分析用の検索サービス

CREATE OR REPLACE CORTEX SEARCH SERVICE global_pf_sustainability_report
    ON chunk_text
    ATTRIBUTES relative_path, scoped_file_url, file_name, page_index, chunk_index_on_page, source_report
    WAREHOUSE = HANDSON_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        chunk_text,
        relative_path,
        scoped_file_url,
        file_name,
        page_index,
        chunk_index_on_page,
        source_report
    FROM combined_global_sustainability_view
);

-- Cortex Search Service 確認
SHOW CORTEX SEARCH SERVICES;


-- =========================================================
-- セッション3-B: Cortex Agent の設定
-- =========================================================

-- ---------------------------------------------------------
-- Step 3-3: Cortex Agent の作成
-- ---------------------------------------------------------
-- スチュワードシップ原則評価用のAIエージェント

CREATE OR REPLACE CORTEX SEARCH SERVICE DEMO_DB.DEMO_SUSTAINABILITY.SUSTAINABILITY_REPORT
    ON chunk_text
    ATTRIBUTES relative_path, scoped_file_url, file_name, page_index, chunk_index_on_page
    WAREHOUSE = HANDSON_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        chunk_text,
        relative_path,
        scoped_file_url,
        file_name,
        page_index,
        chunk_index_on_page
    FROM combined_sustainability_chunks_view
);

-- Cortex Agent定義（SNOWFLAKE_INTELLIGENCE.AGENTSスキーマに作成）
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA AGENTS;

CREATE OR REPLACE CORTEX AGENT STEWARDSHIP_AGENT
    LLM = 'claude-sonnet-4-5'
    TOOLS = (
        CORTEX_SEARCH (
            'DEMO_DB.DEMO_SUSTAINABILITY.SUSTAINABILITY_REPORT'
        )
    )
    DESCRIPTION = 'スチュワードシップ活動原則に基づき、運用機関のサステナビリティレポートを分析・評価するエージェント'
    SYSTEM_PROMPT = '
あなたはスチュワードシップ活動原則の専門家です。
運用機関のサステナビリティレポートを分析し、5つの原則への対応状況を評価します。

【スチュワードシップ活動原則】
原則1: 運用受託機関におけるコーポレート・ガバナンス体制
原則2: 運用受託機関における利益相反管理
原則3: エンゲージメントを含むスチュワードシップ活動方針
原則4: 投資におけるESGなどのサステナビリティの考慮
原則5: 議決権行使

【回答のルール】
1. 必ず検索ツールを使用して関連情報を取得してください
2. 回答には必ず出典（ファイル名、ページ番号）を明記してください
3. 原則の趣旨に沿った取り組みが確認できれば「対応している」と評価してください
4. 情報が見つからない場合のみ「情報なし」と回答してください
5. 日本語で回答してください
';

-- Agent確認
SHOW CORTEX AGENTS;

-- 元のスキーマに戻る
USE DATABASE DEMO_DB;
USE SCHEMA DEMO_SUSTAINABILITY;


-- =========================================================
-- 動作確認
-- =========================================================

-- データ件数の確認
SELECT 'am_sustainability_report_chunk' AS table_name, COUNT(*) AS row_count FROM am_sustainability_report_chunk
UNION ALL
SELECT 'stewardship_principles_2025_chunk', COUNT(*) FROM stewardship_principles_2025_chunk
UNION ALL
SELECT 'domestic_pf_sustainability_report_chunk', COUNT(*) FROM domestic_pf_sustainability_report_chunk
UNION ALL
SELECT 'global_pf_sustainability_report_chunk', COUNT(*) FROM global_pf_sustainability_report_chunk;

-- ビューの確認
SELECT source_table, COUNT(*) AS chunk_count 
FROM combined_sustainability_chunks_view 
GROUP BY source_table;

SELECT source_report, COUNT(*) AS chunk_count 
FROM combined_global_sustainability_view 
GROUP BY source_report;

-- AI_COMPLETE動作確認
SELECT AI_COMPLETE(
    'claude-sonnet-4-5',
    'こんにちは。Snowflake Cortex AIのテストです。'
) AS test_response;


-- =========================================================
-- 【参考】アプリからのレポート追加時の処理
-- =========================================================
-- Streamlitアプリ内でPDFをアップロードした際に実行される処理

-- 1. ステージにファイルをアップロード（アプリ側で実行）
-- PUT file:///path/to/file.pdf @document_stage/global_pf_esg_report/;

-- 2. テキスト抽出（アプリ側で実行）
-- INSERT INTO global_pf_sustainability_report 
-- (relative_path, scoped_file_url, raw_text_dict)
-- SELECT
--     'global_pf_esg_report/new_report.pdf' AS relative_path,
--     GET_PRESIGNED_URL('@document_stage', 'global_pf_esg_report/new_report.pdf') AS scoped_file_url,
--     AI_PARSE_DOCUMENT(
--         TO_FILE('@document_stage', 'global_pf_esg_report/new_report.pdf'),
--         {'mode': 'LAYOUT', 'page_split': true}
--     ) AS raw_text_dict;

-- 3. チャンク化（アプリ側で実行）
-- INSERT INTO global_pf_sustainability_report_chunk ...

-- 4. Cortex Search Serviceは自動的に更新されます（TARGET_LAG設定による）


-- =========================================================
-- クリーンアップ（必要に応じて実行）
-- =========================================================
-- DROP TABLE IF EXISTS am_sustainability_report;
-- DROP TABLE IF EXISTS am_sustainability_report_chunk;
-- DROP TABLE IF EXISTS stewardship_principles_2025;
-- DROP TABLE IF EXISTS stewardship_principles_2025_chunk;
-- DROP TABLE IF EXISTS domestic_pf_sustainability_report;
-- DROP TABLE IF EXISTS domestic_pf_sustainability_report_chunk;
-- DROP TABLE IF EXISTS global_pf_sustainability_report;
-- DROP TABLE IF EXISTS global_pf_sustainability_report_chunk;
-- DROP VIEW IF EXISTS combined_sustainability_chunks_view;
-- DROP VIEW IF EXISTS combined_global_sustainability_view;
-- DROP CORTEX SEARCH SERVICE IF EXISTS sustainability_report;
-- DROP CORTEX SEARCH SERVICE IF EXISTS global_pf_sustainability_report;
-- USE DATABASE SNOWFLAKE_INTELLIGENCE;
-- USE SCHEMA AGENTS;
-- DROP CORTEX AGENT IF EXISTS STEWARDSHIP_AGENT;

