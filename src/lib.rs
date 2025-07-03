// src/lib.rs

use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};
use std::env;
use std::path::Path;

/// 1. 從命令列參數讀取檔案名稱
pub fn parse_input_file() -> String {
    let args: Vec<String> = env::args().collect();
    args.get(1)
        .cloned()
        .unwrap_or_else(|| "data/sql_logs.tsv".to_string())
}

/// 2. 判斷檔案是否為 .tsv
pub fn validate_extension(path: &str) -> Result<()> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    if ext != "tsv" {
        return Err(DataFusionError::Plan(
            "The input file must have a .tsv extension.".to_string(),
        ));
    }
    Ok(())
}

/// 3. 建立 DataFusion 的執行環境
pub fn create_session_ctx() -> SessionContext {
    SessionContext::new()
}

/// 4. 定義 CSV 的讀取選項 (tab 分隔，含標題)
pub fn csv_options() -> CsvReadOptions<'static> {
    CsvReadOptions::new()
        .has_header(true)
        .delimiter(b'\t')
        .file_extension("tsv")
}

/// 5. 註冊 CSV 文件
pub async fn register_csv(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    options: CsvReadOptions<'static>,
) -> Result<()> {
    ctx.register_csv(table_name, path, options).await?;
    Ok(())
}

/// 6. 使用 SQL 方法執行查詢並回傳 DataFrame
pub async fn execute_query(ctx: &SessionContext) -> Result<DataFrame> {
    let sql = r#"
        SELECT
            sql_type,
            date_trunc('day', exec_time) AS exec_day,
            COUNT(*) AS request_count
        FROM sql_logs
        GROUP BY sql_type, date_trunc('day', exec_time)
        ORDER BY exec_day, sql_type
    "#;
    let df = ctx.sql(sql).await?;
    Ok(df)
}

