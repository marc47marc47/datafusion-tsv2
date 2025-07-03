use datafusion::error::Result;
use datafusion06::{
    parse_input_file, validate_extension, create_session_ctx,
    csv_options, register_csv, execute_query,
};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 解析輸入
    let input_file = parse_input_file();
    println!("Load File: {:?}", input_file);

    // 2. 驗證副檔名
    if let Err(e) = validate_extension(&input_file) {
        eprintln!("Error: {}", e);
        return Ok(());
    }

    // 3. 建立上下文
    let ctx = create_session_ctx();

    // 4. 取得 CSV 讀取選項
    let options = csv_options();

    // 5. 註冊表格
    register_csv(&ctx, "sql_logs", &input_file, options).await?;

    // 6. 執行查詢
    let df = execute_query(&ctx).await?;

    // 顯示結果
    df.show().await?;
    Ok(())
}

