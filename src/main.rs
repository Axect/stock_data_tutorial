use peroxide::fuga::*;
use stock_data::*;
use chrono::NaiveDate;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let date1 = NaiveDate::from_ymd_opt(2012, 1, 1).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();

    let url = build_yahoo_finance_url_from_dates("^TNX", date1, date2, "1d", true);
    let mut bytes = vec![];
    while bytes.is_empty() {
        bytes = match download_stock_data(&url).await {
            Ok(b) => {
                println!("Downloaded {} bytes", b.len());
                b
            },
            Err(e) => {
                println!("Error: {}", e);
                vec![]
            }
        }
    }

    println!("Bytes size: {}", bytes.len());

    let path = "stock_data.csv"; 
    write_stock_data(&bytes, path).await?;

    // Wait until the file is written
    // Check file size to make sure it's done
    while std::fs::metadata(path)?.len() == 0 {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut df = DataFrame::read_csv(path, ',')?;
    // Find "null" in the data
    df["Open"].mut_map(|x: &mut String| if x == "null" { *x = "0.0".to_string(); });
    df["High"].mut_map(|x: &mut String| if x == "null" { *x = "0.0".to_string(); });
    df["Low"].mut_map(|x: &mut String| if x == "null" { *x = "0.0".to_string(); });
    df["Close"].mut_map(|x: &mut String| if x == "null" { *x = "0.0".to_string(); });
    df["Adj Close"].mut_map(|x: &mut String| if x == "null" { *x = "0.0".to_string(); });
    df["Volume"].mut_map(|x: &mut String| if x == "null" { *x = "0".to_string(); });
    df.as_types(vec![Str, F64, F64, F64, F64, F64, U64]);
    df.print();

    df.write_parquet("stock_data.parquet", CompressionOptions::Gzip(None))?;
    Ok(())
}
