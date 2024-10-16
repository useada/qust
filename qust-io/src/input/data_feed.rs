#![allow(async_fn_in_trait)]

use std::fs::File;
use polars::prelude::*;
// use serde::Deserialize;
use anyhow::{Result, anyhow};
use chrono::NaiveDateTime;
use qust::prelude::{dt, DataInfo, KlineData, PriceOri, TickData, Tri};


// let persons = dataframe_to_persons(&df)?;
pub fn dataframe_to_tick_datas(df: &DataFrame) -> Result<Vec<TickData>> {
    // // 获取必要列的索引
    // let date_time_idx = df.column("date_time")?.index();
    // let last_price_idx = df.column("last_price")?.index();
    // let open_idx = df.column("open")?.index();
    // let high_idx = df.column("high")?.index();
    // let low_idx = df.column("low")?.index();
    // let close_idx = df.column("close")?.index();
    // let pre_close_idx = df.column("pre_close")?.index();
    // let open_interest_idx = df.column("open_interest")?.index();
    // let volume_idx = df.column("volume")?.index();
    // let amount_idx = df.column("amount")?.index();
    // let bid_price1_idx = df.column("bid_price1")?.index();
    // let ask_price1_idx = df.column("ask_price1")?.index();
    // let bid_volume1_idx = df.column("bid_volume1")?.index();
    // let ask_volume1_idx = df.column("ask_volume1")?.index();
    // let contract_idx = df.column("contract")?.index();
    //
    // // let date_time_format = "%Y-%m-%dT%H:%M:%S%.f";
    // let date_time_format = "%Y-%m-%d %H:%M:%S%.f";
    //
    // // 将 DataFrame 转换为 Person 结构体的向量
    // df.iter().map(|row| {
    //     Ok(TickData {
    //         // date_time: row.get(date_time_idx).or_else(|| anyhow!("Missing date_time"))?.to_string(),
    //         date_time: {
    //             let s = row.get(date_time_idx).or_else(|| anyhow!("Missing date_time"))?.to_string().trim();
    //             dt::parse_from_str(s, date_time_format)
    //             .unwrap_or_else(|_| panic!("failed to parse time, provided: {}, format: {}", s, date_time_format))
    //         },
    //         last_price: row.get(last_price_idx).or_else(|| anyhow!("Missing last_price"))?.try_extract::<i32>()?,
    //         open: row.get(open_idx).or_else(|| anyhow!("Missing open"))?.try_extract::<i32>()?,
    //         high: row.get(high_idx).or_else(|| anyhow!("Missing high"))?.try_extract::<i32>()?,
    //         low: row.get(low_idx).or_else(|| anyhow!("Missing low"))?.try_extract::<i32>()?,
    //         close: row.get(close_idx).or_else(|| anyhow!("Missing close"))?.try_extract::<i32>()?,
    //         pre_close: row.get(pre_close_idx).or_else(|| anyhow!("Missing pre_close"))?.try_extract::<i32>()?,
    //         open_interest: row.get(open_interest_idx).or_else(|| anyhow!("Missing open_interest"))?.try_extract::<i32>()?,
    //         volume: row.get(volume_idx).or_else(|| anyhow!("Missing volume"))?.try_extract::<i32>()?,
    //         amount: row.get(amount_idx).or_else(|| anyhow!("Missing amount"))?.try_extract::<i32>()?,
    //         bid_price1: row.get(bid_price1_idx).or_else(|| anyhow!("Missing bid_price1"))?.try_extract::<i32>()?,
    //         ask_price1: row.get(ask_price1_idx).or_else(|| anyhow!("Missing ask_price1"))?.try_extract::<i32>()?,
    //         bid_volume1: row.get(bid_volume1_idx).or_else(|| anyhow!("Missing bid_volume1"))?.try_extract::<i32>()?,
    //         ask_volume1: row.get(ask_volume1_idx).or_else(|| anyhow!("Missing ask_volume1"))?.try_extract::<i32>()?,
    //         contract: row.get(contract_idx).or_else(|| anyhow!("Missing contract"))?.try_extract::<i32>()?,
    //     })
    // }).collect()

    // println!("{:?}", df);

    // 获取必要的列
    let date_time_col = df.column("date_time")?;
    let last_price_col = df.column("last_price")?;
    let open_col = df.column("open")?;
    let high_col = df.column("high")?;
    let low_col = df.column("low")?;
    let close_col = df.column("close")?;
    let pre_close_col = df.column("pre_close")?;
    let open_interest_col = df.column("open_interest")?;
    let volume_col = df.column("volume")?;
    let amount_col = df.column("amount")?;
    let bid_price1_col = df.column("bid_price1")?;
    let ask_price1_col = df.column("ask_price1")?;
    let bid_volume1_col = df.column("bid_volume1")?;
    let ask_volume1_col = df.column("ask_volume1")?;
    let contract_col = df.column("contract")?;

    let date_time_format = "%Y-%m-%d %H:%M:%S%.f";

    // 将列数据转换为向量
    let date_times: Vec<NaiveDateTime> = date_time_col.str()?.into_iter().map(
        |opt_name| dt::parse_from_str(opt_name.unwrap_or("").to_string().trim(), date_time_format)
                .unwrap_or_else(|_| panic!("failed to parse time, provided: {:?}, format: {}", opt_name, date_time_format))
    ).collect();
    let last_prices: Vec<f64> = last_price_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let opens: Vec<f64> = open_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let highs: Vec<f64> = high_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let lows: Vec<f64> = low_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let closes: Vec<f64> = close_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let pre_closes: Vec<f64> = pre_close_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let open_interests: Vec<f64> = open_interest_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let volumes: Vec<f64> = volume_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let amounts: Vec<f64> = amount_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let bid_price1s: Vec<f64> = bid_price1_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let ask_price1s: Vec<f64> = ask_price1_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let bid_volume1s: Vec<f64> = bid_volume1_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let ask_volume1s: Vec<f64> = ask_volume1_col.f64()?.into_iter().map(|opt_age| opt_age.unwrap_or(0.)).collect();
    let contracts: Vec<i32> = contract_col.i32()?.into_iter().map(|opt_age| opt_age.unwrap_or(0)).collect();

    // 确保所有列的长度相同
    let len = date_times.len();
    // if ages.len() != len || cities.len() != len {
    //     return Err(anyhow!("Column lengths do not match"));
    // }

    // 创建 Person 结构体的向量
    (0..len).map(|i| {
        Ok(TickData {
            date_time: date_times[i].clone(),
            last_price: last_prices[i] as f64,
            open: opens[i] as f64,
            high: highs[i] as f64,
            low: lows[i] as f64,
            close: closes[i] as f64,
            pre_close: pre_closes[i] as f64,
            open_interest: open_interests[i] as f64,
            volume: volumes[i] as f64,
            amount: amounts[i] as f64,
            bid_price1: bid_price1s[i] as f64,
            ask_price1: ask_price1s[i] as f64,
            bid_volume1: bid_volume1s[i] as f64,
            ask_volume1: ask_volume1s[i] as f64,
            contract: contracts[i] as i32,
        })
    }).collect()
}


pub fn load_tick_datas(file_path: &str) -> Result<Vec<TickData>> {
    let file = File::open(file_path)?;
    // let df = CsvReader::from_path(file_path)?
    // let df = CsvReader::new(file)
    //     .infer_schema(Some(100))
    //     .has_header(true)
    //     .finish()
    //     .map_err(|e| anyhow!("Failed to read CSV: {}", e));
    //     // .has_header(true)
    //     // .with_delimiter(b',')  // 指定分隔符，默认为逗号
    //     // .with_ignore_parser_errors(true)  // 忽略解析错误
    //     // .with_chunk_size(10000)  // 设置每次读取的行数，用于大文件
    //     // .with_encoding(CsvEncoding::Utf8)  // 指定编码
    //     // // .with_columns(Some(vec!["column1", "column2"]))  // 只读取特定列
    //     // .finish()?;

    // 创建一个 Schema，指定列的数据类型
    let schema = Schema::from_iter(vec![
        Field::new(PlSmallStr::from("date_time"), DataType::String),
        Field::new(PlSmallStr::from("date_time_nano"), DataType::Int64),
        Field::new(PlSmallStr::from("last_price"), DataType::Float64),
        Field::new(PlSmallStr::from("open"), DataType::Float64),
        Field::new(PlSmallStr::from("high"), DataType::Float64),
        Field::new(PlSmallStr::from("low"), DataType::Float64),
        // Field::new(PlSmallStr::from("average"), DataType::Float32),
        Field::new(PlSmallStr::from("close"), DataType::Float64),
        Field::new(PlSmallStr::from("pre_close"), DataType::Float64),
        Field::new(PlSmallStr::from("volume"), DataType::Float64),
        Field::new(PlSmallStr::from("amount"), DataType::Float64),
        Field::new(PlSmallStr::from("open_interest"), DataType::Float64),
        Field::new(PlSmallStr::from("bid_price1"), DataType::Float64),
        Field::new(PlSmallStr::from("bid_volume1"), DataType::Float64),
        Field::new(PlSmallStr::from("ask_price1"), DataType::Float64),
        Field::new(PlSmallStr::from("ask_volume1"), DataType::Float64),
        Field::new(PlSmallStr::from("contract"), DataType::Int32),
    ]);

    let df = CsvReadOptions::default()
        .with_schema(Some(SchemaRef::from(schema)))
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()?;
    dataframe_to_tick_datas(&df)
}
