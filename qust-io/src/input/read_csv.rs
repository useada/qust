#![allow(async_fn_in_trait)]
use csv::StringRecord;
use qust::prelude::{dt, DataInfo, KlineData, PriceOri, TickData, Tri};


trait ReadRecord {
    type Output;
    fn read_record(&self, record: &StringRecord) -> Self::Output;
}

pub trait ReadCsv {
    type Output;
    async fn read_csv(&self, path: &str) -> Self::Output;
}

pub struct DiReader<T> {
    pub date_time: T,
    pub open: T,
    pub high: T,
    pub low: T,
    pub close: T,
    pub volume: T,
    pub amount: T,
    pub date_time_format: Option<&'static str>,
    pub has_header: bool,
}

struct DiReaderRecord {
    date_time: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    amount: usize,
    date_time_format: &'static str,
}


impl ReadRecord for DiReaderRecord {
    type Output = KlineData;
    fn read_record(&self, record: &StringRecord) -> Self::Output {
        KlineData {
            date_time: {
                let t = record[self.date_time].trim();
                dt::parse_from_str(t, self.date_time_format)
                    .unwrap_or_else(|_| panic!("failed to parse time, provided: {}, format: {}", t, self.date_time_format))
            },
            open: record[self.open].trim().parse().unwrap(),
            high: record[self.high].trim().parse().unwrap(),
            low: record[self.low].trim().parse().unwrap(),
            close: record[self.close].trim().parse().unwrap(),
            volume: record[self.volume].trim().parse().unwrap(),
            amount: record[self.amount].trim().parse().unwrap(),
            ki: Default::default(),
        }
    }
}


impl ReadCsv for DiReader<usize> {
    type Output = PriceOri;
    async fn read_csv(&self, path: &str) -> Self::Output {
        let di_reader_record = DiReaderRecord {
            date_time: self.date_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            amount: self.amount,
            date_time_format: self.date_time_format.unwrap_or("%Y-%m-%dT%H:%M:%S%.f"),
        };
        let skip_n = if self.has_header { 1 } else { 0 };
        let mut price_ori = PriceOri::with_capacity(100_000);
        if path.contains("https") {
            let response = reqwest::get(path).await.unwrap().text().await.unwrap();
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(self.has_header)
                .from_reader(response.as_bytes());
            for record in reader.records().skip(skip_n) {
                let record = record.unwrap();
                let kline_data = di_reader_record.read_record(&record);
                price_ori.update(&kline_data);
            }
        } else {
            let mut reader = csv::Reader::from_path(path).unwrap();
            for record in reader.records().skip(skip_n) {
                let record = record.unwrap();
                let kline_data = di_reader_record.read_record(&record);
                price_ori.update(&kline_data);
            }
        }
        price_ori.shrink_to_fit();
        price_ori
    }
}

pub struct TickReader<T> {
    pub date_time: T,
    pub last_price: T,
    pub last_volume: T,
    pub last_amount: T,
    pub ask_price1: T,
    pub bid_price1: T,
    pub ask_volume1: T,
    pub bid_volume1: T,
    pub contract: T,
    pub date_time_format: Option<&'static str>,
    pub has_header: bool,
}

struct TickReaderRecord {
    date_time: usize,
    last_price: usize,
    last_volume: usize,
    last_amount: usize,
    ask_price1: usize,
    bid_price1: usize,
    ask_volume1: usize,
    bid_volume1: usize,
    contract: usize,
    date_time_format: &'static str,
}

impl ReadRecord for TickReaderRecord {
    type Output = TickData;
    fn read_record(&self, record: &StringRecord) -> Self::Output {
        TickData {
            date_time: {
                let t = record[self.date_time].trim();
                dt::parse_from_str(t, self.date_time_format)
                    .unwrap_or_else(|_| panic!("failed to parse time, provided: {}, format: {}", t, self.date_time_format))
            },
            last_price: record[self.last_price].trim().parse().unwrap(),
            last_volume: record[self.last_volume].trim().parse().unwrap(),
            last_amount: record[self.last_amount].trim().parse().unwrap(),
            ask_price1: record[self.ask_price1].trim().parse().unwrap(),
            bid_price1: record[self.bid_price1].trim().parse().unwrap(),
            ask_volume1: record[self.ask_volume1].trim().parse().unwrap(),
            bid_volume1: record[self.bid_volume1].trim().parse().unwrap(),
            contract: record[self.contract].trim().parse().unwrap(),
        }

    }
}


impl ReadCsv for TickReader<usize> {
    type Output = Vec<TickData>;
    async fn read_csv(&self, path: &str) -> Self::Output {
        let tick_reader_record = TickReaderRecord {
            date_time: self.date_time,
            last_price: self.last_price,
            last_volume: self.last_volume,
            last_amount: self.last_amount,
            ask_price1: self.ask_price1,
            bid_price1: self.bid_price1,
            ask_volume1: self.ask_volume1,
            bid_volume1: self.bid_volume1,
            contract: self.contract,
            date_time_format: self.date_time_format.unwrap_or("%Y-%m-%dT%H:%M:%S%.f"),
        };
        let mut res = Vec::with_capacity(100000);
        if path.contains("https") {
            let response = reqwest::get(path).await.unwrap().text().await.unwrap();
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(self.has_header)
                .from_reader(response.as_bytes());
            for record in reader.records() {
                let record = record.unwrap();
                let tick_data = tick_reader_record.read_record(&record);
                res.push(tick_data);
            }
        } else {
            let mut reader = csv::Reader::from_path(path).unwrap();
            for record in reader.records() {
                let record = record.unwrap();
                let tick_data = tick_reader_record.read_record(&record);
                res.push(tick_data);
            }
        }
        res.shrink_to_fit();
        res
    }
}

// const remote_kline_url: &str = "https://raw.githubusercontent.com/baiguoname/qust/refs/heads/main/examples/git_test/kline_data.csv";
// const remote_tick_url: &str = "https://raw.githubusercontent.com/baiguoname/qust/refs/heads/main/examples/git_test/tick_data.csv";

pub async fn read_kline_data(file_path: &str) -> DataInfo {
    let di_reader: DiReader<usize> = DiReader {
        date_time: 0,
        open: 1,
        high: 2,
        low: 3,
        close: 4,
        volume: 5,
        amount: 6,
        date_time_format: None,
        has_header: true,
    };
    di_reader.read_csv(file_path).await.to_di(qust::prelude::aler, qust::prelude::rl5m.tri_box())
}

pub async fn read_tick_data(file_path: &str) -> Vec<TickData> {
    let tick_reader = TickReader {
        date_time: 0,
        last_price: 1,
        last_volume: 2,
        last_amount: 3,
        ask_price1: 4,
        bid_price1: 5,
        ask_volume1: 6,
        bid_volume1: 7,
        contract: 8,
        date_time_format: None,
        has_header:true,
    };
    tick_reader
        .read_csv(file_path)
        .await
}