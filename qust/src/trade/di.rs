use crate::{idct::prelude::*, std_prelude::*, trade::prelude::*};
use qust_ds::prelude::*;
use qust_derive::*;

/* #region Price */
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PriceTick {
    #[serde(
        serialize_with = "serialize_vec_dt",
        deserialize_with = "deserialize_vec_dt"
    )]
    pub date_time: vdt,
    pub last_price: v64,
    pub open: v64,
    pub high: v64,
    pub low: v64,
    pub close: v64,
    pub pre_close: v64,
    pub open_interest: v64,
    pub volume: v64,
    pub amount: v64,
    pub bid_price1: v64,
    pub ask_price1: v64,
    pub bid_volume1: v64,
    pub ask_volume1: v64,
    pub contract: Vec<i32>,
}

impl PriceTick {
    pub fn with_capacity(i: usize) -> Self {
        Self {
            date_time: Vec::with_capacity(i),
            last_price: Vec::with_capacity(i),
            open: Vec::with_capacity(i),
            high: Vec::with_capacity(i),
            low: Vec::with_capacity(i),
            close: Vec::with_capacity(i),
            pre_close: Vec::with_capacity(i),
            open_interest: Vec::with_capacity(i),
            volume: Vec::with_capacity(i),
            amount: Vec::with_capacity(i),
            bid_price1: Vec::with_capacity(i),
            ask_price1: Vec::with_capacity(i),
            bid_volume1: Vec::with_capacity(i),
            ask_volume1: Vec::with_capacity(i),
            contract: Vec::with_capacity(i),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.date_time.shrink_to_fit();
        self.last_price.shrink_to_fit();
        self.open.shrink_to_fit();
        self.high.shrink_to_fit();
        self.low.shrink_to_fit();
        self.close.shrink_to_fit();
        self.pre_close.shrink_to_fit();
        self.open_interest.shrink_to_fit();
        self.volume.shrink_to_fit();
        self.amount.shrink_to_fit();
        self.bid_price1.shrink_to_fit();
        self.ask_price1.shrink_to_fit();
        self.bid_volume1.shrink_to_fit();
        self.ask_volume1.shrink_to_fit();
        self.contract.shrink_to_fit();
    }

    pub fn cat(&mut self, price: &mut PriceTick) {
        self.date_time.append(&mut price.date_time);
        self.last_price.append(&mut price.last_price);
        self.open.append(&mut price.last_price);
        self.high.append(&mut price.last_price);
        self.low.append(&mut price.last_price);
        self.close.append(&mut price.last_price);
        self.pre_close.append(&mut price.last_price);
        self.open_interest.append(&mut price.last_price);
        self.volume.append(&mut price.volume);
        self.amount.append(&mut price.amount);
        self.bid_price1.append(&mut price.bid_price1);
        self.ask_price1.append(&mut price.ask_price1);
        self.bid_volume1.append(&mut price.bid_volume1);
        self.ask_volume1.append(&mut price.ask_volume1);
        self.contract.append(&mut price.contract);
    }

    pub fn to_price_ori(&self, r: TriBox, ticker: Ticker) -> PriceOri {
        if self.date_time.is_empty() {
            return PriceOri::with_capacity(0);
        }
        let mut price_ori = r.gen_price_ori(self);
        let mut f = r.update_tick_func(ticker);

        for (&date_time, &last_price, &open, &high, &low, &close,
            &pre_close, &open_interest, &volume, &amount,
            &bid_price1, &ask_price1, &bid_volume1, &ask_volume1, &contract) in izip!(
            self.date_time.iter(),
            self.last_price.iter(),
            self.open.iter(),
            self.high.iter(),
            self.low.iter(),
            self.close.iter(),
            self.pre_close.iter(),
            self.open_interest.iter(),
            self.volume.iter(),
            self.amount.iter(),
            self.bid_price1.iter(),
            self.ask_price1.iter(),
            self.bid_volume1.iter(),
            self.ask_volume1.iter(),
            self.contract.iter(),
        ) {
            let tick_data = TickData {
                date_time,
                last_price,
                open,
                high,
                low,
                close,
                pre_close,
                open_interest,
                volume,
                amount,
                bid_price1,
                ask_price1,
                bid_volume1,
                ask_volume1,
                contract,
            };
            f(&tick_data, &mut price_ori);
        }
        price_ori.shrink_to_fit();
        price_ori
    }

    pub fn to_di(&self, r: TriBox, ticker: Ticker) -> DataInfo {
        self.to_price_ori(r.clone(), ticker)
            .to_pcon(r, ticker)
            .to_di()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KlineInfo {
    #[serde(serialize_with = "serialize_dt", deserialize_with = "deserialize_dt")]
    pub open_time: dt,
    pub pass_last: u16,
    pub pass_this: u16,
    pub contract: i32,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PriceOri {
    #[serde(
        serialize_with = "serialize_vec_dt",
        deserialize_with = "deserialize_vec_dt"
    )]
    pub date_time: vdt,
    pub open: v64,
    pub high: v64,
    pub low: v64,
    pub close: v64,
    pub volume: v64,
    pub amount: v64,
    pub ki: Vec<KlineInfo>,
    pub immut_info: Vec<vv64>,
}

impl PriceOri {
    pub fn with_capacity(i: usize) -> Self {
        PriceOri {
            date_time: Vec::with_capacity(i),
            open: Vec::with_capacity(i),
            high: Vec::with_capacity(i),
            low: Vec::with_capacity(i),
            close: Vec::with_capacity(i),
            volume: Vec::with_capacity(i),
            amount: Vec::with_capacity(i),
            ki: Vec::with_capacity(i),
            immut_info: Default::default(),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.date_time.shrink_to_fit();
        self.open.shrink_to_fit();
        self.high.shrink_to_fit();
        self.low.shrink_to_fit();
        self.close.shrink_to_fit();
        self.volume.shrink_to_fit();
        self.amount.shrink_to_fit();
        self.ki.shrink_to_fit();
    }

    pub fn cat(&mut self, price: &mut PriceOri) {
        self.date_time.append(&mut price.date_time);
        self.open.append(&mut price.open);
        self.high.append(&mut price.high);
        self.low.append(&mut price.low);
        self.close.append(&mut price.close);
        self.volume.append(&mut price.volume);
        self.amount.append(&mut price.amount);
        self.ki.append(&mut price.ki);
    }

    pub fn to_pcon(self, inter: TriBox, ticker: Ticker) -> Pcon {
        Pcon {
            price: self,
            inter,
            ticker,
        }
    }
    pub fn to_di(self, ticker: Ticker, inter: TriBox) -> DataInfo {
        self.to_pcon(inter, ticker).to_di()
    }
}

#[derive(Clone)]
pub struct PriceArc {
    pub date_time: avdt,
    pub open: av64,
    pub high: av64,
    pub low: av64,
    pub close: av64,
    pub volume: av64,
    pub amount: av64,
    pub ki: Arc<Vec<KlineInfo>>,
    pub immut_info: Vec<Arc<vv64>>,
    pub finished: Option<Vec<KlineState>>,
}

impl PriceArc {
    pub fn to_price_ori(self) -> PriceOri {
        PriceOri {
            date_time: self.date_time.to_vec(),
            open: self.open.to_vec(),
            high: self.high.to_vec(),
            low: self.low.to_vec(),
            close: self.close.to_vec(),
            volume: self.volume.to_vec(),
            amount: self.amount.to_vec(),
            ki: self.ki.to_vec(),
            immut_info: self
                .immut_info
                .into_iter()
                .map(|x| x.to_vec())
                .collect_vec(),
        }
    }
}

/* #endregion */

/* #region Pcon */
#[derive(Clone, Serialize, Deserialize)]
pub struct PconType<T, N> {
    pub ticker: Ticker,
    pub inter: T,
    pub price: N,
}
pub type Pcon = PconType<TriBox, PriceOri>;

#[ta_derive]
pub struct PconIdent {
    pub inter: TriBox,
    pub ticker: Ticker,
}

impl PconIdent {
    pub fn new(inter: TriBox, ticker: Ticker) -> Self {
        Self { inter, ticker }
    }
}

impl std::fmt::Display for PconIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {:?}", self.ticker, self.inter)
    }
}
impl PartialEq for PconIdent {
    fn eq(&self, other: &Self) -> bool {
        self.ticker == other.ticker && format!("{:?}", self.inter) == format!("{:?}", other.inter)
    }
}
impl Eq for PconIdent {}
impl std::hash::Hash for PconIdent {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        format!("{:?}", self).hash(state)
    }
}

impl PartialEq for Pcon {
    fn eq(&self, other: &Self) -> bool {
        self.ident() == other.ident()
    }
}

impl Pcon {
    pub fn from_price(price: PriceOri, inter: TriBox, ticker: Ticker) -> Self {
        Pcon {
            ticker,
            inter,
            price,
        }
    }

    pub fn ident(&self) -> PconIdent {
        PconIdent::new(self.inter.clone(), self.ticker)
    }

    pub fn to_di(self) -> DataInfo {
        DataInfo {
            pcon: self,
            data_save: DataSave::default(),
            dcon: RwLock::new(vec![Tf(0, 1)]),
            part: RwLock::new(vec![Part::ono]),
        }
    }
}

/* #endregion */

/* #region Di */
#[derive(Serialize, Deserialize, AsRef)]
pub struct DataInfoType<T> {
    pub pcon: T,
    #[serde(skip)]
    pub data_save: DataSave,
    pub dcon: RwLock<Vec<Convert>>,
    pub part: RwLock<Vec<Part>>,
}
pub type DataInfo = DataInfoType<Pcon>;

impl Clone for DataInfo {
    fn clone(&self) -> Self {
        self.pcon.clone().to_di()
    }
}

impl DataInfo {
    pub fn size(&self) -> usize {
        self.pcon.price.date_time.len()
    }
    pub fn last_dcon(&self) -> Convert {
        let dcon_vec = self.dcon.read().unwrap();
        dcon_vec[dcon_vec.len() - 1].clone()
    }

    pub fn last_part(&self) -> Part {
        let part_vec = self.part.read().unwrap();
        part_vec[part_vec.len() - 1].clone()
    }

    pub fn get_kline(&self, p: &KlineType) -> av64 {
        match p {
            KlineType::Open => self.open(),
            KlineType::High => self.high(),
            KlineType::Low => self.low(),
            _ => self.close(),
        }
    }

    pub fn repeat(&self, n: usize) -> DataInfoList {
        DataInfoList {
            dil: vec![self.clone(); n],
        }
    }

    pub fn date_time(&self) -> avdt {
        self.calc(self.last_dcon()).date_time
    }
    pub fn open(&self) -> av64 {
        self.calc(self.last_dcon()).open
    }
    pub fn high(&self) -> av64 {
        self.calc(self.last_dcon()).high
    }
    pub fn low(&self) -> av64 {
        self.calc(self.last_dcon()).low
    }
    pub fn close(&self) -> av64 {
        self.calc(self.last_dcon()).close
    }
    pub fn volume(&self) -> av64 {
        self.calc(self.last_dcon()).volume
    }
    pub fn amount(&self) -> av64 {
        self.calc(self.last_dcon()).amount
    }
    pub fn immut_info(&self) -> Vec<Arc<vv64>> {
        self.calc(self.last_dcon()).immut_info
    }

    pub fn len(&self) -> usize {
        self.pcon.price.date_time.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pcon.price.date_time.is_empty()
    }

    pub fn clear(&self) {
        self.data_save.clear();
    }

    pub fn clear2(&self) {
        self.data_save.save_pms2d.write().unwrap().clear();
        self.data_save.save_dcon.write().unwrap().clear();
        self.data_save.save_others.write().unwrap().clear();
    }

    pub fn calc<T: AsRef<N>, N: Calc<R> + ?Sized, R>(&self, x: T) -> R {
        x.as_ref().calc(self)
    }

    pub fn tz_profit(&self) -> f64 {
        let tz = self.pcon.ticker.info().price_tick;
        10000. * tz / self.pcon.price.close.last().unwrap()
    }
}

impl Debug for DataInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<15} ---  {:<24} .. {:<24}  ---  {:<10} --- {}",
            self.pcon.ident().to_string(),
            self.pcon.price.ki.first().unwrap().open_time.to_string(),
            self.pcon.price.date_time.last().unwrap().to_string(),
            self.pcon.price.date_time.len().to_string(),
            (self.pcon.price.ki.map(|x| x.pass_this as f64).mean() / 120.) as usize,
        )
    }
}

/* #endregion */

/* #region Dil */
#[derive(Serialize, Deserialize, Clone)]
pub struct DataInfoList {
    pub dil: Vec<DataInfo>,
}
impl DataInfoList {
    pub fn clear(&self) {
        self.dil.iter().for_each(|x| x.clear());
    }
    pub fn clear1(&self) {
        self.dil
            .iter()
            .for_each(|x| x.data_save.save_pms2d.write().unwrap().clear());
    }
    pub fn clear2(&mut self) {
        self.dil
            .iter()
            .for_each(|x| x.data_save.save_dcon.write().unwrap().clear());
    }

    pub fn total_kline_nums(&self) -> usize {
        self.dil.iter().map(|x| x.size()).sum::<usize>()
    }
}

impl Debug for DataInfoList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dil size {}\n{}",
            self.size(),
            self.dil
                .iter()
                .map(|x| x.debug_string() + "\n")
                .collect_vec()
                .concat(),
        )
    }
}
/* #endregion */

/* #region Price -> PriceArc */
pub trait ToArc {
    type Output;
    fn to_arc(self) -> Self::Output;
}

impl<T> ToArc for Vec<T> {
    type Output = Arc<Vec<T>>;
    fn to_arc(self) -> Self::Output {
        Arc::new(self)
    }
}

impl ToArc for PriceOri {
    type Output = PriceArc;
    fn to_arc(self) -> Self::Output {
        PriceArc {
            date_time: self.date_time.to_arc(),
            open: self.open.to_arc(),
            high: self.high.to_arc(),
            low: self.low.to_arc(),
            close: self.close.to_arc(),
            volume: self.volume.to_arc(),
            amount: self.amount.to_arc(),
            ki: self.ki.to_arc(),
            immut_info: self.immut_info.map(|x| x.clone().to_arc()),
            finished: None,
        }
    }
}

impl ToArc for (PriceOri, Option<Vec<KlineState>>) {
    type Output = PriceArc;
    fn to_arc(self) -> Self::Output {
        let mut res = self.0.to_arc();
        res.finished = self.1;
        res
    }
}
/* #endregion */
