use crate::std_prelude::*;
use crate::trade::di::{DataInfo, PriceArc, PriceOri, ToArc};
use crate::trade::inter::{KlineData, KlineState, Pri};
use qust_derive::AsRef;
use qust_ds::prelude::*;

// #[ta_derive]
#[derive(Clone, Serialize, Deserialize, AsRef)]
// #[serde(from = "Ttt")]
pub enum Convert {
    Tf(usize, usize),
    Ha(usize),
    Event(Box<dyn Pri>),
    PreNow(Box<Convert>, Box<Convert>),
    VolFilter(usize, usize),
    Log,
    FlatTick,
}
impl PartialEq for Convert {
    fn eq(&self, other: &Self) -> bool {
        self.debug_string() == other.debug_string()
    }
}
impl Debug for Convert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Convert::*;
        let show_str = match self {
            Tf(_, _) => "ori".into(),
            Ha(u) => format!("Ha({})", u),
            PreNow(pre, now) => format!("{:?} : {:?}", pre, now),
            Event(t) => format!("{:?}", t),
            VolFilter(a, b) => format!("VolFilter({}, {})", a, b),
            Log => "Log".into(),
            FlatTick => "FlatTick".into(),
        };
        f.write_str(&show_str)
    }
}

use Convert::*;

impl Convert {
    pub fn get_pre(&self, di: &DataInfo) -> PriceArc {
        match self {
            PreNow(pre, _now) => di.calc(&**pre),
            _ => {
                let mut price_res = di.pcon.price.clone().to_arc();
                price_res.finished = None;
                price_res
            }
        }
    }

    pub fn convert(&self, price: PriceArc, di: &DataInfo) -> PriceArc {
        match self {
            Tf(_start, _end) => price,
            Ha(w) => {
                let close_price = izip!(
                    price.open.iter(),
                    price.high.iter(),
                    price.low.iter(),
                    price.close.iter(),
                )
                .map(|(o, h, l, c)| (o + h + l + c) / 4.0);
                let open_price = price.close.ema(*w);
                let mut open_price = open_price.lag(1);
                open_price[0] = open_price[1];
                let high_price: Vec<f64> =
                    izip!(price.high.iter(), open_price.iter(), close_price.clone())
                        .map(|(a, b, c)| a.max(*b).max(c))
                        .collect();
                let low_price: Vec<f64> =
                    izip!(price.low.iter(), open_price.iter(), close_price.clone())
                        .map(|(a, b, c)| a.min(*b).min(c))
                        .collect();
                PriceArc {
                    date_time: price.date_time.clone(),
                    open: Arc::new(open_price),
                    high: Arc::new(high_price),
                    low: Arc::new(low_price),
                    close: Arc::new(close_price.collect()),
                    volume: price.volume.clone(),
                    amount: price.amount.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
            PreNow(_pre, now) => now.convert(price, di),
            Event(tri) => {
                let mut price_res = PriceOri::with_capacity(price.open.len());
                let mut finished_vec = Vec::with_capacity(price_res.open.capacity());
                let mut f = tri.update_kline_func(di, &price);
                for (i, (&date_time, &open, &high, &low, &close, &volume, &amount, ki)) in izip!(
                    price.date_time.iter(),
                    price.open.iter(),
                    price.high.iter(),
                    price.low.iter(),
                    price.close.iter(),
                    price.volume.iter(),
                    price.amount.iter(),
                    price.ki.iter(),
                )
                .enumerate()
                {
                    let kline_data = KlineData { date_time, open, high, low, close, volume, amount, ki: ki.clone() };
                    let finished = f(&kline_data, &mut price_res, i);
                    finished_vec.push(finished);
                }
                (price_res, Some(finished_vec)).to_arc()
            }
            VolFilter(window, percent) => {
                let (finished_vec, mask_len) = price.volume.rolling(*window).fold(
                    (Vec::with_capacity(price.volume.len()), 0usize),
                    |mut accu, x| {
                        let m = x.last().unwrap() >= &x.quantile(*percent as f64 / 100f64);
                        if m {
                            accu.1 += 1;
                        }
                        let state: KlineState = m.into();
                        accu.0.push(state);
                        accu
                    },
                );
                let mut price_res = PriceOri::with_capacity(mask_len);
                izip!(
                    finished_vec.iter(),
                    price.date_time.iter(),
                    price.open.iter(),
                    price.high.iter(),
                    price.low.iter(),
                    price.close.iter(),
                    price.volume.iter(),
                    price.amount.iter(),
                )
                .for_each(|(i, date_time, open, high, low, close, volume, amount)| {
                    if let KlineState::Finished = i {
                        price_res.date_time.push(*date_time);
                        price_res.open.push(*open);
                        price_res.high.push(*high);
                        price_res.low.push(*low);
                        price_res.close.push(*close);
                        price_res.volume.push(*volume);
                        price_res.amount.push(*amount);
                    }
                });
                (price_res, Some(finished_vec)).to_arc()
            }
            Log => {
                let numerator = price.low.min();
                PriceArc {
                    date_time: price.date_time.clone(),
                    open: price.open.map(|x| x / numerator).to_arc(),
                    high: price.high.map(|x| x / numerator).to_arc(),
                    low: price.low.map(|x| x / numerator).to_arc(),
                    close: price.close.map(|x| x / numerator).to_arc(),
                    volume: price.volume.clone(),
                    amount: price.amount.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
            FlatTick => {
                let mut res = Vec::with_capacity(price.low.len());
                let c_vec_ori = &price.close;
                res.push(c_vec_ori[0]);
                for i in 2..c_vec_ori.len() {
                    if c_vec_ori[i - 2] == c_vec_ori[i] && c_vec_ori[i - 1] != c_vec_ori[i] {
                        res.push(c_vec_ori[i - 2]);
                    } else {
                        res.push(c_vec_ori[i - 1]);
                    }
                }
                res.push(*c_vec_ori.last().unwrap());
                let res = Arc::new(res);
                PriceArc {
                    date_time: price.date_time.clone(),
                    open: res.clone(),
                    high: res.clone(),
                    low: res.clone(),
                    close: res,
                    volume: price.volume.clone(),
                    amount: price.amount.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
        }
    }
}

pub trait VertBack {
    fn vert_back(&self, di: &DataInfo, res: Vec<&[f64]>) -> Option<vv64>;
}

impl VertBack for Convert {
    fn vert_back(&self, di: &DataInfo, res: Vec<&[f64]>) -> Option<vv64> {
        match self {
            PreNow(pre, now) => {
                let price_now = di.calc(self);
                let res_pre = match &price_now.finished {
                    None => res.map(|x| x.to_vec()),
                    Some(finished_vec) => res
                        .iter()
                        .map(|x| {
                            let mut res_pre = vec![f64::NAN; finished_vec.len()];
                            let mut v_iter = x.iter();
                            finished_vec.iter().enumerate().for_each(|(i, x)| {
                                if x.into() {
                                    res_pre[i] = *v_iter.next().unwrap();
                                }
                            });
                            res_pre
                        })
                        .collect_vec(),
                };
                match (*pre.clone(), *now.clone()) {
                    (Tf(_, _), _) => Some(res_pre),
                    (pre_n, _) => pre_n.vert_back(di, res_pre.iter().map(|x| &x[..]).collect_vec()),
                }
            }
            _ => {
                let price_now = di.calc(self);
                match &price_now.finished {
                    None => res.map(|x| x.to_vec()).into(),
                    Some(finished_vec) => res
                        .iter()
                        .map(|x| {
                            let mut res_pre = vec![f64::NAN; finished_vec.len()];
                            let mut v_iter = x.iter();
                            finished_vec.iter().enumerate().for_each(|(i, x)| {
                                if x.into() {
                                    res_pre[i] = *v_iter.next().unwrap();
                                }
                            });
                            res_pre
                        })
                        .collect_vec()
                        .into(),
                }
            }
        }
    }
}

// impl VertBack for (Convert, Convert) {
//     fn vert_back(&self, di: &Di, res: Vec<&[f64]>) -> Option<vv64> {
//         match self {
//             (Convert::PreNow(pre, _), Tf(_, _)) => {
//                 let data_pre = self.0.vert_back(di, res)?;
//                 (*pre.clone(), self.1.clone()).vert_back(di, data_pre.iter().map(|x| &x[..]).collect_vec())
//             }
//             (pre, Tf(_, _)) => {
//                 (self.1.clone() + pre.clone()).vert_back(di, res)
//             }
//         }
//     }
// }
