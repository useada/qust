use std::sync::Arc;

use super::pms::GetPmsFromTa;
use super::prelude::Convert;
use crate::idct::fore::ForeTaCalc;
use crate::idct::part::Part::*;
use crate::prelude::{find_day_index_night_flat, KlineState, PriBox};
use crate::trade::di::DataInfo;
use crate::trade::ticker::Commission;
use qust_ds::roll::RollFunc;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};

#[ta_derive]
pub enum KlineType {
    Time,
    Open,
    High,
    Low,
    Close,
    Volume,
}

#[clone_trait]
pub trait Ta {
    fn start(&self, _di: &DataInfo) {}
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64;
    fn end(&self, _di: &DataInfo) {}
}

#[derive(Clone, Serialize, Deserialize, AsRef)]
pub struct ForeTa(pub Box<dyn Ta>, pub Box<dyn ForeTaCalc>);

impl std::fmt::Debug for ForeTa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} + {:?}", self.0, self.1)
    }
}

#[typetag::serde]
impl Ta for ForeTa {
    fn start(&self, di: &DataInfo) {
        di.part.write().unwrap().push(ono);
    }
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        let part = di
            .part
            .read()
            .unwrap()
            .iter()
            .rev()
            .skip(1)
            .take(1)
            .next()
            .unwrap()
            .clone();
        let pms = (part, self.0.clone()).get_pms_from_ta(di);
        di.calc(&pms)
    }
    fn calc_da(&self, da: Vec<&[f64]>, di: &DataInfo) -> vv64 {
        self.1.fore_ta_calc(da, di)
    }
    fn end(&self, di: &DataInfo) {
        di.part.write().unwrap().pop();
    }
}

#[ta_derive]
pub struct CommSlip(pub f64, pub f64);

impl DataInfo {
    pub fn profit2(&self) -> Vec<f64> {
        let c = self.close();
        let c_lag = c.lag(1f64);
        let mut res = izip!(c.iter(), c_lag.iter())
            .map(|(x, y)| x / y - 1.)
            .collect_vec();
        res[0] = 0f64;
        res
    }

    pub fn profit(&self) -> Vec<f64> {
        let c = self.close();
        let c_lag = c.lag(1f64);
        let mut res = izip!(c.iter(), c_lag.iter(), self.pcon.price.ki.rolling(2))
            .map(|(x, y, z)| {
                if z.first().unwrap().contract == z.last().unwrap().contract {
                    x / y - 1.
                } else {
                    0.
                }
            })
            .collect_vec();
        res[0] = 0f64;
        res
    }
}

#[typetag::serde]
impl Ta for CommSlip {
    fn calc_da(&self, data: Vec<&[f64]>, di: &DataInfo) -> vv64 {
        let c = *di.close().last().unwrap();
        let ticker_info = di.pcon.ticker.info();
        let tz = ticker_info.price_tick;
        let pv = ticker_info.volume_multiple;
        let comm_percent = match ticker_info.commission {
            Commission::Fixed(i) => self.0 * i / (c * pv),
            Commission::Proportional(i) => self.0 * i,
        };
        let slip_percent = self.1 * ticker_info.slippage * tz / c;
        let s = data[0].len();
        vec![
            vec![comm_percent; s],
            vec![comm_percent; s],
            vec![slip_percent; s],
            vec![slip_percent; s],
        ]
    }
}

#[typetag::serde]
impl Ta for KlineType {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        use KlineType::*;
        match self {
            Open => di.open(),
            High => di.high(),
            Low => di.low(),
            Close => di.close(),
            _ => todo!(),
        }
        .pip(|x| vec![x])
    }

    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        vec![da[0].to_vec()]
    }
}

/* #region Rsi */
#[ta_derive]
// #[derive(AsRef)]
pub struct Rsi(pub usize);

#[typetag::serde]
impl Ta for Rsi {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let da = da[0];
        let ret = izip!(da.iter(), da.lag((1usize, f64::NAN)).iter())
            .map(|(a, b)| a - b)
            .collect_vec();
        let ret_l = ret
            .iter()
            .map(|x| if x > &0f64 { *x } else { 0f64 })
            .collect_vec();
        let ret_s = ret
            .iter()
            .map(|x| if x < &0f64 { -x } else { 0f64 })
            .collect_vec();
        let ret_l = ret_l.ema(self.0);
        let ret_s = ret_s.ema(self.0);
        let res = izip!(ret_l.iter(), ret_s.iter())
            .map(|(x, y)| (100f64 * x) / (x + y))
            .collect_vec();
        vec![res]
        // vec![ret_s, ret_l, res]
    }
}
/* #endregion */

/* #region Tr */
#[ta_derive]
pub struct Tr;

#[typetag::serde]
impl Ta for Tr {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.high(), di.low(), di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let h = da[0];
        let l = da[1];
        let c = da[2];
        let h_c = h.iter().zip(c.iter()).map(|(&a, &b)| (a - b).abs());
        let l_1 = c.lag(1f64);
        let c_l = l.iter().zip(l_1.iter()).map(|(&a, &b)| (a - b).abs());
        let h_l = h.iter().zip(l.iter()).map(|(&a, &b)| (a - b).abs());
        let res = izip!(h_c, c_l, h_l)
            .map(|(a, b, c)| a.max(b).max(c))
            .collect();
        vec![res]
    }
}
/* #endregion */

/* #region Atr */
#[ta_derive]
pub struct Atr(pub usize);

#[typetag::serde]
impl Ta for Atr {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.calc(&Tr)[0].clone()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let res = da[0].ema(self.0);
        vec![res]
    }
}

#[ta_derive]
pub struct RollTa<T>(pub T, pub RollFunc, pub RollOps);

#[typetag::serde(name = "rollta_klinetype")]
impl Ta for RollTa<KlineType> {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        da.roll(self.1, self.2.clone())
    }
}

impl AsRef<Box<dyn Ta>> for Box<dyn Ta> {
    fn as_ref(&self) -> &Box<dyn Ta> {
        self
    }
}

#[typetag::serde(name = "rollta_boxta")]
impl Ta for RollTa<Box<dyn Ta>> {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        di.calc::<&Box<dyn Ta>, Box<dyn Ta>, avv64>(&self.0)
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        da.roll(self.1, self.2.clone())
    }
}

#[ta_derive]
pub struct Max(pub KlineType, pub usize);

#[typetag::serde]
impl Ta for Max {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.get_kline(&self.0)]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        da.roll_max(self.1)
    }
}

#[ta_derive]
pub struct Min(pub KlineType, pub usize);

#[typetag::serde]
impl Ta for Min {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.get_kline(&self.0)]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        da.roll_min(self.1)
    }
}

/* #endregion */

/* #region KDayRatio */
#[ta_derive]
pub struct KDayRatio(pub usize);

#[typetag::serde]
impl Ta for KDayRatio {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.open(), di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let gap = izip!(da[0].iter(), da[1].iter())
            .map(|(x, y)| (x - y).abs())
            .collect::<v64>();
        let k_range_ma = gap.roll_mean(self.0);
        let res = izip!(gap.iter(), k_range_ma.iter())
            .map(|(x, y)| 100. * x / y)
            .collect::<v64>();
        vec![res]
    }
}
/* #endregion */

#[ta_derive]
pub enum IndexSpec {
    First,
    Last,
    Max,
    Min,
}

impl IndexSpec {
    fn get(&self, data: &[f64]) -> f64 {
        match self {
            IndexSpec::First => data[0],
            IndexSpec::Last => *data.last().unwrap(),
            IndexSpec::Max => data.max(),
            IndexSpec::Min => data.min(),
        }
    }
}

///0: shift len 2: which data 3: how
#[ta_derive]
pub struct ShiftDays(pub usize, pub KlineType, pub IndexSpec);

#[typetag::serde]
impl Ta for ShiftDays {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.get_kline(&self.1)]
    }

    fn calc_da(&self, da: Vec<&[f64]>, di: &DataInfo) -> vv64 {
        // let da_vec = di.t().iter().map(|x| x.date()).collect();
        let da_vec = find_day_index_night_flat(di.date_time());
        let grp = Grp(da_vec);
        let (vec_index, vec_value) = grp.apply(da[0], |x| self.2.get(x));
        let vec_value = vec_value.lag(self.0 as f64);
        let ri = Reindex::new(&vec_index[..], &grp.0[..]);
        let res = ri.reindex(&vec_value[..]).fillna(f64::NAN);
        vec![res]
    }
}

#[ta_derive]
pub struct ShiftInter {
    pub inter: PriBox,
    pub n: usize,
    pub kline: KlineType,
    pub index_spec: IndexSpec,
}

#[typetag::serde]
impl Ta for ShiftInter {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        let price_arc = di.calc(Convert::Event(self.inter.clone()));
        let finished_vec = &price_arc.finished.unwrap();
        let mut da_vec = Vec::with_capacity(finished_vec.len());
        let mut index = 0.;
        finished_vec
            .iter()
            .for_each(|x| {
                da_vec.push(index);
                if let KlineState::Finished = x {
                    index += 1.;
                }
            });
         vec![di.get_kline(&self.kline), Arc::new(da_vec)]
    }

    fn calc_da(&self, da:Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let da_vec = da[1].to_vec();
        let grp = Grp(da_vec);
        let(vec_index, vec_value) = grp.apply(da[0], |x| self.index_spec.get(x));
        let vec_value = vec_value.lag(self.n as f64);
        let ri = Reindex::new(&vec_index[..], &grp.0[..]);
        let res = ri.reindex(&vec_value[..]).fillna(f64::NAN);
        vec![res]
    }
}


#[ta_derive]
pub struct DayKlineWrapper(pub KlineType);

#[typetag::serde(name = "DayKlineWrapper")]
impl Ta for DayKlineWrapper {
    fn calc_da(&self, _da: Vec<&[f64]>, di: &DataInfo) -> vv64 {
        let da_vec = find_day_index_night_flat(di.date_time());
        let grp = Grp(da_vec);
        let (_, vec_value) = match self.0 {
            KlineType::Open => grp.apply(&di.open(), |x| vec![x[0]; x.len()]),
            KlineType::High => grp.apply(&di.high(), |x| x.cum_max()),
            KlineType::Low => grp.apply(&di.low(), |x| x.cum_min()),
            KlineType::Close => grp.apply(&di.close(), |x| vec![*x.last().unwrap(); x.len()]),
            _ => panic!("this type of Kline does not implement DayKlineWrapper"),
        };
        vec![vec_value.concat()]
    }
}

/* #region Mace */
#[ta_derive]
pub struct Diff(pub usize, pub usize);

#[typetag::serde]
impl Ta for Diff {
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let res = izip!(da[0].ema(self.0), da[0].ema(self.1))
            .map(|(x, y)| x - y)
            .collect_vec();
        vec![res]
    }
}

#[ta_derive]
pub struct Macd(pub usize, pub usize, pub usize);

#[typetag::serde]
impl Ta for Macd {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.calc(Diff(self.0, self.1))[0].clone()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let res = izip!(da[0].iter(), da[0].ema(self.2).iter())
            .map(|(x, y)| x - y)
            .collect();
        vec![res]
    }
}

/* #endregion */

/* #region K D J */

#[ta_derive]
pub struct Kta(pub usize, pub usize, pub usize);
#[ta_derive]
pub struct Dta(pub usize, pub usize, pub usize);
#[ta_derive]
pub struct Jta(pub usize, pub usize, pub usize);

#[typetag::serde]
impl Ta for Kta {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        // vec![di.h(), di.l(), di.c()]
        vec![di.close(), di.close(), di.close()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let rsvnum =
            izip!(da[2].iter(), da[1].roll(RollFunc::Min, RollOps::N(self.0))).map(|(x, y)| x - y);
        let rsvdom = izip!(
            da[0].roll(RollFunc::Max, RollOps::N(self.0)),
            rsvnum.clone()
        )
        .map(|(x, y)| x - y);
        let rsv = izip!(rsvnum, rsvdom).map(|(x, y)| 100. * x / y);
        vec![rsv.collect_vec().ema(self.1)]
    }
}

#[typetag::serde]
impl Ta for Dta {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![di.calc(Kta(self.0, self.1, self.2))[0].clone()]
    }
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        vec![da[0].ema(self.2)]
    }
}

#[typetag::serde]
impl Ta for Jta {
    fn calc_di(&self, di: &DataInfo) -> avv64 {
        vec![
            di.calc(Kta(self.0, self.1, self.2))[0].clone(),
            di.calc(Dta(self.0, self.1, self.2))[0].clone(),
        ]
    }

    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let res = izip!(da[1].iter(), da[0].iter())
            .map(|(x, y)| 3. * x - 2. * y)
            .collect_vec();
        vec![res]
    }
}
/* #endregion */

/* #region EffRatio */
#[ta_derive]
pub struct EffRatio(pub usize, pub usize);

#[typetag::serde]
impl Ta for EffRatio {
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let lag_da = da[0].lag(self.0 as f64);
        let diff_data = izip!(da[0].iter(), lag_da.iter())
            .map(|(x, y)| x - y)
            .collect_vec();
        let vol = diff_data
            .iter()
            .map(|x| x.abs())
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(self.1));
        let res = izip!(diff_data.iter(), vol.iter())
            .map(|(x, &y)| if y == 0. { 0. } else { 100. * x / y })
            .collect_vec();
        vec![res]
    }
}
/* #endregion */

/* #region Spread */
#[ta_derive]
pub struct Spread(pub usize);

#[typetag::serde]
impl Ta for Spread {
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let res = izip!(
            da[0].iter(),
            da[0].roll(RollFunc::Mean, RollOps::N(self.0)).iter()
        )
        .map(|(x, y)| (x / y) - 1.)
        .collect_vec();
        vec![res]
    }
}
/* #endregion */

/* #region Rankma */
#[ta_derive]
pub struct Rankma(pub usize, pub usize);

#[typetag::serde]
impl Ta for Rankma {
    fn calc_da(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let ma = da[0].roll(RollFunc::Mean, RollOps::N(self.0));
        let madiff = ma
            .rolling(2)
            .map(|x| if x.len() == 1 { 0f64 } else { x[1] - x[0] })
            .collect_vec();
        let mafac = madiff
            .iter()
            .map(|x| if x > &0. { 1f64 } else { 0f64 })
            .collect_vec();
        vec![mafac.roll(RollFunc::Sum, RollOps::N(self.1))]
    }
}
/* #endregion */
