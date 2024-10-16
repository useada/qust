use crate::idct::dcon::{Convert, VertBack};
use crate::prelude::rank_day;
use crate::trade::di::DataInfo;
use qust_derive::ta_derive;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};

#[typetag::serde(tag = "ForeTaCalc", content = "value")]
pub trait ForeTaCalc: DynClone + Send + Sync + std::fmt::Debug + 'static {
    fn fore_ta_calc(&self, da: Vec<&[f64]>, di: &DataInfo) -> vv64;
}
clone_trait_object!(ForeTaCalc);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Serialize, Deserialize)]
pub struct Rank(pub usize, pub usize);
impl Rank {
    pub fn rank1d_(data: &[f64], i: usize) -> v64 {
        let mut res = vec![f64::NAN; data.len() - i];
        // println!("{:?}, {:?}, {:?}", data.len(), i, res.len());
        // if res.len() <= 1 {
        //     return res;
        // }
        let mut sorted_res = data[0..i].to_vec();
        sorted_res.sort_by(|&a, &b| match (a.is_nan(), b.is_nan()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => a.partial_cmp(&b).unwrap(),
        });
        for (i, &n) in data.iter().skip(i).enumerate() {
            let iloc = sorted_res.partition_point(|&x| x < n);
            sorted_res.insert(iloc, n);
            res[i] = 100f64 * (iloc as f64) / sorted_res.len() as f64;
        }
        res
    }
    pub fn rank1d(&self, data: &[f64]) -> v64 {
        let roll_step = RollStep(self.0, self.1);
        roll_step.roll(data, Rank::rank1d_)
    }
    pub fn rank(&self, data: Vec<&[f64]>) -> Vec<v64> {
        data.iter().map(|x| self.rank1d(x)).collect()
    }
}

#[typetag::serde]
impl ForeTaCalc for Rank {
    fn fore_ta_calc(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        self.rank(da)
    }
}

#[typetag::serde]
impl ForeTaCalc for Convert {
    fn fore_ta_calc(&self, da: Vec<&[f64]>, di: &DataInfo) -> vv64 {
        // let res = (di.last_dcon(), self.clone()).vert_back(di, da);
        let res = di.last_dcon().vert_back(di, da);
        // res.unwrap().ffill()
        res.unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillCon(pub Convert);

#[typetag::serde]
impl ForeTaCalc for FillCon {
    fn fore_ta_calc(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        let mut res = self.0.fore_ta_calc(da, _di);
        res.iter_mut().for_each(|x| x.ffill());
        res
    }
}

#[ta_derive]
pub struct WithRank;

#[typetag::serde]
impl ForeTaCalc for WithRank {
    fn fore_ta_calc(&self, da: Vec<&[f64]>, _di: &DataInfo) -> vv64 {
        da.iter().fold(vec![], |mut accu, x| {
            accu.push(x.to_vec());
            accu.push(rank_day.rank1d(x));
            accu
        })
    }
}
