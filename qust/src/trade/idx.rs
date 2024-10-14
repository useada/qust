use super::inter::TriBox;
use crate::prelude::{
    find_day_index_night_pre, PconIdent, PriceArc, Stra, StraKind, Stral, Ticker,
};
use crate::prelude::{DataInfo, DataInfoList, InfoPnlRes, PnlRes, PriceOri, PriceTick};
use qust_ds::prelude::*;
use std::borrow::Cow;
use std::ops::Range;

#[derive(Clone)]
enum Idx {
    Range(Range<usize>),
    List(Vec<usize>),
}

impl Idx {
    fn into_vec(self) -> Vec<usize> {
        match self {
            Idx::Range(r) => r.collect_vec(),
            Idx::List(v) => v,
        }
    }
}

trait IdxOut {
    fn idx_out(&self, idx: Idx) -> Self;
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>>;
}

pub trait GetPart<T> {
    fn get_part(&self, idx: T) -> Self;
}

impl<T> GetPart<Range<usize>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Range<usize>) -> Self {
        let idx = Idx::Range(idx);
        self.idx_out(idx)
    }
}

impl<T> GetPart<Vec<usize>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Vec<usize>) -> Self {
        let idx = Idx::List(idx);
        self.idx_out(idx)
    }
}

impl<T> GetPart<ForCompare<dt>> for T
where
    T: IdxOut + Clone,
{
    fn get_part(&self, idx: ForCompare<dt>) -> Self {
        let time_vec = self.get_time_vec();
        // let idx = idx.as_ref();
        let idx = match idx {
            ForCompare::List(_) => time_vec
                .iter()
                .enumerate()
                .filter_map(|(i, t)| if idx.compare_same(t) { Some(i) } else { None })
                .collect_vec()
                .pip(Idx::List),
            _ => {
                let start_i = time_vec.iter().position(|x| idx.compare_same(x));
                let end_i = time_vec.iter().rev().position(|x| idx.compare_same(x));
                match (start_i, end_i) {
                    (Some(i), Some(j)) => i..time_vec.len() - j,
                    (Some(i), None) => i..time_vec.len(),
                    (None, Some(j)) => 0..time_vec.len() - j,
                    (None, None) => 0..0,
                }
                .pip(Idx::Range)
            }
        };
        self.idx_out(idx)
    }
}

impl<T> GetPart<T> for DataInfoList
where
    DataInfo: GetPart<T>,
    T: Clone,
{
    fn get_part(&self, idx: T) -> Self {
        let di_vec = self.dil.map(|x| x.get_part(idx.clone()));
        DataInfoList { dil: di_vec }
    }
}

impl<T> GetPart<Range<tt>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Range<tt>) -> Self {
        let index_vec = self
            .get_time_vec()
            .iter()
            .enumerate()
            .flat_map(|(i, x)| {
                if idx.contains(&x.time()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect_vec();
        let idx = Idx::List(index_vec);
        self.idx_out(idx)
    }
}

impl<T, N> GetPart<T> for Vec<N>
where
    N: GetPart<T>,
    T: Clone,
{
    fn get_part(&self, idx: T) -> Self {
        self.map(|x| x.get_part(idx.clone()))
    }
}

#[derive(Clone)]
pub enum NLast {
    Num(usize),
    Day(usize),
    DayFirst(usize),
    DayNth(usize),
}
pub const last_day: NLast = NLast::Day(1);

impl<T> GetPart<NLast> for T
where
    T: IdxOut + Clone + HasLen,
{
    fn get_part(&self, idx: NLast) -> Self {
        match idx {
            NLast::Num(n) => {
                let end = self.size();
                let start = end - n.min(end);
                let idx = Idx::Range(start..end);
                self.idx_out(idx)
            }
            NLast::Day(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let start_point = cut_points.iter().cloned().nth_back(n).unwrap_or_default();
                let end_point = cut_points.last().cloned().unwrap_or_default();
                self.get_part(start_point..end_point)
            }
            NLast::DayFirst(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let end_point = cut_points.iter().cloned().nth(n).unwrap_or_default();
                self.get_part(0..end_point)
            }
            NLast::DayNth(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let start_point = cut_points[n];
                let end_point = cut_points[n + 1];
                self.get_part(start_point..end_point)
            }
        }
    }
}

impl IdxOut for PriceTick {
    fn idx_out(&self, idx: Idx) -> Self {
        match idx {
            Idx::Range(r) => PriceTick {
                date_time: self.date_time[r.clone()].to_vec(),
                last_price: self.last_price[r.clone()].to_vec(),
                last_volume: self.last_volume[r.clone()].to_vec(),
                last_amount: self.last_amount[r.clone()].to_vec(),
                contract: self.contract[r.clone()].to_vec(),
                bid_price1: self.bid_price1[r.clone()].to_vec(),
                ask_price1: self.ask_price1[r.clone()].to_vec(),
                bid_volume1: self.bid_volume1[r.clone()].to_vec(),
                ask_volume1: self.ask_volume1[r].to_vec(),
            },
            Idx::List(v) => PriceTick {
                date_time: self.date_time.get_list_index(&v),
                last_price: self.last_price.get_list_index(&v),
                last_volume: self.last_volume.get_list_index(&v),
                last_amount: self.last_amount.get_list_index(&v),
                contract: self.contract.get_list_index(&v),
                bid_price1: self.bid_price1.get_list_index(&v),
                ask_price1: self.ask_price1.get_list_index(&v),
                bid_volume1: self.bid_volume1.get_list_index(&v),
                ask_volume1: self.ask_volume1.get_list_index(&v),
            },
        }
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.date_time)
    }
}

impl IdxOut for PriceOri {
    fn idx_out(&self, idx: Idx) -> Self {
        match idx {
            Idx::Range(r) => PriceOri {
                date_time: self.date_time[r.clone()].to_vec(),
                open: self.open[r.clone()].to_vec(),
                high: self.high[r.clone()].to_vec(),
                low: self.low[r.clone()].to_vec(),
                close: self.close[r.clone()].to_vec(),
                volume: self.volume[r.clone()].to_vec(),
                amount: self.amount[r.clone()].to_vec(),
                ki: self.ki[r.clone()].to_vec(),
                immut_info: {
                    if self.immut_info.len() < self.date_time.len() {
                        self.immut_info.clone()
                    } else {
                        self.immut_info[r.clone()].to_vec()
                    }
                },
            },
            Idx::List(v) => PriceOri {
                date_time: self.date_time.get_list_index(&v).to_vec(),
                open: self.open.get_list_index(&v).to_vec(),
                high: self.high.get_list_index(&v).to_vec(),
                low: self.low.get_list_index(&v).to_vec(),
                close: self.close.get_list_index(&v).to_vec(),
                volume: self.volume.get_list_index(&v).to_vec(),
                amount: self.amount.get_list_index(&v).to_vec(),
                ki: self.ki.get_list_index(&v).to_vec(),
                immut_info: {
                    if self.immut_info.len() < self.date_time.len() {
                        self.immut_info.clone()
                    } else {
                        self.immut_info.get_list_index(&v)
                    }
                },
            },
        }
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.date_time)
    }
}

impl IdxOut for DataInfo {
    fn idx_out(&self, idx: Idx) -> Self {
        self.pcon
            .price
            .idx_out(idx)
            .to_pcon(self.pcon.inter.clone(), self.pcon.ticker)
            .to_di()
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        self.pcon.price.get_time_vec()
    }
}

impl IdxOut for PnlRes<dt> {
    fn idx_out(&self, idx: Idx) -> Self {
        let index_vec = idx.into_vec();
        Self(
            self.0.get_list_index(&index_vec),
            self.1.map(|x| x.get_list_index(&index_vec)),
        )
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.0)
    }
}

impl IdxOut for PnlRes<da> {
    fn idx_out(&self, idx: Idx) -> Self {
        let index_vec = idx.into_vec();
        Self(
            self.0.get_list_index(&index_vec),
            self.1.map(|x| x.get_list_index(&index_vec)),
        )
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        let data = self.0.map(|x| x.to_dt());
        Cow::Owned(data)
    }
}

impl<T, N> IdxOut for InfoPnlRes<T, N>
where
    T: Clone,
    PnlRes<N>: IdxOut,
{
    fn idx_out(&self, idx: Idx) -> Self {
        InfoPnlRes(self.0.clone(), self.1.idx_out(idx))
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        self.1.get_time_vec()
    }
}

trait GetEqual<I> {
    fn get_equal(&self, other: &I) -> bool;
}

impl GetEqual<Ticker> for Ticker {
    fn get_equal(&self, other: &Ticker) -> bool {
        self == other
    }
}
impl GetEqual<Ticker> for DataInfo {
    fn get_equal(&self, other: &Ticker) -> bool {
        &self.pcon.ticker == other
    }
}
impl GetEqual<TriBox> for DataInfo {
    fn get_equal(&self, other: &TriBox) -> bool {
        &self.pcon.inter == other
    }
}
impl GetEqual<PconIdent> for DataInfo {
    fn get_equal(&self, other: &PconIdent) -> bool {
        &self.pcon.ident() == other
    }
}
impl<T> GetEqual<Vec<T>> for DataInfo
where
    DataInfo: GetEqual<T>,
    T: Sized,
{
    fn get_equal(&self, other: &Vec<T>) -> bool {
        for o in other.iter() {
            if self.get_equal(o) {
                return true;
            }
        }
        false
    }
}
impl<F> GetEqual<F> for DataInfo
where
    F: Fn(&DataInfo) -> bool,
{
    fn get_equal(&self, other: &F) -> bool {
        other(self)
    }
}

impl GetEqual<Ticker> for Stra {
    fn get_equal(&self, other: &Ticker) -> bool {
        &self.ident.ticker == other
    }
}
impl GetEqual<TriBox> for Stra {
    fn get_equal(&self, other: &TriBox) -> bool {
        &self.ident.inter == other
    }
}
impl GetEqual<PconIdent> for Stra {
    fn get_equal(&self, other: &PconIdent) -> bool {
        &self.ident == other
    }
}
impl GetEqual<StraKind> for Stra {
    fn get_equal(&self, other: &StraKind) -> bool {
        if let Some(x) = &self.name.kind {
            x == other
        } else {
            false
        }
    }
}
impl<F> GetEqual<F> for Stra
where
    F: Fn(&Stra) -> bool,
{
    fn get_equal(&self, other: &F) -> bool {
        other(self)
    }
}

#[derive(Clone)]
pub struct StraOnlyName<T>(pub T);
impl GetEqual<StraOnlyName<&str>> for Stra {
    fn get_equal(&self, other: &StraOnlyName<&str>) -> bool {
        self.name.frame() == other.0
    }
}
impl GetEqual<StraOnlyName<Vec<&str>>> for Stra {
    fn get_equal(&self, other: &StraOnlyName<Vec<&str>>) -> bool {
        other.0.contains(&self.name.frame())
    }
}
pub struct RevStraOnlyName<T>(pub T);
impl<T> GetEqual<RevStraOnlyName<T>> for Stra
where
    Stra: GetEqual<StraOnlyName<T>>,
    T: Clone,
{
    fn get_equal(&self, other: &RevStraOnlyName<T>) -> bool {
        !self.get_equal(&StraOnlyName(other.0.clone()))
    }
}
impl<T> std::ops::Not for StraOnlyName<T> {
    type Output = RevStraOnlyName<T>;
    fn not(self) -> Self::Output {
        RevStraOnlyName(self.0)
    }
}

impl<I, T, N> GetEqual<I> for InfoPnlRes<T, N>
where
    T: GetEqual<I>,
{
    fn get_equal(&self, other: &I) -> bool {
        self.0.get_equal(other)
    }
}

pub trait GetCdt<T> {
    type Output<'a>
    where
        Self: 'a;
    fn get_idx(&self, idx: T) -> Self::Output<'_>;
}

impl<T> GetCdt<T> for DataInfoList
where
    DataInfo: GetEqual<T>,
{
    type Output<'a> = DataInfoList;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.dil
            .get_idx(idx)
            .into_map(|x| x.clone())
            .pip(|x| DataInfoList { dil: x })
    }
}

impl<T> GetCdt<T> for Stral
where
    Stra: GetEqual<T>,
{
    type Output<'a> = Stral;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.0.get_idx(idx).into_map(|x| x.clone()).pip(Stral)
    }
}

impl<T, N> GetCdt<T> for [N]
where
    N: GetEqual<T>,
{
    type Output<'a> = Vec<&'a N> where N: 'a;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.iter()
            .flat_map(|x| if x.get_equal(&idx) { Some(x) } else { None })
            .collect_vec()
    }
}

pub struct OnlyOne<T>(pub T);

impl<T> GetCdt<OnlyOne<T>> for DataInfoList
where
    DataInfo: GetEqual<T>,
{
    type Output<'a> = Option<&'a DataInfo>;
    fn get_idx(&self, idx: OnlyOne<T>) -> Self::Output<'_> {
        let g = self.dil.iter().position(|x| x.get_equal(&idx.0))?;
        Some(&self.dil[g])
    }
}

pub trait HasLen {
    fn size(&self) -> usize;
}
impl<T> HasLen for [T] {
    fn size(&self) -> usize {
        self.len()
    }
}
impl HasLen for PriceTick {
    fn size(&self) -> usize {
        self.date_time.size()
    }
}
impl HasLen for PriceOri {
    fn size(&self) -> usize {
        self.date_time.size()
    }
}
impl HasLen for PriceArc {
    fn size(&self) -> usize {
        self.date_time.size()
    }
}
impl HasLen for DataInfo {
    fn size(&self) -> usize {
        self.pcon.price.size()
    }
}
impl HasLen for DataInfoList {
    fn size(&self) -> usize {
        self.dil.size()
    }
}
impl HasLen for Stral {
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl<T> HasLen for PnlRes<T> {
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl<T, N> HasLen for InfoPnlRes<T, N> {
    fn size(&self) -> usize {
        self.1.size()
    }
}
