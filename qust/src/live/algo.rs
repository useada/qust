#![allow(unused_imports)]
use crate::loge;
use crate::prelude::{TickData, Ticker};
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

use super::prelude::{HoldLocal, LiveTarget, OrderAction, RetFnAlgo };
use crate::sig::prelude::ToNum;

#[clone_trait]
pub trait Algo {
    fn algo(&self, ticker: Ticker) -> RetFnAlgo;
}

#[ta_derive]
pub struct TargetSimple;

#[typetag::serde]
impl Algo for TargetSimple {
    fn algo(&self, _ticker: Ticker) -> RetFnAlgo {
        Box::new(move |stream_algo| {
            use OrderAction::*;
            let target = stream_algo.live_target.to_num() as i32;
            let hold_local = stream_algo.stream_api.hold;
            let tick_data = stream_algo.stream_api.tick_data;
            let gap = target - hold_local.sum();
            match (gap, target, hold_local.yd_short, hold_local.yd_long, hold_local.td_short, hold_local.td_long) {
                (0, ..) => Nothing,
                (_, 0.., 1.., ..) => LongCloseYd(hold_local.yd_short, tick_data.bid_price1),
                (_, 0.., 0, _, 1.., _) => LongClose(hold_local.td_short, tick_data.bid_price1),
                (0.., 0.., 0, _, 0, _) => LongOpen(gap, tick_data.bid_price1),
                (..=-1, 0.., 0, 1.., 0, 0..) => {
                    if hold_local.yd_long >= -gap {
                        ShortCloseYd(-gap, tick_data.ask_price1)
                    } else {
                        ShortCloseYd(hold_local.yd_long, tick_data.ask_price1)
                    }
                }
                (..=-1, 0.., 0, 0, 0, 1..) => ShortClose(-gap, tick_data.ask_price1),
                (_, ..=-1, _, 1.., ..) => ShortCloseYd(hold_local.yd_long, tick_data.ask_price1),
                (_, ..=-1, _, 0, _, 1..) => ShortClose(hold_local.td_long, tick_data.ask_price1),
                (..=-1, ..=-1, _, 0, _, 0) => ShortOpen(-gap, tick_data.ask_price1),
                (0.., ..=-1, 1.., 0, 0.., 0) => {
                    if hold_local.yd_short >= gap {
                        LongCloseYd(gap, tick_data.bid_price1)
                    } else {
                        LongCloseYd(hold_local.yd_short, tick_data.bid_price1)
                    }
                }
                (0.., ..=-1, 0, 0, 1.., 0) => LongClose(gap, tick_data.bid_price1),
                _ => panic!("something action wrong"),
            }
        })
    }
}

#[ta_derive]
#[derive(Default)]
pub struct TargetPriceDum {
    original_price: f32,
    exit_counts: usize,
    last_action: OrderAction,
    n_thre: usize,
    open_counts: usize,
    n_thre_open: usize,
}

impl TargetPriceDum {
    pub fn from_n_thre(n: usize, n_open: usize) -> Self {
        Self {
            n_thre: n,
            n_thre_open: n_open,
            ..Default::default()
        }
    }
}


#[ta_derive]
pub struct AlgoTargetAndPrice;


#[typetag::serde]
impl Algo for AlgoTargetAndPrice {
    fn algo(&self, ticker:Ticker) -> RetFnAlgo {
        Box::new(move |stream_algo| {
            let LiveTarget::OrderAction(target) = &stream_algo.live_target else { 
                panic!("wrong match algo: {} {:?}", ticker, stream_algo.live_target);
            };
            target.clone()
        })
    }
}

