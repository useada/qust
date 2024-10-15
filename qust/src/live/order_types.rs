#![allow(unused_imports)]
use serde::{ Serialize, Deserialize };
use qust_ds::prelude::*;
use qust_derive::*;
use crate::loge;
use crate::prelude::{PconIdent, Ticker};
use crate::sig::prelude::{NormHold, ToNum};
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

fn generate_order_ref() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(16).collect()
}


#[derive(Clone, Debug, thiserror::Error)]
pub enum OrderError {
    #[error("order error: {0}")]
    Message(String),
    #[error("order not found by order_ref: {0}")]
    OrderNotFound(String),
    #[error("di not found {0:?}")]
    DiNotFound(PconIdent),
    #[error("order logic error: {0}")]
    Logic(String)
}

pub type OrderResult<T> = Result<T, OrderError>;

#[ta_derive]
#[derive(Default, PartialEq)]
pub enum OrderAction {
    LongOpen(i32, f32),
    LongClose(i32, f32),
    LongCloseYd(i32, f32),
    ShortOpen(i32, f32),
    ShortClose(i32, f32),
    ShortCloseYd(i32, f32),
    #[default]
    Nothing,
}


impl From<NormHold> for LiveTarget {
    fn from(value: NormHold) -> Self {
        match value {
            NormHold::Nothing => Self::Nothing,
            NormHold::Long(i) => Self::Long(i),
            NormHold::Short(i) => Self::Short(i),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub enum LiveTarget {
    #[default]
    Nothing,
    Long(f32),
    Short(f32),
    OrderAction(OrderAction),
}

impl LiveTarget {
    pub fn add_live_target(&self, other: &Self) -> Self {
        use LiveTarget::*;
        match (self, other) {
            (Nothing, other) => other.clone(),
            (other, Nothing) => other.clone(),
            (Long(n1), Long(n2)) => Long(n1 + n2),
            (Short(n1), Short(n2)) => Short(n1 + n2),
            (Long(n1), Short(n2)) => {
                if n1 > n2 {
                    Long(n1 - n2)
                } else if n1 < n2 {
                    Short(n2 - n1)
                } else {
                    Nothing
                }
            }
            (Short(n1), Long(n2)) => {
                if n1 > n2 {
                    Short(n1 - n2)
                } else if n1 < n2 {
                    Long(n1 - n2)
                } else {
                    Nothing
                }
            }
            _ => panic!("cannot add: {:?} {:?}", self, other),
        }
    }
}

impl ToNum for LiveTarget {
    fn to_num(&self) -> f32 {
        use LiveTarget::*;
        match self {
            Long(i) => *i,
            Short(i) => -i,
            Nothing => 0.,
            _ => panic!("cannot convert to num for: {:?}", self),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct HoldLocal {
    pub yd_short: i32,
    pub yd_long: i32,
    pub td_short: i32,
    pub td_long: i32,
    pub exit_short: i32,
    pub exit_long: i32,
}

impl HoldLocal {
    pub fn sum(&self) -> i32 {
        self.yd_long + self.td_long - self.yd_short - self.td_short
    }

    pub fn sum_pending(&self) -> i32 {
        self.sum() + self.exit_long - self.exit_short
    }
}



#[derive(Clone, Debug, Default)]
pub enum OrderStatus {
    #[default]
    SubmittingToApi,
    AllTraded,
    PartTradedQueueing(i32),
    Canceled(i32),
    NotTouched,
    Unknown(char),
    Inserted,
    InsertError(i32),
}

#[derive(Clone, Debug, Default)]
pub struct OrderSend {
    pub id: String,
    pub order_action: OrderAction,
    pub order_status: OrderStatus,
    pub is_to_cancel: bool,
    pub create_time: dt,
    pub update_time: dt,
    pub order_ref: Option<[i8; 13]>,
    pub front_id: Option<i32>,
    pub session_id: Option<i32>,
    pub exchange_id: Option<[i8; 9]>,
}

#[derive(Clone, Debug, Default)]
pub struct OrderReceive {
    pub id: String,
    pub order_status: OrderStatus,
    pub update_time: dt,
    pub order_ref: Option<[i8; 13]>,
    pub front_id: Option<i32>,
    pub session_id: Option<i32>,
    pub exchange_id: Option<[i8; 9]>,
}


#[derive(Debug)]
pub struct OrderPool {
    pub ticker: Ticker,
    pub hold: HoldLocal,
    pub pool: hm<String, OrderSend>,
}

impl OrderPool {
    pub fn create_order(&mut self, order_action: OrderAction) -> OrderSend {
        // let order_ref: String = uuid::Uuid::new_v4().to_string().chars().take(12).collect();
        let order_id = generate_order_ref();
        let new_order = OrderSend {
            id: order_id.clone(),
            order_action,
            order_status: OrderStatus::SubmittingToApi,
            create_time: chrono::Local::now().naive_local(),
            update_time: chrono::Local::now().naive_local(),
            is_to_cancel: false,
            order_ref: None,
            front_id: None,
            session_id: None,
            exchange_id: None,
        };
        loge!(self.ticker, "order pool create a order: {:?}", new_order);
        self.pool.insert(order_id, new_order.clone());
        new_order
    }

    pub fn cancel_order(&mut self, order_ref: &str) -> OrderResult<Option<OrderSend>> {
        let order = self.pool
            .get_mut(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))?;

        match order.is_to_cancel {
            true => {
                loge!(self.ticker, "cancel_order: canceling");
                Ok(None)
            }
            false => {
                loge!(self.ticker, "cancel_order: set order canceling");
                order.is_to_cancel = true;
                Ok(Some(order.clone()))
            }
        }
    }

    fn delete_order(&mut self, order_ref: &str) -> OrderResult<OrderSend> {
        self.pool
            .remove(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))
    }

    fn finished_order_update(&mut self, order_ref: &str, c: Option<i32>) -> OrderResult<bool> {
        let order_action = self
            .pool
            .get(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))?;

        match &order_action.order_action {
            OrderAction::LongOpen(i, _) => {
                self.hold.td_long += c.unwrap_or(*i);
            }
            OrderAction::ShortOpen(i, _) => {
                self.hold.td_short += c.unwrap_or(*i);
            }
            OrderAction::LongClose(i, _) => {
                self.hold.td_short -= c.unwrap_or(*i);
            }
            OrderAction::ShortClose(i, _) => {
                self.hold.td_long -= c.unwrap_or(*i);
            }
            other => {
                return Err(OrderError::Logic(format!("unknown order action on what? {:?} {:?}", other, line!())));
            }
        }

        self.delete_order(order_ref)?;
        Ok(true)
    }

    pub fn update_order(&mut self, order: OrderReceive) -> OrderResult<bool> {
        loge!(self.ticker, "order pool get a order rtn from ctp: {:?}", order);
        loge!(self.ticker, "order pool: {:?}", self.pool.iter().map(|x| x.0.to_string()).collect_vec());
        let order_local = self
            .pool
            .get_mut(&order.id)
            .ok_or(OrderError::OrderNotFound(order.id.clone()))?;

        order_local.order_ref = order.order_ref;
        order_local.front_id = order.front_id;
        order_local.session_id = order.session_id;
        order_local.exchange_id = order.exchange_id;
        order_local.update_time = order.update_time;
        order_local.order_status = order.order_status.clone();
        let is_changed = match order.order_status {
            OrderStatus::AllTraded => {
                loge!(self.ticker, "order pool order update finished");
                self.finished_order_update(&order.id,  None)?
            }
            OrderStatus::Canceled(i) => {
                loge!(self.ticker, "order pool order update canceled");
                self.finished_order_update(&order.id, Some(i))?
            }
            OrderStatus::InsertError(_i) => {
                loge!(self.ticker, "order pool order update insert error");
                self.delete_order(&order.id)?;
                false
            }
            _ => { false }
        };
        Ok(is_changed)
    }

    fn is_need_to_wait(&self) -> bool {
        let mut res = false;
        for order_input in self.pool.values() {
            if let OrderStatus::NotTouched | OrderStatus::Unknown(_) | OrderStatus::SubmittingToApi = order_input.order_status {
                res = true;
                break;
            }
        }
        res
    }

    fn get_to_cancel_order(&self, order_action: &OrderAction) -> CancelResult {
        use OrderStatus::*;

        // @@ 暂时不取消所有订单
        // if let OrderAction::Nothing = order_action {
        //     return if !self.pool.is_empty() {
        //         CancelResult::CancelAll
        //     } else {
        //         CancelResult::DoNothing
        //     }
        // }

        for order_input in self.pool.values() {
            if let  PartTradedQueueing(_) = order_input.order_status {
                return if &order_input.order_action != order_action {
                    CancelResult::HaveDiffOrder(order_input.id.clone())
                } else {
                    CancelResult::HaveTheSameOrder
                }
            }
        }

        CancelResult::NotHave
    }

    pub fn process_order_action(&mut self, order_action: OrderAction) -> OrderResult<Option<OrderSend>> {
        if self.is_need_to_wait() {
            std::thread::sleep(std::time::Duration::from_millis(10));
            loge!(self.ticker, "order pool: frequency is too high");
            return Ok(None);
        }

        match self.get_to_cancel_order(&order_action) {
            CancelResult::HaveTheSameOrder => {
                loge!(self.ticker, "order pool: have same order");
                Ok(None)
            }
            CancelResult::HaveDiffOrder(order_ref) => {
                let order_res = self.cancel_order(&order_ref)?;
                loge!(self.ticker, "order pool: cancel the old order: {:?}", order_res);
                Ok(order_res)
            }
            CancelResult::NotHave => {
                let order_res = self.create_order(order_action);
                loge!(self.ticker, "order pool: create a new order: {:?}", order_res);
                Ok(Some(order_res))
            }
            CancelResult::CancelAll => {
                loge!(self.ticker, "order pool: cancel all orders with OrderAction::{:?}", order_action);
                match self.pool.keys().take(1).next().cloned() {
                    Some(order_id) => self.cancel_order(&order_id),
                    None => Ok(None),
                }
            }
            CancelResult::DoNothing => {
                loge!(self.ticker, "order pool: nothing to do with OrderAction::{:?}", order_action);
                Ok(None)
            }
        }
    }
}


enum CancelResult {
    HaveTheSameOrder,
    HaveDiffOrder(String),
    NotHave,
    CancelAll,
    DoNothing,
}