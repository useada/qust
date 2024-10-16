#![allow(non_camel_case_types)]
use std::cell::Ref;
use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use std::sync::Arc;

pub type v64 = Vec<f64>;
pub type vv64 = Vec<v64>;
pub type vuz = Vec<usize>; 
pub type R64<'a> = Ref<'a, v64>;
pub type VVa<'a> = Vec<&'a v64>;
pub type dt = NaiveDateTime;
pub type da = NaiveDate;
pub type tt = NaiveTime;
pub type vdt = Vec<dt>;
pub type vda = Vec<da>;
pub type av64 = Arc<v64>;
pub type avv64 = Vec<av64>;
pub type av_v64<'a> = Vec<&'a v64>;
pub type avdt = Arc<vdt>;
pub type avda = Arc<vda>;
pub type vv<T> = Vec<Vec<T>>;
pub type hm<K, V> = std::collections::HashMap<K, V>;