#![allow(non_camel_case_types, unused_imports)]
use itertools::Itertools;
use super::types::*;

#[derive(Clone)]
pub enum Series {
    date(Vec<da>),
    datetime(Vec<dt>),
    time(Vec<tt>),
    f64(Vec<f64>),
}

pub struct Df<T> {
    pub index: T,
    pub value: Vec<Series>,
    pub column: Vec<&'static str>,
}

use std::ops::Add;
use std::iter::{Map, Zip};
use std::vec::IntoIter; 

/* #region  */

pub struct MyVec1(Vec<f64>);

impl Add for MyVec1 {
    type Output = Map<Zip<IntoIter<f64>, IntoIter<f64>>, fn((f64, f64)) -> f64>;
    fn add(self, rhs: MyVec1) -> Self::Output {
        self.0.into_iter().zip(rhs.0).map(|(x, y)| x + y)
    }
}



/* #endregion */

impl Add<&Self> for Series {
    type Output = Series;
    fn add(self, rhs: &Self) -> Self::Output {
        if let (Series::f64(mut data1), Series::f64(data2)) = (self, rhs) {
            data1.iter_mut()
                .zip(data2.iter())
                .for_each(|(x, y)| *x += y);
            Series::f64(data1)
        } else {
            panic!("dddd")
        }
    }
}

impl Add<f64> for Series {
    type Output = Series;
    fn add(self, rhs: f64) -> Self::Output {
        if let Series::f64(mut data1) = self {
            data1.iter_mut()
                .for_each(|x| *x += rhs);
            Series::f64(data1)
        } else {
            panic!("dddd")
        }
    }
}

impl FromIterator<f64> for Series {
    fn from_iter<T: IntoIterator<Item = f64>>(iter: T) -> Self {
        let mut res_vec = vec![];
        for x in iter {
            res_vec.push(x);
        }
        Series::f64(res_vec)
    }
}

impl Series {
    pub fn iter(&self) -> std::slice::Iter<'_, f64> {
        if let Series::f64(data) = self {
            data.iter()
        } else {
            panic!("expect f64")
        }
    }
}
