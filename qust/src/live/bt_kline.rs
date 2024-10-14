use std::thread;
use crate::prelude::*;

pub trait BtKline<Input> {
    type Output;
    fn bt_kline(&self, input: Input) -> Self::Output;
}


impl<'a> BtKline<(&'a DataInfo, CommSlip)> for Ptm {
    type Output = PnlRes<dt>;
    fn bt_kline(&self, input: (&DataInfo, CommSlip)) -> Self::Output {
        input.0.pnl(self, input.1)
    }
}

impl<'a> BtKline<(&'a DataInfo, CommSlip)> for Vec<Ptm> {
    type Output = Vec<PnlRes<dt>>;
    fn bt_kline(&self, input: (&'a DataInfo, CommSlip)) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for ptm in self.iter() {
                let di = input.0;
                let comm_slip = input.1.clone();
                let handle = scope.spawn(move || {
                    ptm.bt_kline((di, comm_slip))
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .map(|x| x.join().unwrap())
                .collect()
        })
    }
}

impl<'a, T, N> BtKline<(Vec<&'a DataInfo>, CommSlip)> for T
where
    T: BtKline<(&'a DataInfo, CommSlip), Output = N> + Clone + Send + Sync,
    N: Send + Sync,
{
    type Output = Vec<N>;
    fn bt_kline(&self, input: (Vec<&'a DataInfo>, CommSlip)) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for di in input.0.into_iter() {
                let ptm = self.clone();
                let comm_slip = input.1.clone();
                let handle = scope.spawn(move || {
                    ptm.bt_kline((di, comm_slip))
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .map(|x| x.join().unwrap())
                .collect()
        })
    }
}