use crate::manager::{
    manager::Manager,
    status::{AnyJob, TransitionPlan},
};

use super::error::ManagerError;
use super::status::Job;

impl Manager {
    /// 执行状态机转换产生的TransitPlan并更改Manager中Job状态机
    pub async fn apply_plan<NextSt>(
        &mut self,
        plan: TransitionPlan<NextSt>,
    ) -> Result<(), ManagerError>
    where
        Job<NextSt>: Into<AnyJob>,
    {
        todo!()
    }
}
