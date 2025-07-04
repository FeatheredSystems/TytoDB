use serde::{Deserialize, Serialize};

use crate::alba_types::AlbaTypes;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Row{
    pub data : Vec<AlbaTypes>,
}