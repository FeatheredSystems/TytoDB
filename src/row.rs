use std::collections::HashMap;
use crate::alba_types::AlbaTypes;


#[derive(Clone,Debug)]
pub struct Row{
    pub data : HashMap<String,AlbaTypes>,
}