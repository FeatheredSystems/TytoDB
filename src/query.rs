use std::{collections::BTreeSet, fs::File, io::Error, os::unix::fs::FileExt, sync::Arc, usize, vec};
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{container::Container, Token, query_conditions::QueryConditions, row::Row};

pub type PrimitiveQueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>, Vec<Row>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub rows: Rows,
}

#[derive(Clone)]
pub struct SearchArguments {
    pub element_size : usize,
    pub header_offset : usize,
    pub file : Arc<Mutex<File>>,
    pub conditions : QueryConditions

}
const CHUNK_MATRIX : usize = 4096 * 10;


pub async fn search(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<(Vec<Row>,Vec<u64>), Error> {
    let file = args.file.lock().await;
    let lck = container.lock().await;
    let graveyard: tokio::sync::MutexGuard<'_, BTreeSet<u64>> = lck.graveyard.lock().await;
    let size = file.metadata().unwrap().len() as usize;
    if size == args.header_offset{
        return Ok((Vec::new(),Vec::new()))
    }
    let dataset_size = (size - args.header_offset)/args.element_size;
    if dataset_size == 0{
        return Ok((Vec::new(),Vec::new()))
    }

    let read_chunk = {
        let m = CHUNK_MATRIX / args.element_size;
        if m < 1{1}else{m}
    };
    let cn: Vec<String> = lck.column_names();
    let mut result = Vec::new();
    let mut offsets = Vec::new();

    for i in 0..(dataset_size/read_chunk){
        let mut buffer = vec![0u8;(read_chunk*args.element_size) as usize];
        file.read_exact_at(&mut buffer, (read_chunk *args.element_size*i) as u64)?;
        for (idx,j) in buffer.chunks_exact(args.element_size).enumerate(){
            let abs = (((i*read_chunk)*args.element_size+idx*args.element_size)+args.header_offset) as u64;
            if graveyard.get(&((((i*read_chunk)+idx)) as u64)).is_none(){
                let val = Row {data:lck.deserialize_row(j).await?};
                if args.conditions.row_match(&val, &cn)?{
                    result.push(val);
                    offsets.push(abs);
                };
            }
        }
    }

    Ok((result,offsets))
}
