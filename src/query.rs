use std::{fs::File, io::Error, os::unix::fs::FileExt, sync::Arc, usize, vec};
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{container::Container, query_conditions::{QueryConditions, QueryIndexType, QueryType}, row::Row, Token};

pub type PrimitiveQueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>, Vec<Row>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub rows: Rows,
}

#[derive(Clone,Debug)]
pub struct SearchArguments {
    pub element_size : usize,
    pub header_offset : usize,
    pub file : Arc<Mutex<File>>,
    pub conditions : QueryConditions

}
const CHUNK_SIZE_BYTES : usize = 4096 * 10;


pub async fn search(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<(Vec<Row>,Vec<u64>), Error> {
    let file = args.file.lock().await;
    let lck = container.lock().await;
    let size = file.metadata().unwrap().len() as usize;
    if size == args.header_offset{
        return Ok((Vec::new(),Vec::new()))
    }
    let column_names = &lck.column_names();
    let qt = args.conditions.query_type()?;
    if let QueryType::Indexed(QueryIndexType::Strict(u)) = qt{
        println!("--> u={}",u);
        if let Some(offset) = lck.index_map.lock().await.get(u)?{
            let mut buff = vec![0u8;args.element_size];
            file.read_exact_at(&mut buff, offset)?;
            let b = Row{data:lck.deserialize_row(&buff).await?};
            if args.conditions.row_match(&b, column_names)?{
                return Ok((vec![b],vec![u]))
            }else{
                return Ok((Vec::new(),Vec::new()))
            }
        }else{
            println!("not found :(");
            return Ok((Vec::new(),Vec::new()))
        }
    }

    let total_rows = (file.metadata()?.len() as usize - args.header_offset)/args.element_size;
    let rows_per_it = (CHUNK_SIZE_BYTES / args.element_size).max(1);
    let chunk_size = (rows_per_it * args.element_size).min(total_rows*args.element_size);
    let count_its = (total_rows / rows_per_it).max(1);
    let gy = lck.graveyard.lock().await;
    //let empty = vec![0u8;args.element_size];

    let mut rows = Vec::new();
    let mut offsets = Vec::new();
    for i in 0..count_its{
        if rows.len() == 100{
            break;
        }
        let mut buffer = vec![0u8;chunk_size];
        let file_offset = args.header_offset as u64 + (i * chunk_size) as u64;
        file.read_exact_at(&mut buffer, file_offset).unwrap();

        for (j,row_bin) in buffer.chunks_exact(args.element_size).enumerate(){
            //if row_bin == empty{continue;}
            let offset_in_file = args.header_offset+i*chunk_size+j*args.element_size;
            if gy.get(&(offset_in_file as u64)).is_some(){continue;}; 
            let bare_row = lck.deserialize_row(row_bin).await?;
            let row = Row { data: bare_row };
            if args.conditions.row_match(&row, &column_names)?{
                offsets.push(offset_in_file as u64);
                rows.push(row);
            }
        }
    }
    Ok((rows,offsets))
}
