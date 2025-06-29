use std::{collections::{btree_map::Range, BTreeSet, HashMap}, fs::File, hash::{DefaultHasher, Hash, Hasher}, io::Error, ops::RangeInclusive, os::unix::fs::{FileExt, MetadataExt}, sync::Arc, usize, vec};
use ahash::AHashSet;
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{alba_types::AlbaTypes, container::Container, database::generate_secure_code, gerr, Token, logerr, loginfo, query_conditions::QueryConditions, row::Row};

pub type PrimitiveQueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>, Vec<Vec<AlbaTypes>>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub rows: Rows,
    pub current_page: usize,
    pub column_names: Vec<String>,
    pub column_types: Vec<AlbaTypes>,
    pub id: String,
}

impl Query {

    pub fn trim(&mut self) {
        
        self.column_types = self
            .column_types
            .iter()
            .filter(|p| !matches!(p, AlbaTypes::NONE))
            .cloned()
            .collect();
        self.column_names = self
            .column_names
            .iter()
            .filter(|p| !p.is_empty())
            .cloned()
            .collect();
    }

    pub fn new(column_types: Vec<AlbaTypes>) -> Self {
        
        let mut n = Query {
            rows: (Vec::new(), Vec::new()), 
            current_page: 0, 
            column_names: Vec::new(), 
            column_types,
            id: generate_secure_code(100),
        };
        n.trim();
        
        n
    }

    pub fn new_none(column_types: Vec<AlbaTypes>) -> Self {
        
        let mut a = Query {
            rows: (Vec::new(), Vec::new()), 
            current_page: 0, 
            column_names: Vec::new(), 
            column_types,
            id: "".to_string(),
        };
        a.trim();
        
        a
    }

    pub fn join(&mut self, foreign: Query) {
        if foreign.column_types != self.column_types {
            return;
        }
        self.rows.0.extend_from_slice(&foreign.rows.0);
        self.rows.1.extend_from_slice(&foreign.rows.1);
        
        self.trim();
    }

}

#[derive(Clone)]
pub struct SearchArguments {
    pub element_size : usize,
    pub header_offset : usize,
    pub file : Arc<Mutex<File>>,
    pub container_values : Vec<(String,AlbaTypes)>,
    pub conditions : QueryConditions

}
const CHUNK_MATRIX : usize = 4096 * 10;

pub async fn search(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<Query, Error> {
    let file = args.file.lock().await;
    let lck = container.lock().await;
    let graveyard = lck.graveyard.lock().await;
    let size = file.metadata().unwrap().len() as usize;
    let columns = lck.column_names();
    let mut query = Query::new(args.container_values.iter().map(|f| f.1.clone()).collect());
    query.rows.0 = lck.column_names();

    if size == args.header_offset{
        return Ok(query)
    }
    loginfo!("size {:?}",size);
    loginfo!("elementsize {:?}",args.element_size);
    loginfo!("headersize {:?}",args.header_offset);
    let to_read = (size-args.header_offset)/args.element_size;
    loginfo!("toread: {}",to_read);

    let r = to_read;
    loginfo!("r: {:?}",r);
    for i in 0..r{
        let i = (i * args.element_size) + args.header_offset;
        if i + args.element_size > size {
            break;
        }
        if graveyard.get(&(i as u64)).is_none(){
            let mut b = vec![0u8;args.element_size];
            loginfo!("i:{}",i);
            if file.read_exact_at(&mut b, i as u64).is_err(){break;};
            let r = lck.deserialize_row(&mut b).await.unwrap();
            let mut h : HashMap<String,AlbaTypes> = HashMap::new();
            for i in r.iter().zip(columns.iter().cloned()){
                h.insert(i.1,i.0.to_owned());
            }
            let row = Row{data:h};
            if args.conditions.row_match(&row).unwrap(){
                query.rows.1.push(r);
            }
        }
    }


    Ok(query)
}

pub async fn search_direct(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<Vec<(Vec<AlbaTypes>, u64)>, Error> {
    let element_size = args.element_size;
    let header_offset = args.header_offset;
    

    
    let file = args.file.lock().await;
    

    let file_size = file.metadata().unwrap().size() as usize;
    let total_rows = (file_size - header_offset) / element_size;
    let rows_per_iteration = std::cmp::max(1, CHUNK_MATRIX / element_size).min(total_rows);
    
    let container_book = container.lock().await;
    let container_headers = {
        container_book.headers.clone()
    };
    

    let mut result: Vec<(Vec<AlbaTypes>, u64)> = Vec::new();
    let mut readen_rows = 0;
    while readen_rows < total_rows {
        let to_read = rows_per_iteration.min(total_rows - readen_rows);
        let read_size = to_read * element_size;
        let offset = readen_rows as u64;
        

        if offset > file.metadata()?.size() {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file.metadata()?.size(), readen_rows);
            readen_rows += to_read;
            continue;
        }

        let mut buffer = vec![0u8; read_size];
        file.read_exact_at(&mut buffer, offset).unwrap();
        


        for i in 0..to_read {
            let buff = &buffer[(i * element_size)..((i + 1) * element_size)];
            let row_address = (readen_rows + i) as u64;
            
            let row = match container_book.deserialize_row(buff).await {
                Ok(row_content) => {
                    let mut data: HashMap<String, AlbaTypes> = HashMap::new();
                    for (index, value) in container_headers.iter().enumerate() {
                        let column_value = match row_content.get(index) {
                            Some(a) => {
                                let cv = a.to_owned();
                                if std::mem::discriminant(&cv) != std::mem::discriminant(&value.1) {
                                    return Err(gerr("Invalid alba type row order, unmatching stuff"));
                                }
                                cv
                            }
                            None => {
                                return Err(gerr("Invalid alba type row order, missing stuff"));
                            }
                        };
                        data.insert(value.0.clone(), column_value);
                    }
                    Row { data }
                }
                Err(e) => {
                    logerr!("Error deserializing row {}: {}", row_address, e);
                    return Err(e);
                }
            };
            

            if args.conditions.row_match(&row).unwrap() {
                result.push((row.data.values().map(|f| f.to_owned()).collect(), row_address));
                
            } else {
                
            }
        }
        readen_rows += to_read;
        
    }

    
    Ok(result)
}

pub async fn indexed_search_direct(container: Arc<Mutex<Container>>, args: SearchArguments, address: &BTreeSet<u64>) -> Result<Vec<(Vec<AlbaTypes>, u64)>, Error> {
    let element_size = args.element_size;
    let container = container.lock().await;
    let file = container.file.lock().await;
    let file_size = file.metadata()?.size();
    let mut runned : AHashSet<u64> = AHashSet::new();
    let mut result: Vec<(Vec<AlbaTypes>, u64)> = Vec::new();
    for (idx, &row_address) in address.iter().enumerate() {
        loginfo!("row_address: {}",row_address);
        let mut buffer = vec![0u8; element_size];
        let offset = row_address as u64;
        if offset > file_size {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file_size, row_address);
            continue;
        }
        if runned.get(&offset).is_some(){
            continue;
        }
        runned.insert(offset);
        file.read_exact_at(&mut buffer, offset).unwrap();
        let row_content = match container.deserialize_row(&buffer).await {
            Ok(row_content) => {
                let mut data: HashMap<String, AlbaTypes> = HashMap::new();
                for (index, value) in container.headers.iter().enumerate() {
                    let column_value = match row_content.get(index) {
                        Some(a) => {
                            let cv = a.to_owned();
                            if std::mem::discriminant(&cv) != std::mem::discriminant(&value.1) {
                                return Err(gerr("Invalid alba type row order, unmatching stuff"));
                            }
                            cv
                        }
                        None => {
                            return Err(gerr("Invalid alba type row order, missing stuff"));
                        }
                    };
                    data.insert(value.0.clone(), column_value);
                }
                let row = Row { data };
                

                if args.conditions.row_match(&row).unwrap() {
                    
                    Some(row_content)
                } else {
                    
                    None
                }
            }
            Err(e) => {
                logerr!("Error deserializing row at offset {}: {}", offset, e);
                return Err(e);
            }
        };

        if let Some(content) = row_content {
            result.push((content, row_address));
            
        }
    }

    
    Ok(result)
}



pub async fn indexed_search(container: Arc<Mutex<Container>>, args: SearchArguments, address: &BTreeSet<u64>) -> Result<Query, Error> {
    let element_size = args.element_size;
    
    
    let container = container.lock().await;
    
    
    let file = args.file.lock().await;
    
    let file_size = file.metadata()?.size();
    let mut runned : AHashSet<u64> = AHashSet::new();
    let mut rows: Vec<(Row, u64)> = Vec::new();
    for i in address.iter() {
        loginfo!("row_address: {}",i);
        let mut buffer = vec![0u8; element_size];
        let offset = *i;
        if offset > file_size {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file_size, *i);
            continue;
        }
        if runned.get(&offset).is_some(){continue;}
        
        file.read_exact_at(&mut buffer, offset).unwrap();
        runned.insert(offset);
        let row = match container.deserialize_row(&buffer).await {
            Ok(row_content) => {
                let mut data: HashMap<String, AlbaTypes> = HashMap::new();
                for (index, value) in container.headers.iter().enumerate() {
                    let column_value = match row_content.get(index) {
                        Some(a) => {
                            let cv = a.to_owned();
                            if std::mem::discriminant(&cv) != std::mem::discriminant(&value.1) {
                                return Err(gerr("Invalid alba type row order, unmatching stuff"));
                            }
                            cv
                        }
                        None => {
                            return Err(gerr("Invalid alba type row order, missing stuff"));
                        }
                    };
                    data.insert(value.0.clone(), column_value);
                }
                Row { data }
            },
            Err(e) => {
                logerr!("Error deserializing row at offset {}: {}", offset, e);
                return Err(e);
            }
        };
        if args.conditions.row_match(&row).unwrap(){
            rows.push((row, *i));
        }
        
    }
    
    drop(file);
    

    
    
    let mut query = Query::new(args.container_values.iter().map(|f| f.1.clone()).collect());
    let headers = container.column_names();
    let mut header_map = HashMap::new();
    for i in headers.iter().enumerate(){
        header_map.insert(i.1,i.0);
    }
    let v = container.columns();
    for i in rows {
        let mut r = v.clone();
        for i in i.0.data.iter(){
            if let Some(a) = header_map.get(i.0){
                r.insert(*a, i.1.to_owned());
            }
        }
        query.rows.1.push(r);
        
    }

    
    Ok(query)
}
