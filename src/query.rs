use std::{collections::{BTreeSet, HashMap}, fs::File, io::Error, os::unix::fs::{FileExt, MetadataExt}, sync::Arc, vec};
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{alba_types::AlbaTypes, container::Container, database::generate_secure_code, gerr, lexer_functions::Token, logerr, loginfo, query_conditions::QueryConditions, row::Row};

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

pub struct SearchArguments {
    pub element_size : usize,
    pub header_offset : usize,
    pub file : Arc<Mutex<File>>,
    pub container_values : Vec<(String,AlbaTypes)>,
    pub conditions : QueryConditions

}
const CHUNK_MATRIX : usize = 4096 * 10;

pub async fn search(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<Query, Error> {
    let element_size = args.element_size;
    let header_offset = args.header_offset;


    loginfo!("locking container...");
    let container = container.lock().await;
    loginfo!("locked");
    let arrlen_abs = container.arrlen_abs().await?;
    loginfo!("locking file...");
    let file = args.file.lock().await;
    loginfo!("File locked");

    let file_size = file.metadata().unwrap().size() as usize;
    let total_rows = (file_size - header_offset) / element_size;
    let rows_per_iteration = std::cmp::max(1, CHUNK_MATRIX / element_size).min(total_rows);
    loginfo!("File size: {}, Header offset: {}, Element size: {}", file_size, header_offset, element_size);
    loginfo!("Total rows: {}, Rows per iteration: {}", total_rows, rows_per_iteration);


    let mut rows: Vec<(Row, usize)> = Vec::new();
    let mut readen_rows = 0;
    while readen_rows < total_rows {
        let to_read = rows_per_iteration.min(total_rows - readen_rows);
        let read_size = to_read * element_size;
        loginfo!("Reading {} rows starting from offset {}", to_read, header_offset + (readen_rows * element_size));

        let mut buffer = vec![0u8; read_size];
        file.read_exact_at(&mut buffer, (header_offset + ((readen_rows * element_size).min(arrlen_abs as usize * element_size))) as u64).unwrap();

        for i in 0..to_read {
            let buff = &buffer[(i * element_size)..((i + 1) * element_size)];
            loginfo!("Deserializing row {}", readen_rows + i);
            let row = match container.deserialize_row(buff).await {
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
                            },
                            None => {
                                return Err(gerr("Invalid alba type row order, missing stuff"));
                            }
                        };
                        data.insert(value.0.clone(), column_value);
                    }
                    Row { data }
                },
                Err(e) => {
                    logerr!("Error deserializing row {}: {}", readen_rows + i, e);
                    return Err(e);
                }
            };
            rows.push((row, readen_rows + i));
        }
        readen_rows += to_read;
        loginfo!("Processed {} rows, total read: {}", to_read, readen_rows);
    }

    loginfo!("Building query from {} rows", rows.len());
    let mut query = Query::new(args.container_values.iter().map(|f| f.1.clone()).collect());
    let headers = container.column_names();
    let mut header_map = HashMap::new();
    for i in headers.iter().enumerate(){
        header_map.insert(i.1,i.0);
    }
    let l = headers.len();
    for i in rows {
        if args.conditions.row_match(&i.0).unwrap() {
            let mut r = Vec::with_capacity(l);
            for i in i.0.data.iter(){
                if let Some(a) = header_map.get(i.0){
                    r.insert(*a, i.1.to_owned());
                }
            }
            query.rows.1.push(r);
        }
    }

    loginfo!("Search completed successfully");
    Ok(query)
}

pub async fn indexed_search(container: Arc<Mutex<Container>>, args: SearchArguments, address: &BTreeSet<u64>) -> Result<Query, Error> {
    let element_size = args.element_size;
    loginfo!("Starting indexed_search | element_size: {}", element_size);
    loginfo!("container locking |indexed_search|");
    let container = container.lock().await;
    loginfo!("container locked |indexed_search|");
    loginfo!("file locking |indexed_search|");
    let file = args.file.lock().await;
    loginfo!("file locked |indexed_search|");
    let file_size = file.metadata()?.size();
    loginfo!("File size: {}, Number of addresses: {}", file_size, address.len());
    let mut rows: Vec<(Row, u64)> = Vec::new();
    for (idx, i) in address.iter().enumerate() {
        loginfo!("Processing address {} | index: {}, offset: {}", idx, *i, *i);
        let mut buffer = vec![0u8; element_size];
        let offset = *i;
        if offset > file_size {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file_size, *i);
            continue;
        }
        loginfo!("Reading data at offset: {}", offset);
        file.read_exact_at(&mut buffer, offset).unwrap();
        loginfo!("Deserializing row at offset: {}", offset);
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
        loginfo!("Row added | total rows: {}", rows.len());
    }
    loginfo!("Finished processing addresses | total rows collected: {}", rows.len());
    drop(file);
    loginfo!("File lock dropped");

    loginfo!("rows: {}",rows.len());
    loginfo!("Building query");
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

    loginfo!("indexed_search completed successfully");
    Ok(query)
}

pub async fn search_direct(container: Arc<Mutex<Container>>, args: SearchArguments) -> Result<Vec<(Vec<AlbaTypes>, u64)>, Error> {
    let element_size = args.element_size;
    let header_offset = args.header_offset;
    loginfo!("Starting search_direct | element_size: {}, header_offset: {}", element_size, header_offset);

    loginfo!("Locking file...");
    let file = args.file.lock().await;
    loginfo!("File locked");

    let file_size = file.metadata().unwrap().size() as usize;
    let total_rows = (file_size - header_offset) / element_size;
    let rows_per_iteration = std::cmp::max(1, CHUNK_MATRIX / element_size).min(total_rows);
    loginfo!("File size: {}, Total rows: {}, Rows per iteration: {}", file_size, total_rows, rows_per_iteration);
    let container_book = container.lock().await;
    let container_headers = {
        container_book.headers.clone()
    };
    loginfo!("Retrieved {} container headers", container_headers.len());

    let mut result: Vec<(Vec<AlbaTypes>, u64)> = Vec::new();
    let mut readen_rows = 0;
    while readen_rows < total_rows {
        let to_read = rows_per_iteration.min(total_rows - readen_rows);
        let read_size = to_read * element_size;
        let offset = readen_rows as u64;
        loginfo!("Reading {} rows at offset: {}", to_read, offset);

        if offset > file.metadata()?.size() {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file.metadata()?.size(), readen_rows);
            readen_rows += to_read;
            continue;
        }

        let mut buffer = vec![0u8; read_size];
        file.read_exact_at(&mut buffer, offset).unwrap();
        loginfo!("Read {} bytes from file", read_size);


        for i in 0..to_read {
            let buff = &buffer[(i * element_size)..((i + 1) * element_size)];
            let row_address = (readen_rows + i) as u64;
            loginfo!("Processing row {} at address: {}", readen_rows + i, row_address);
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
            loginfo!("Row deserialized successfully for address: {}", row_address);

            if args.conditions.row_match(&row).unwrap() {
                result.push((row.data.values().map(|f| f.to_owned()).collect(), row_address));
                loginfo!("Row matched conditions, added to result | result size: {}", result.len());
            } else {
                loginfo!("Row did not match conditions");
            }
        }
        readen_rows += to_read;
        loginfo!("Processed {} rows, total read: {}", to_read, readen_rows);
    }

    loginfo!("search_direct completed | total results: {}", result.len());
    Ok(result)
}

pub async fn indexed_search_direct(container: Arc<Mutex<Container>>, args: SearchArguments, address: &BTreeSet<u64>) -> Result<Vec<(Vec<AlbaTypes>, u64)>, Error> {
    let element_size = args.element_size;
    loginfo!("Starting indexed_search_direct | element_size: {}, address count: {}", element_size, address.len());


    loginfo!("Locking container...");
    let container = container.lock().await;
    loginfo!("Container locked");


    loginfo!("Locking file...");
    let file = container.file.lock().await;
    loginfo!("File locked");

    let file_size = file.metadata()?.size();
    loginfo!("File size: {}", file_size);

    let mut result: Vec<(Vec<AlbaTypes>, u64)> = Vec::new();
    for (idx, &row_address) in address.iter().enumerate() {
        loginfo!("Processing row {} | address: {}", idx, row_address);
        let mut buffer = vec![0u8; element_size];
        let offset = row_address as u64;
        if offset > file_size {
            logerr!("WARNING: Bad offset | offset: {} size: {} index: {}", offset, file_size, row_address);
            continue;
        }

        loginfo!("Reading data at offset: {}", offset);
        file.read_exact_at(&mut buffer, offset).unwrap();
        loginfo!("Read {} bytes for row", element_size);

        loginfo!("Deserializing row at offset: {}", offset);
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
                loginfo!("Row deserialized successfully");

                if args.conditions.row_match(&row).unwrap() {
                    loginfo!("Row matched conditions");
                    Some(row_content)
                } else {
                    loginfo!("Row did not match conditions");
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
            loginfo!("Added row to result | result size: {}", result.len());
        }
    }

    loginfo!("indexed_search_direct completed | total results: {}", result.len());
    Ok(result)
}