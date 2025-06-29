use std::{collections::HashMap, fs, io::{Error, ErrorKind, Read, Write}, os::unix::fs::FileExt, path::PathBuf, pin::Pin, str::FromStr, sync::Arc};
use ahash::AHashMap;
use base64::{alphabet, engine::{self, GeneralPurpose}, Engine};
use futures::future::UnwrapOrElse;
use lazy_static::lazy_static;
use serde::{Serialize,Deserialize};
use serde_yaml;
use crate::{alba_types::AlbaTypes, container::Container, gerr, indexing::Search, logerr, loginfo, query::{indexed_search, indexed_search_direct, search, search_direct, Query, SearchArguments}, query_conditions::{QueryConditions, QueryType}, AlbaContainer, AstCommit, AstCreateRow, AstDeleteContainer, AstDeleteRow, AstEditRow, AstRollback, AstSearch, Token, AST};
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng, RngCore};
use tokio::sync::Mutex;
/////////////////////////////////////////////////
/////////     DEFAULT_SETTINGS    ///////////////
/////////////////////////////////////////////////

pub const MAX_STR_LEN : usize = 128;
const DEFAULT_SETTINGS : &str = r#"
max_columns: 125
min_columns: 1
memory_limit: 4096
auto_commit: false
ip: "127.0.0.1"
port: 4287
workers: 1
"#;

#[derive(Serialize,Deserialize, Default,Debug)]
struct Settings{
    max_columns : u32,
    min_columns : u32,
    memory_limit : u32,
    auto_commit : bool,
    ip:String,
    port: u32,
    workers: u32
}

const SECRET_KEY_PATH : &str = "TytoDB/.secret";
pub const DATABASE_PATH : &str = "TytoDB";

pub fn database_path() -> String{
    let first = std::env::var("HOME").unwrap();
    return format!("{}/{}",first,DATABASE_PATH)
}
fn secret_key_path() -> String{
    let first = std::env::var("HOME").unwrap();
    return format!("{}/{}",first,SECRET_KEY_PATH)
}
/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////



#[link(name = "io", kind = "static")]
unsafe extern "C" {
    pub fn write_data(buffer: *const u8, len: usize, path: *const std::os::raw::c_char) -> i32;
}

pub fn generate_secure_code(len: usize) -> String {
    let mut rng = rand::rngs::OsRng;
    let code: String = (0..len)
        .map(|_| rng.sample(Alphanumeric))
        .map(char::from)
        .collect();
    code
}

#[derive(Default,Debug)]
pub struct Database{
    location : String,
    settings : Settings,
    containers : Vec<String>,
    headers : Vec<(Vec<String>,Vec<AlbaTypes>)>,
    pub container : HashMap<String,Arc<Mutex<Container>>>,
}

fn check_for_reference_folder(location : &String) -> Result<(), Error>{
    let path = format!("{}/rf",location);
    if !match fs::exists(path.clone()){Ok(a)=>a,Err(e)=>{return Err(e)}}{
        return match fs::create_dir(path){
            Ok(a)=>Ok(a),
            Err(e)=>Err(e)
        }
    }
    Ok(())
}



const SETTINGS_FILE : &str = "settings.yaml";

lazy_static!{
    static ref B64_ENGINE : GeneralPurpose = new_b64_engine();
}

fn new_b64_engine() -> GeneralPurpose{
    let crazy_config = engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_encode_padding(true)
        .with_decode_padding_mode(engine::DecodePaddingMode::Indifferent);
    return base64::engine::GeneralPurpose::new(&alphabet::Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/").unwrap(), crazy_config);
}

impl Database{
    fn set_default_settings(&self) -> Result<(), Error> {
        let path = format!("{}/{}", self.location, SETTINGS_FILE);
        
        if fs::metadata(&path).is_err() {
            
            let mut file = fs::File::create_new(&path)?;
            
            let content = DEFAULT_SETTINGS.to_string();
            
            file.write_all(content.as_bytes())?;
            
        } else {
            
        }
        Ok(())
    }
    
    async fn load_containers(&mut self) -> Result<(), Error> {
        check_for_reference_folder(&database_path())?;
        let path = format!("{}/containers.yaml", &self.location);
        if !fs::exists(&path).unwrap() {
            
            let yaml = serde_yaml::to_string(&self.containers)
                .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string())).unwrap();
            let mut file = fs::File::create_new(path).unwrap();
            file.write_all(&yaml.as_bytes()).unwrap();
            
            return Ok(());
        }
        let mut file = fs::File::open(path).unwrap();
        
        let mut raw = String::new();
        file.read_to_string(&mut raw).unwrap();
        
        self.containers = serde_yaml::from_str(&raw)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string())).unwrap();
        
        self.headers.clear();
        
        for contain in self.containers.iter() {
            
            let (he,header_offset) = self.get_container_headers(&contain).unwrap();
            
            self.headers.push(he.clone());
            
            let mut element_size: usize = 0;
            for el in he.1.iter() {
                element_size += el.size();
                
            }
            
            self.container.insert(
                contain.to_string(),
                Container::new(
                    contain.to_string(),
                    &format!("{}/{}", self.location, contain),
                    self.location.clone(),
                    element_size,
                    he.1,
                    MAX_STR_LEN,
                    header_offset,
                    he.0.clone(),
                ).await.unwrap(),
            );
            
        }
        for (_, wedfygt) in self.container.iter() {
            let wedfygt = wedfygt.lock().await;
            let count = (wedfygt.len().await? - wedfygt.headers_offset) / wedfygt.element_size as u64;
            
            if count < 1 {
                
                continue;
            }
            for i in 0..count {
                
                let mut wb = vec![0u8; wedfygt.element_size];
                
                if let Err(e) = wedfygt.file.lock().await.read_exact_at(
                    &mut wb,
                    wedfygt.headers_offset as u64 + (wedfygt.element_size as u64 * i as u64),
                ) {
                    logerr!("{}", e);
                    
                    continue;
                };
                if wb == vec![0u8; wedfygt.element_size] {
                    
                    wedfygt.graveyard.lock().await.insert(i);
                    
                }
            }
        }
        
        Ok(())
    }
    
    fn save_containers(&self) -> Result<(), Error> {
        let path = std::path::PathBuf::from(&self.location).join("containers.yaml");
        
        let yaml = serde_yaml::to_string(&self.containers)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        
        fs::remove_file(&path)?;
        
        fs::write(&path, yaml.as_bytes())?;
        
        Ok(())
    }
    
    pub async fn commit(&mut self) -> Result<(), Error> {
        
        for (_, c) in self.container.iter_mut() {
            
            c.lock().await.commit().await?;
            
        }
        
        Ok(())
    }
    
    pub async fn rollback(&mut self) -> Result<(), Error> {
        
        for (_, c) in self.container.iter_mut() {
            
            c.lock().await.rollback().await?;
            
        }
        
        Ok(())
    }
    
    pub async fn setup(&self) -> Result<(), Error> {
        let db_path = database_path();
        
        if !std::fs::exists(&db_path)? {
            
            std::fs::create_dir(&db_path)?;
            
        } else {
            
        }
        Ok(())
    }
    
    fn load_settings(&mut self) -> Result<(), Error> {
        let dir = PathBuf::from(&self.location);
        
        let path = dir.join(SETTINGS_FILE);
        
        fs::create_dir_all(&dir)?;
        
        if path.exists() && fs::metadata(&path)?.is_dir() {
            
            fs::remove_dir(&path)?;
            
        }
        if !path.is_file() {
            
            self.set_default_settings()?;
            
        }
        let mut rewrite = true;
        
        let raw = fs::read_to_string(&path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to read {}: {}", SETTINGS_FILE, e)))?;
        
        let mut settings: Settings = serde_yaml::from_str(&raw)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Invalid {}: {}", SETTINGS_FILE, e)))?;
        
        if settings.max_columns <= settings.min_columns {
            
            settings.min_columns = 1;
            rewrite = true;
        }
        if settings.max_columns <= 1 {
            
            settings.max_columns = 10;
            rewrite = true;
        }
        if settings.min_columns > settings.max_columns {
            
            settings.min_columns = 1;
            rewrite = true;
        }
        if settings.memory_limit < 1_048_576 {
            
            settings.memory_limit = 1_048_576;
            rewrite = true;
        }
        if rewrite {
            
            let new_yaml = serde_yaml::to_string(&settings)
                .map_err(|e| Error::new(ErrorKind::Other, format!("Serialize failed: {}", e)))?;
            
            fs::write(&path, new_yaml)
                .map_err(|e| Error::new(e.kind(), format!("Failed to rewrite {}: {}", SETTINGS_FILE, e)))?;
            
        }
        self.settings = settings;
        
        Ok(())
    }
    
    fn get_container_headers(&self, container_name: &str) -> Result<((Vec<String>, Vec<AlbaTypes>),u64), Error> {
        let path = format!("{}/{}", self.location, container_name);
        let exists = fs::exists(&path)?;
        
        if exists {
            let file = fs::File::open(&path)?;
            let mut num_buffer = [0u8;8];
            file.read_exact_at(&mut num_buffer, 0)?;
            let header_size = u64::from_be_bytes(num_buffer);
            let mut buffer = vec![0u8;header_size as usize];
            file.read_exact_at(&mut buffer, 8)?;

            let mut read = 0;
            let mut column_names = Vec::new();
            let mut column_values = Vec::new();
            while read < buffer.len(){
                let mut cnb = [0u8;2];
                let mut atb = [0u8;1];
                cnb.copy_from_slice(&buffer[read..(read+2)]);
                read += 2;
                atb.copy_from_slice(&buffer[read..(read+1)]);
                read += 1;

                let column_name_size = u16::from_be_bytes(cnb);
                let alba_type_id = u8::from_be_bytes(atb);
                let column_name = match String::from_utf8(buffer[..(column_name_size as usize)].to_vec()){
                    Ok(a) => a.to_string(),
                    Err(e) => {return Err(gerr(&e.to_string()))}
                };
                read += column_name_size as usize;
                let alba_type = AlbaTypes::from_id(alba_type_id)?;
                column_names.push(column_name);
                column_values.push(alba_type);
            }
            return Ok(((column_names,column_values),header_size+8))
        }
        
        Err(gerr("Container not found"))
    }
    
    pub async fn run(&mut self, ast: AST) -> Result<Query, Error> {
        let min_column: usize = (self.settings.min_columns as usize).max(1);
        let max_columns: usize = self.settings.max_columns as usize;
        
        match ast {
            AST::CreateContainer(structure) => {
                if structure.name.len() > 60{
                    return Err(gerr(&format!("Failed to create container, the maximum length of a container name is 60, the entered is {}",structure.name.len())))
                }
                if structure.col_nam.len() != structure.col_val.len(){
                    return Err(gerr("Failed to create container, the count of names does not match to the count of values"))
                }
                if structure.col_val.len() == 0{
                    return Err(gerr(&format!("Failed to create container, it must have at least {}",min_column)))
                }
                if structure.col_val.len() > max_columns{
                    return Err(gerr("Failed to create container, the count of columns are higher than the maximum set on the settings file."));
                }
                let path = format!("{}/{}",self.location,structure.name);
                if self.container.get(&structure.name).is_some() || fs::exists(&path).unwrap(){
                    return Err(gerr("Failed to create container, there is already a container with this name or a file with this name on the container directory."))
                }
                let mut file = fs::File::create_new(&path).unwrap();
                let mut buffer : Vec<u8> = Vec::new();
                for i in structure.col_nam.iter().zip(structure.col_val.iter()){
                    let n = i.0.as_bytes();
                    let m = i.1.get_id().to_be_bytes();
                    if n.len() > u16::MAX as usize || m.len() > u16::MAX as usize{
                        return Err(gerr(&format!("The maximum size in bytes of the column name is {}, and the current size is {}",u16::MAX,n.len())))
                    }


                    let column_name_size : u16 = n.len() as u16;
                    let mut curr = Vec::new();
                    curr.extend_from_slice(&column_name_size.to_be_bytes());
                    curr.extend_from_slice(&m);
                    curr.extend_from_slice(&n);
                    buffer.extend_from_slice(&curr);
                }
                let header_size : u64 = buffer.len() as u64;
                let mut buff = header_size.to_be_bytes().to_vec();
                buff.extend_from_slice(&buffer); 
                file.write_all(&buff).unwrap();
                self.containers.push(structure.name.clone());
                let mut el : usize = 0;
                for i in structure.col_val.iter(){
                    el += i.size()
                }
                let c = Container::new(structure.name.clone(),
                    &path,
                    database_path(),
                    el,
                    structure.col_val.clone(), 
                    MAX_STR_LEN,
                    header_size + 8,
                    structure.col_nam.clone()
                ).await.unwrap();
                self.container.insert(structure.name, c);
                self.save_containers().unwrap();
            },
            AST::CreateRow(structure) => {
                
                let mut container = match self.container.get_mut(&structure.container) {
                    None => {
                        
                        return Err(gerr(&format!("Container '{}' does not exist.", structure.container)));
                    },
                    Some(a) => a.lock().await,
                };
                
                if structure.col_nam.len() != structure.col_val.len() {
                    
                    return Err(gerr(&format!(
                        "In CREATE ROW, expected {} values for the specified columns, but got {}",
                        structure.col_nam.len(),
                        structure.col_val.len()
                    )));
                }
                let mut val : Vec<AlbaTypes> = container.columns();
                let cols = container.columns();
                let mut hm = AHashMap::new();
                for i in container.headers.iter().enumerate(){
                    hm.insert(i.1.0.clone(),i.0);
                }

                for i in structure.col_nam.iter().enumerate(){
                    let j = hm.get(i.1);
                    if let Some(a) = j{
                        val.insert(*a,structure.col_val[i.0].clone().try_from_existing(cols[*a].clone())?);
                    }
                }


                container.push_row(&val).await?;
                if self.settings.auto_commit {
                    
                    container.commit().await?;
                }
                
            },
            AST::Search(structure) => {
                let mut query : Option<Query> = None;
                for i in structure.container{
                    
                    if let AlbaContainer::Virtual(i) = i{
                        let result_query = Box::pin(self.run(AST::Search(i))).await?;
                            if let Some(ref mut q) = query{
                                q.join(result_query);
                            }else{
                                query = Some(result_query)
                            }
                        
                        continue;
                    }
                    
                    if let AlbaContainer::Real(container_name) = i{
                        let container = match self.container.get(&container_name){
                            Some(a) => a,
                            None => {return Err(gerr(&format!("Failed to perform the query, there is no container named {}",container_name)))}
                        };
                        let container_book = container.lock().await;
                        let header_types = container_book.headers.clone();

                        
                        let mut headers_hash_map = HashMap::new();
                        for i in header_types.iter().cloned(){
                            headers_hash_map.insert(i.0,i.1);
                        }
                        let qc = QueryConditions::from_primitive_conditions( structure.conditions.clone(), &headers_hash_map,if let Some(a) = header_types.first(){a.0.clone()}else{return Err(gerr("Error, no primary key found"))}).unwrap();
                        let qt = qc.query_type().unwrap();
                        let element_size = container_book.element_size.clone();
                        let headers_offset = container_book.headers_offset.clone();
                        let file = container_book.file.clone();
                        let indexing = container_book.indexing.clone();
                        drop(container_book);
                        let result = match qt{
                            QueryType::Scan => { 
                                
                                let r = search(container.to_owned(), SearchArguments{
                                    element_size,
                                    header_offset: headers_offset as usize,
                                    file,
                                    container_values: header_types,
                                    conditions: qc,
                                }).await.unwrap();
                                
                                r
                            },
                            QueryType::Indexed(query_index_type) => {

                                
                                let values = match query_index_type{
                                    crate::query_conditions::QueryIndexType::Strict(t) => indexing.search(t).await,
                                    crate::query_conditions::QueryIndexType::Range(t) => indexing.search(t).await,
                                    crate::query_conditions::QueryIndexType::InclusiveRange(t) => indexing.search(t).await,
                                }?;
                                loginfo!("values: {:?}",values);
                                let r = indexed_search(container.to_owned(), SearchArguments{
                                    element_size,
                                    header_offset: headers_offset as usize,
                                    file,
                                    container_values: header_types,
                                    conditions: qc,
                                },&values).await.unwrap();

                                
                                r
                            }
                        };

                        
                        match query{
                            Some(ref mut b) => b.join(result),
                            None => {query = Some(result)}
                        }
                    };
                }
                if let Some(q) = query{
                    return Ok(q)
                }else{
                    return Err(gerr("Error, no query result found"))
                }
            },
            AST::EditRow(structure) => {
                
                let container = match self.container.get(&structure.container) {
                    Some(a) => {
                        
                        a
                    }
                    None => {
                        logerr!("No container named: {}", structure.container);
                        return Err(gerr(&format!("Failed to perform the query, there is no container named {}", structure.container)))
                    }
                };
            
                
                let header_types = {
                    let container_book = container.lock().await;
                    
                    container_book.headers.clone()
                };
                
            
                
                let mut headers_hash_map = HashMap::new();
                for i in header_types.iter().cloned() {
                    headers_hash_map.insert(i.0, i.1);
                }
                
            
                
                
                let qc = QueryConditions::from_primitive_conditions(
                    structure.conditions.clone(),
                    &headers_hash_map,
                    if let Some(a) = header_types.first() {
                        a.0.clone()
                    } else {
                        logerr!("No primary key found");
                        return Err(gerr("Error, no primary key found"))
                    }
                ).unwrap();
            
                
                let container_book = container.lock().await;
                
                let qt = qc.query_type().unwrap();
            
                
                let mut column_name_idx: AHashMap<String, usize> = AHashMap::new();
                let mut changes: AHashMap<usize, AlbaTypes> = AHashMap::new();
                for i in container_book.headers.iter().enumerate() {
                    column_name_idx.insert(i.1.0.clone(), i.0);
                }
                for i in structure.col_nam.iter().enumerate() {
                    let val = if let Some(v) = structure.col_val.get(i.0) {
                        v
                    } else {
                        logerr!("Missing value for column: {}", i.1);
                        return Err(gerr("Failed to execute edit because there is a value missing for one of the columns entered"))
                    };
                    let id = column_name_idx.get(i.1).unwrap();
                    changes.insert(*id, val.to_owned());
                }
                
            
                let element_size = container_book.element_size.clone();
                let headers_offset = container_book.headers_offset.clone();
                let file = container_book.file.clone();
                let indexing = container_book.indexing.clone();
                
                drop(container_book);
                
            
                let result: Vec<(Vec<AlbaTypes>, u64)> = {
                    match qt {
                        QueryType::Scan => {
                            
                            search_direct(container.clone(), SearchArguments {
                                element_size,
                                header_offset: headers_offset as usize,
                                file,
                                container_values: header_types,
                                conditions: qc,
                            }).await.unwrap()
                        }
                        QueryType::Indexed(query_index_type) => {
                            
                            let values = match query_index_type {
                                crate::query_conditions::QueryIndexType::Strict(t) => {
                                    
                                    indexing.search(t).await
                                }
                                crate::query_conditions::QueryIndexType::Range(t) => {
                                    
                                    indexing.search(t).await
                                }
                                crate::query_conditions::QueryIndexType::InclusiveRange(t) => {
                                    
                                    indexing.search(t).await
                                }
                            }.unwrap();
                            
                            indexed_search_direct(container.clone(), SearchArguments {
                                element_size,
                                header_offset: headers_offset as usize,
                                file,
                                container_values: header_types,
                                conditions: qc,
                            }, &values).await.unwrap()
                        }
                    }.iter_mut().map(|f| {
                        
                        for (index, new_value) in &changes {
                            f.0[*index] = new_value.clone();
                        }
                        f.to_owned()
                    }).collect()
                };
                
            
                
                let container_book = container.lock().await;
                
                let mvcc = container_book.mvcc.clone();
                drop(container_book);
                
            
                
                let mut mvcc = mvcc.lock().await;
                
                
                for i in result {
                    let ind = i.1.saturating_sub(headers_offset).saturating_div(element_size as u64);
                    
                    
                    mvcc.0.insert(ind, (false, i.0));
                }
            
                
            },
            AST::DeleteRow(structure) => {
                
                let container = match self.container.get(&structure.container){
                    Some(a) => a,
                    None => {return Err(gerr(&format!("Failed to perform the query, there is no container named {}",structure.container)))}
                };
                let header_types = container.lock().await.headers.clone();

                let mut headers_hash_map = HashMap::new();
                for i in header_types.iter().cloned(){
                    headers_hash_map.insert(i.0,i.1);
                }
                
                let qc = QueryConditions::from_primitive_conditions( if let Some(c) = structure.conditions{c}else{(Vec::new(),Vec::new())}, &headers_hash_map,if let Some(a) = header_types.first(){a.0.clone()}else{return Err(gerr("Error, no primary key found"))})?;
                let container_book = container.lock().await;
                let element_size = container_book.element_size.clone();
                let headers_offset = container_book.headers_offset.clone();
                let file = container_book.file.clone();
                let indexing = container_book.indexing.clone();
                let qt = qc.query_type()?;
                
                drop(container_book);
                let result : Vec<(Vec<AlbaTypes>,u64)> = match qt{
                    QueryType::Scan => { 
                        
                        let r = search_direct(container.clone(), SearchArguments{
                            element_size,
                            header_offset: headers_offset as usize,
                            file,
                            container_values: header_types,
                            conditions: qc,
                        }).await?;
                        
                        
                        r
                    }
                    QueryType::Indexed(query_index_type) => {

                        
                        let values = match query_index_type{
                            crate::query_conditions::QueryIndexType::Strict(t) => indexing.search(t).await,
                            crate::query_conditions::QueryIndexType::Range(t) => indexing.search(t).await,
                            crate::query_conditions::QueryIndexType::InclusiveRange(t) => indexing.search(t).await,
                        }?;

                        
                        let r= indexed_search_direct(container.clone(), SearchArguments{
                            element_size,
                            header_offset: headers_offset as usize,
                            file,
                            container_values: header_types,
                            conditions: qc,
                        },&values).await?;

                        
                        r
                    }
                };
                
                let container_book = container.lock().await;
                let mut mvcc = container_book.mvcc.lock().await;
                for i in result{
                    let k = (i.1-headers_offset)/element_size as u64;
                    
                    mvcc.0.insert(k, (true,i.0));
                }

                
            },
            AST::DeleteContainer(structure) => {
                
                if self.containers.contains(&structure.container) {
                    let mut ind = Vec::new();
                    for (i, name) in self.containers.iter().enumerate() {
                        if structure.container == *name {
                            ind.push(i);
                            
                        }
                    }
                    for i in ind {
                        self.containers.remove(i);
                        
                    }
                    self.container.remove(&structure.container);
                    
                    let path = format!("{}/{}", self.location, structure.container);
                    let _ = tokio::fs::remove_file(path.clone()).await;
                    let path = format!("{}/{}.index", self.location, structure.container);
                    let _ = tokio::fs::remove_file(path.clone()).await;
                    
                    self.save_containers()?;
                    
                } else {
                    
                    return Err(gerr(&format!("There is no database with the name {}", structure.container)));
                }
            },
            AST::Commit(structure) => {
                
                match structure.container {
                    Some(container) => {
                        match self.container.get_mut(&container) {
                            Some(a) => {
                                
                                a.lock().await.commit().await.unwrap();
                                
                                return Ok(Query::new(Vec::new()));
                            },
                            None => {
                                
                                return Err(gerr(&format!("There is no container named {}", container)));
                            }
                        }
                    },
                    None => {
                        
                        self.commit().await?;
                        
                    }
                }
            },
            AST::Rollback(structure) => {
                
                match structure.container {
                    Some(container) => {
                        match self.container.get_mut(&container) {
                            Some(a) => {
                                
                                a.lock().await.rollback().await?;
                                
                                return Ok(Query::new(Vec::new()));
                            },
                            None => {
                                
                                return Err(gerr(&format!("There is no container named {}", container)));
                            }
                        }
                    },
                    None => {
                        
                        self.rollback().await?;
                        
                    }
                }
            }
        }
        
        Ok(Query::new_none(Vec::new()))
    }
    
    // pub async fn execute(&mut self, input: &str, arguments: Vec<String>) -> Result<Query, Error> {
    //     let ast = parse(input.to_owned(), arguments)?;
    //     let result = self.run(ast).await?;
    //     Ok(result)
    // }
}

pub async fn connect() -> Result<Database, Error>{
    let dbp = database_path();
    let path : &str = if dbp.ends_with('/') {
        &dbp[..dbp.len()-1]
    }else{
        &dbp
    };

    let db_path = PathBuf::from(path);
    if db_path.exists() {
        if !db_path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("`{}` exists but is not a directory", path),
            ));
        }
    } else {
        fs::create_dir_all(&db_path)?;
    }

    // if let Some(strix) = STRIX.get(){
    //     start_strix(strix.clone()).await;
    // }

    let mut db = Database{location:database_path().to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new()};
    db.setup().await?;
    if let Err(e) = db.load_settings(){
        logerr!("err: load_settings");
        return Err(e)
    };if let Err(e) = db.load_containers().await{
        logerr!("err: load_containers");
        return Err(e)
    };
    //
    return Ok(db)
}


use tytodb_conn::{commands::{AlbaContainer as NetworkAlbaContainer, Commands as commands, Compile, CreateContainer}, db_response::DBResponse, logical_operators::LogicalOperator};
use tytodb_conn::types::AlbaTypes as NetworkAlbaTypes;

fn ab_from_nat(a : NetworkAlbaTypes) -> AlbaTypes{
    match a{
        NetworkAlbaTypes::String(a) => AlbaTypes::LargeString(a),
        NetworkAlbaTypes::U8(a) => AlbaTypes::Int(a as i32),
        NetworkAlbaTypes::U16(a) => AlbaTypes::Int(a as i32),
        NetworkAlbaTypes::U32(a) => AlbaTypes::Bigint(a as i64),
        NetworkAlbaTypes::U64(a) => AlbaTypes::Bigint(a as i64),
        NetworkAlbaTypes::U128(a) => AlbaTypes::Bigint(a as i64),
        NetworkAlbaTypes::F32(a) => AlbaTypes::Float(a as f64),
        NetworkAlbaTypes::F64(a) => AlbaTypes::Float(a as f64),
        NetworkAlbaTypes::Bool(a) => AlbaTypes::Bool(a as bool),
        NetworkAlbaTypes::I32(a) => AlbaTypes::Int(a as i32),
        NetworkAlbaTypes::I64(a) => AlbaTypes::Bigint(a as i64),
        NetworkAlbaTypes::Bytes(items) => AlbaTypes::LargeBytes(items),
    }
}


impl Query {
    fn to_row_list(&self) -> Vec<tytodb_conn::db_response::Row> {
        let mut a: Vec<tytodb_conn::db_response::Row> = Vec::new();
        for i in self.rows.1.iter() {
            let row_data: Vec<NetworkAlbaTypes> = i.iter().map(|a| {
                match a {
                    AlbaTypes::Text(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::Int(i) => NetworkAlbaTypes::I32(*i),
                    AlbaTypes::Bigint(i) => NetworkAlbaTypes::I64(*i),
                    AlbaTypes::Float(f) => NetworkAlbaTypes::F64(*f),
                    AlbaTypes::Bool(b) => NetworkAlbaTypes::Bool(*b),
                    AlbaTypes::Char(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::NanoString(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::SmallString(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::MediumString(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::BigString(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::LargeString(s) => NetworkAlbaTypes::String(s.to_string()),
                    AlbaTypes::NanoBytes(items) => NetworkAlbaTypes::Bytes(items.clone()),
                    AlbaTypes::SmallBytes(items) => NetworkAlbaTypes::Bytes(items.clone()),
                    AlbaTypes::MediumBytes(items) => NetworkAlbaTypes::Bytes(items.clone()),
                    AlbaTypes::BigSBytes(items) => NetworkAlbaTypes::Bytes(items.clone()),
                    AlbaTypes::LargeBytes(items) => NetworkAlbaTypes::Bytes(items.clone()),
                    AlbaTypes::NONE => NetworkAlbaTypes::U8(0),
                }
            }).collect();
            
            a.push(tytodb_conn::db_response::Row::new(row_data)); // or however Row is constructed
        }
        a
    }
}


fn row_list_to_bytes(a : Vec<tytodb_conn::db_response::Row>) -> Vec<u8>{
    let mut b = Vec::new(); 
    for i in a{
        b.extend_from_slice(&i.encode());
    }
    b
}

fn boop(q : Query) -> Vec<u8>{
    let mut byte = vec![0u8];
    byte.extend_from_slice(&row_list_to_bytes(q.to_row_list()));
    return byte
}
fn alba_types_to_token(alba_type: AlbaTypes) -> Token {
    match alba_type {
        AlbaTypes::Text(s) => Token::String(s),
        AlbaTypes::Int(i) => Token::Int(i as i64),
        AlbaTypes::Bigint(i) => Token::Int(i),
        AlbaTypes::Float(f) => Token::Float(f),
        AlbaTypes::Bool(b) => Token::Bool(b),
        AlbaTypes::Char(s) => Token::String(s.to_string()),
        AlbaTypes::NanoString(s) => Token::String(s),
        AlbaTypes::SmallString(s) => Token::String(s),
        AlbaTypes::MediumString(s) => Token::String(s),
        AlbaTypes::BigString(s) => Token::String(s),
        AlbaTypes::LargeString(s) => Token::String(s),
        AlbaTypes::NanoBytes(items) => Token::Bytes(items),
        AlbaTypes::SmallBytes(items) => Token::Bytes(items),
        AlbaTypes::MediumBytes(items) => Token::Bytes(items),
        AlbaTypes::BigSBytes(items) => Token::Bytes(items),
        AlbaTypes::LargeBytes(items) => Token::Bytes(items),
        AlbaTypes::NONE => Token::Int(0),
    }
}
fn conditions_to_tyto_db(t: (Vec<(String, LogicalOperator, NetworkAlbaTypes)>, Vec<(usize, char)>)) -> (Vec<(Token, Token, Token)>, Vec<(usize, char)>) {
    (t.0.iter().map(|f| {
        (
            Token::String(f.0.clone()),
            Token::Operator(match f.1 {
                LogicalOperator::Equal => "=".to_string(),
                LogicalOperator::Diferent => "!=".to_string(),
                LogicalOperator::Higher => ">".to_string(),
                LogicalOperator::Lower => "<".to_string(),
                LogicalOperator::HigherEquality => ">=".to_string(),
                LogicalOperator::LowerEquality => "<=".to_string(),
                LogicalOperator::StringContains => "&>".to_string(),
                LogicalOperator::StringContainsInsensitive => "&&>".to_string(),
                LogicalOperator::StringRegex => "&&&>".to_string(),
            }),
            (alba_types_to_token(ab_from_nat(f.2.clone()))) // Convert AlbaTypes to Token
        )
    }).collect(), t.1)
}

fn build_ast_search_from_search(search : tytodb_conn::commands::Search) -> AstSearch{
    AstSearch{            
        col_nam: search.col_nam,
        container: {
            let mut a = Vec::new();
            for f in search.container{
                a.push(match f {
                    NetworkAlbaContainer::Real(a) => AlbaContainer::Real(a),
                    NetworkAlbaContainer::Virtual(searchs) => AlbaContainer::Virtual(build_ast_search_from_search(searchs)),
                });
            }
            a
        },
        conditions: conditions_to_tyto_db(search.conditions)
    }
}
use falcotcp::Server;
impl Database{
    pub async fn run_database(self) -> Result<(), Error>{
        let mut password : [u8;32] = [0u8;32];
        if fs::exists(secret_key_path()).unwrap(){
            let mut buffer : Vec<u8> = Vec::new();
            fs::File::open(secret_key_path()).unwrap().read_to_end(&mut buffer)?;
            password[0..].copy_from_slice(&buffer);
            // let bv : Vec<Vec<u8>> = val.iter().map(|s|{
            //     match eng.decode(s){
            //         Ok(a)=>a,
            //         Err(e)=>{
            //             logerr!("{}",e);
            //         }
            //     }
            // }).collect();
        }else{
            let mut file = fs::File::create_new(secret_key_path()).unwrap();
            let mut bytes: [u8; 32] = [0u8;32];
            let mut osr = OsRng;
            for i in 0..4usize{
                let b : [u8;8] = OsRng::next_u64(&mut osr).to_le_bytes();
                bytes[i*8..(i+1)*8].copy_from_slice(&b);

            }
            let _ = file.write_all(&bytes);
            file.flush()?;
            file.sync_all()?;
            password = bytes;
        }
        let host = format!("{}:{}",self.settings.ip.clone(),self.settings.port.clone());
        let workers = self.settings.workers.clone() as usize;
        let mtx_db: &'static Arc<Mutex<Database>> = Box::leak(Box::new(Arc::new(Mutex::new(self))));


        let message_handler: Arc<(dyn Fn(Vec<u8>) -> Pin<Box<(dyn futures::Future<Output = Vec<u8>> + std::marker::Send + 'static)>> + std::marker::Send + Sync + 'static)> = Arc::new(move |input: Vec<u8>| { Box::pin(async move {
            println!("bytes tytodb received: {:?}",input);
            match commands::decompile(&match zstd::bulk::decompress(&input,input.len()){
                Ok(a) => a,
                Err(e) => {
                    let mut b = vec![1u8];
                    b.extend_from_slice(&e.to_string().as_bytes());
                    return b
                }
            }){
                Ok(a) => {
                    match a{
                        commands::CreateContainer(create_container) => {
                            let mut col_v = Vec::new();
                            for f in create_container.col_val{
                                match AlbaTypes::from_id(f){
                                    Ok(a) => {
                                        col_v.push(a);
                                    },
                                    Err(e) => {
                                        let mut b = vec![1u8];
                                        b.extend_from_slice(&e.to_string().as_bytes());
                                        return b
                                    }
                                }
                            }
                            let mut db = mtx_db.lock().await;
                            let c =  db.run(AST::CreateContainer(crate::AstCreateContainer {
                                name: create_container.name,
                                col_nam: create_container.col_nam,
                                col_val: col_v
                            })).await;
                            match c {
                                Ok(q) => {
                                    return boop(q)
                                }
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::CreateRow(create_row) => {
                            match mtx_db.lock().await.run(AST::CreateRow(AstCreateRow{
                                col_nam: create_row.col_nam,
                                col_val: create_row.col_val.iter().map(|f|{ab_from_nat(f.clone())}).collect(),
                                container: create_row.container
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::EditRow(edit_row) => {
                            match mtx_db.lock().await.run(AST::EditRow(AstEditRow{
                                col_nam: edit_row.col_nam,
                                col_val: edit_row.col_val.iter().map(|f|{ab_from_nat(f.clone())}).collect(),
                                container: edit_row.container,
                                conditions: conditions_to_tyto_db(edit_row.conditions)
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::DeleteRow(delete_row) => {
                            match mtx_db.lock().await.run(AST::DeleteRow(AstDeleteRow{
                                container: delete_row.container,
                                conditions: if let Some(s) = delete_row.conditions{Some(conditions_to_tyto_db(s))}else{None}
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::DeleteContainer(delete_container) => {
                            match mtx_db.lock().await.run(AST::DeleteContainer(AstDeleteContainer{
                                container: delete_container.container,
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::Search(search) => {
                            let mtx_db = &mtx_db;
                            async fn a(search: tytodb_conn::commands::Search,mtx_db: &'static Arc<Mutex<Database>>) -> Vec<u8>{
                                match mtx_db.lock().await.run(AST::Search(AstSearch{
                                
                                    col_nam: search.col_nam,
                                    container: {
                                        let mut a = Vec::new();
                                        for f in search.container{
                                            a.push(match f {
                                                NetworkAlbaContainer::Real(a) => AlbaContainer::Real(a),
                                                NetworkAlbaContainer::Virtual(searchs) => AlbaContainer::Virtual(build_ast_search_from_search(searchs)),
                                            })
                                        }
                                        a
                                    },
                                    conditions: conditions_to_tyto_db(search.conditions)
                                })).await{
                                    Ok(a) => {boop(a)},
                                    Err(e) => {
                                        let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                        b.extend_from_slice(&e.to_string().as_bytes());
                                        return b
                                    }
                                }
                            }
                            a(search,&mtx_db).await
                        },
                        commands::Commit(commit) => {
                            match mtx_db.lock().await.run(AST::Commit(AstCommit{
                                container: commit.container
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::Rollback(rollback) => {
                            match mtx_db.lock().await.run(AST::Rollback(AstRollback{
                                container: rollback.container,
                            })).await{
                                Ok(a) => return boop(a),
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                    }
                },
                Err(e) => {
                    let mut b = vec![1u8];
                    b.extend_from_slice(e.to_string().as_bytes());
                    return b
                }
            };
            return vec![1u8,115, 111, 109, 101, 116, 104, 105, 110, 103, 32, 117, 110, 101, 120, 112, 101, 99, 116, 101,100, 32, 104, 97, 112, 112, 101, 110, 101, 100]
        })});
        Server::new(host, password, message_handler, workers).await
    }
}