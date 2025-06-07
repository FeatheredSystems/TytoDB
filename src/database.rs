use std::{collections::HashMap, fs, io::{Error, ErrorKind, Read, Write}, os::unix::fs::FileExt, path::PathBuf, str::FromStr, sync::Arc};
use ahash::AHashMap;
use base64::{alphabet, engine::{self, GeneralPurpose}, Engine};
use lazy_static::lazy_static;
use serde::{Serialize,Deserialize};
use serde_yaml;
use crate::{alba_types::AlbaTypes, container::Container, gerr, indexing::Search, logerr, loginfo, parser::{debug_tokens, parse}, query::{indexed_search, indexed_search_direct, search, search_direct, Query, SearchArguments}, query_conditions::{QueryConditions, QueryType}, AlbaContainer, AST};
use rand::{Rng, distributions::Alphanumeric};
use tokio::{net::TcpListener, sync::Mutex};
/////////////////////////////////////////////////
/////////     DEFAULT_SETTINGS    ///////////////
/////////////////////////////////////////////////

pub const MAX_STR_LEN : usize = 128;
const DEFAULT_SETTINGS : &str = r#"
max_columns: 50
min_columns: 1
auto_commit: false            
memory_limit: 1048576000
ip: 127.0.0.1
connections_port: 1515
data_port: 5000
max_connections: 10
max_connection_requests_per_minute: 10
max_data_requests_per_minute: 10000000
on_insecure_rejection_delay_ms: 100
safety_level: strict # strict | permissive
request_handling: sync # sync | asynchronous
secret_key_count: 10
"#;
#[derive(Serialize, Deserialize, Debug, Default)]
enum SafetyLevel {
    #[default]
    #[serde(rename="strict")]
    Strict,
    #[serde(rename="permissive")]
    Permissive,
}

#[derive(Serialize, Deserialize, Debug, Default)]
enum RequestHandling {
    #[default]
    #[serde(rename="sync")]
    Sync,
    #[serde(rename="asynchronous")]
    Asynchronous,
}
#[derive(Serialize,Deserialize, Default,Debug)]
struct Settings{
    max_columns : u32,
    min_columns : u32,
    memory_limit : u64,
    auto_commit : bool,
    ip:String,
    connections_port: u32,
    data_port: u32,
    max_connections: u32,
    max_connection_requests_per_minute: u32,
    max_data_requests_per_minute: u32,
    on_insecure_rejection_delay_ms: u64,
    safety_level: SafetyLevel,
    request_handling: RequestHandling,
    secret_key_count: u64
}

const SECRET_KEY_PATH : &str = "TytoDB/.tytodb-keys";
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
    secret_keys : Arc<Mutex<HashMap<[u8;32],Vec<u8>>>>,
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
                for i in &structure.col_nam {
                    if !container.column_names().contains(&i) {
                        
                        return Err(gerr(&format!("There is no column {} in the container {}", i, structure.container)));
                    }
                }
                let mut val: Vec<AlbaTypes> = container.columns();
                
                for (index, col_name) in structure.col_nam.iter().enumerate() {
                    match container.column_names().iter().position(|c| c == col_name) {
                        Some(ri) => {
                            let input_val = structure.col_val.get(index).cloned().unwrap();
                            let expected_val = &container.columns()[ri];
                            
                            if let AlbaTypes::NONE = &input_val {
                                val[ri] = AlbaTypes::NONE;
                                
                            } else if let AlbaTypes::Text(_) = expected_val {
                                if let AlbaTypes::Text(s) = &input_val {
                                    let mut code = generate_secure_code(MAX_STR_LEN);
                                    let txt_path = format!("{}/rf/{}", self.location, code);
                                    
                                    while fs::exists(&txt_path)? {
                                        let code_full = generate_secure_code(MAX_STR_LEN);
                                        code = code_full.chars().take(MAX_STR_LEN).collect::<String>();
                                        
                                    }
                                    val[ri] = AlbaTypes::Text(code.clone());
                                    let mut mvcc = container.mvcc.lock().await;
                                    loginfo!("code: {:?}",code);
                                    mvcc.1.insert(code, (false, s.to_string()));
                                } else {
                                    
                                    return Err(gerr(&format!(
                                        "For column '{}', expected Text, but got {:?}.",
                                        col_name, input_val
                                    )));
                                }
                            } else if std::mem::discriminant(&input_val) == std::mem::discriminant(expected_val) {
                                val[ri] = input_val;
                                
                            } else {
                                match expected_val.try_from_existing(input_val.clone()) {
                                    Ok(converted_val) => {
                                        val[ri] = converted_val;
                                        
                                    },
                                    Err(e) => {
                                        
                                        return Err(gerr(&format!(
                                            "Type conversion error for column '{}': expected {:?}, got {:?}. Error: {}",
                                            col_name, expected_val, input_val, e
                                        )));
                                    }
                                }
                            }
                        },
                        None => {
                            
                            return Err(gerr(&format!(
                                "Column '{}' not found in container '{}'.",
                                col_name, structure.container
                            )));
                        }
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
                    loginfo!("search-row:p1");
                    if let AlbaContainer::Virtual(virt) = i{
                        let ast = debug_tokens(&virt)?;
                        let result_query = Box::pin(self.run(ast)).await?;
                        if let Some(ref mut q) = query{
                            q.join(result_query);
                        }else{
                            query = Some(result_query)
                        }
                        continue;
                    }
                    loginfo!("search-row:p2");
                    if let AlbaContainer::Real(container_name) = i{
                        let container = match self.container.get(&container_name){
                            Some(a) => a,
                            None => {return Err(gerr(&format!("Failed to perform the query, there is no container named {}",container_name)))}
                        };
                        let container_book = container.lock().await;
                        let header_types = container_book.headers.clone();

                        loginfo!("search-row:p3");
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
                                loginfo!("search-row:p3.1-");
                                let r = search(container.to_owned(), SearchArguments{
                                    element_size,
                                    header_offset: headers_offset as usize,
                                    file,
                                    container_values: header_types,
                                    conditions: qc,
                                }).await.unwrap();
                                loginfo!("search-row:p3.1");
                                r
                            },
                            QueryType::Indexed(query_index_type) => {

                                loginfo!("search-row:p3.2-");
                                let values = match query_index_type{
                                    crate::query_conditions::QueryIndexType::Strict(t) => indexing.search(t).await,
                                    crate::query_conditions::QueryIndexType::Range(t) => indexing.search(t).await,
                                    crate::query_conditions::QueryIndexType::InclusiveRange(t) => indexing.search(t).await,
                                }?;
                                loginfo!("{:?}",values);
                                let r = indexed_search(container.to_owned(), SearchArguments{
                                    element_size,
                                    header_offset: headers_offset as usize,
                                    file,
                                    container_values: header_types,
                                    conditions: qc,
                                },&values).await.unwrap();

                                loginfo!("search-row:p3.2");
                                r
                            }
                        };

                        loginfo!("search-row:p4");
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
                loginfo!("Starting EditRow | container: {}", structure.container);
                let container = match self.container.get(&structure.container) {
                    Some(a) => {
                        loginfo!("Found container: {}", structure.container);
                        a
                    }
                    None => {
                        logerr!("No container named: {}", structure.container);
                        return Err(gerr(&format!("Failed to perform the query, there is no container named {}", structure.container)))
                    }
                };
            
                loginfo!("Locking container for headers...");
                let header_types = {
                    let container_book = container.lock().await;
                    loginfo!("Container locked for headers");
                    container_book.headers.clone()
                };
                loginfo!("Retrieved {} headers", header_types.len());
            
                loginfo!("Building headers hash map");
                let mut headers_hash_map = HashMap::new();
                for i in header_types.iter().cloned() {
                    headers_hash_map.insert(i.0, i.1);
                }
                loginfo!("Headers hash map built with {} entries", headers_hash_map.len());
            
                loginfo!("Creating QueryConditions");
                loginfo!("Primitive conditions: {:?}",structure.conditions);
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
            
                loginfo!("Locking container for query execution...");
                let container_book = container.lock().await;
                loginfo!("Container locked for query execution");
                let qt = qc.query_type().unwrap();
            
                loginfo!("Building column name index and changes map");
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
                loginfo!("Column name index built with {} entries, changes map with {} entries", column_name_idx.len(), changes.len());
            
                let element_size = container_book.element_size.clone();
                let headers_offset = container_book.headers_offset.clone();
                let file = container_book.file.clone();
                let indexing = container_book.indexing.clone();
                loginfo!("Preparing search | element_size: {}, headers_offset: {}", element_size, headers_offset);
                drop(container_book);
                loginfo!("Container lock dropped");
            
                let result: Vec<(Vec<AlbaTypes>, u64)> = {
                    match qt {
                        QueryType::Scan => {
                            loginfo!("Performing search_direct");
                            search_direct(container.clone(), SearchArguments {
                                element_size,
                                header_offset: headers_offset as usize,
                                file,
                                container_values: header_types,
                                conditions: qc,
                            }).await.unwrap()
                        }
                        QueryType::Indexed(query_index_type) => {
                            loginfo!("Performing indexed_search_direct");
                            let values = match query_index_type {
                                crate::query_conditions::QueryIndexType::Strict(t) => {
                                    loginfo!("Searching index: Strict({:?})", t);
                                    indexing.search(t).await
                                }
                                crate::query_conditions::QueryIndexType::Range(t) => {
                                    loginfo!("Searching index: Range({:?})", t);
                                    indexing.search(t).await
                                }
                                crate::query_conditions::QueryIndexType::InclusiveRange(t) => {
                                    loginfo!("Searching index: InclusiveRange({:?})", t);
                                    indexing.search(t).await
                                }
                            }.unwrap();
                            loginfo!("Index search returned {} values", values.len());
                            indexed_search_direct(container.clone(), SearchArguments {
                                element_size,
                                header_offset: headers_offset as usize,
                                file,
                                container_values: header_types,
                                conditions: qc,
                            }, &values).await.unwrap()
                        }
                    }.iter_mut().map(|f| {
                        loginfo!("Applying changes to row {:?}",f);
                        for (index, new_value) in &changes {
                            f.0[*index] = new_value.clone();
                        }
                        f.to_owned()
                    }).collect()
                };
                loginfo!("Search completed | result rows: {}", result.len());
            
                loginfo!("Locking container for MVCC update...");
                let container_book = container.lock().await;
                loginfo!("Container locked for MVCC update");
                let mvcc = container_book.mvcc.clone();
                drop(container_book);
                loginfo!("Container lock dropped");
            
                loginfo!("Locking MVCC...");
                let mut mvcc = mvcc.lock().await;
                loginfo!("MVCC locked");
                loginfo!("MVCC updated with {} entries", result.len());
                for i in result {
                    let ind = i.1.saturating_div(element_size as u64).saturating_add(headers_offset);
                    loginfo!("ind: {}, i.1: {}",ind,i.1);
                    loginfo!("Inserting row into MVCC | address: {}", ind);
                    mvcc.0.insert(ind, (false, i.0));
                }
            
                loginfo!("EditRow completed successfully");
            },
            AST::DeleteRow(structure) => {
                loginfo!("del-row:p1");
                let container = match self.container.get(&structure.container){
                    Some(a) => a,
                    None => {return Err(gerr(&format!("Failed to perform the query, there is no container named {}",structure.container)))}
                };
                let header_types = container.lock().await.headers.clone();

                let mut headers_hash_map = HashMap::new();
                for i in header_types.iter().cloned(){
                    headers_hash_map.insert(i.0,i.1);
                }
                loginfo!("del-row:p2");
                let qc = QueryConditions::from_primitive_conditions( if let Some(c) = structure.conditions{c}else{(Vec::new(),Vec::new())}, &headers_hash_map,if let Some(a) = header_types.first(){a.0.clone()}else{return Err(gerr("Error, no primary key found"))})?;
                let container_book = container.lock().await;
                let element_size = container_book.element_size.clone();
                let headers_offset = container_book.headers_offset.clone();
                let file = container_book.file.clone();
                let indexing = container_book.indexing.clone();
                let qt = qc.query_type()?;
                loginfo!("del-row:p2.5");
                drop(container_book);
                let result : Vec<(Vec<AlbaTypes>,u64)> = match qt{
                    QueryType::Scan => { 
                        loginfo!("del-row:p2.5.0");
                        let r = search_direct(container.clone(), SearchArguments{
                            element_size,
                            header_offset: headers_offset as usize,
                            file,
                            container_values: header_types,
                            conditions: qc,
                        }).await?;
                        
                        loginfo!("del-row:p2.5.1");
                        r
                    }
                    QueryType::Indexed(query_index_type) => {

                        loginfo!("del-row:p2.5.5");
                        let values = match query_index_type{
                            crate::query_conditions::QueryIndexType::Strict(t) => indexing.search(t).await,
                            crate::query_conditions::QueryIndexType::Range(t) => indexing.search(t).await,
                            crate::query_conditions::QueryIndexType::InclusiveRange(t) => indexing.search(t).await,
                        }?;

                        loginfo!("del-row:p2.5.6");
                        let r= indexed_search_direct(container.clone(), SearchArguments{
                            element_size,
                            header_offset: headers_offset as usize,
                            file,
                            container_values: header_types,
                            conditions: qc,
                        },&values).await?;

                        loginfo!("del-row:p2.5.7");
                        r
                    }
                };
                loginfo!("del-row:p3");
                let container_book = container.lock().await;
                let mut mvcc = container_book.mvcc.lock().await;
                for i in result{
                    let k = (i.1-headers_offset)/element_size as u64;
                    loginfo!("del-key: {:?}",k);
                    mvcc.0.insert(k, (true,i.0));
                }

                loginfo!("del-row:p4");
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
                    tokio::fs::remove_file(path.clone()).await?;
                    let path = format!("{}/{}.cindex", self.location, structure.container);
                    tokio::fs::remove_file(path.clone()).await?;
                    let path = format!("{}/{}.cimeta", self.location, structure.container);
                    tokio::fs::remove_file(path.clone()).await?;
                    
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
    
    pub async fn execute(&mut self, input: &str, arguments: Vec<String>) -> Result<Query, Error> {
        let ast = parse(input.to_owned(), arguments)?;
        let result = self.run(ast).await?;
        Ok(result)
    }
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

    let mut db = Database{location:database_path().to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new(),secret_keys:Arc::new(Mutex::new(HashMap::new()))};
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


async fn handle_connections_tcp_inner(payload : Vec<u8>,dbref: Arc<Mutex<Database>>) -> Vec<u8>{
    let mut secret_key_hash : [u8;32] = [0u8;32];
    secret_key_hash.clone_from_slice(payload.as_slice());

    let secret_key = match dbref.lock().await.secret_keys.lock().await.get(&secret_key_hash){
        Some(a) => a.clone(),
        None => {
            let buffer : [u8;1] = [false as u8];
            logerr!("the given secret key are not registred");
            return buffer.to_vec()
        }
    };

    let mut buffer : Vec<u8> = Vec::new();
    let session_id = secret_key.clone();
    let hash = blake3::hash(&session_id).as_bytes().clone();
    let key = Key::<Aes256Gcm>::from_slice(secret_key.as_slice());
    let _ = session_secret_rel.lock().await.insert(hash.clone(), session_id.clone());
    cipher_map.lock().await.insert(hash.clone(),Aes256Gcm::new(key));

    if let Ok(a) = encrypt(&session_id, &secret_key_hash).await{
        buffer.push(true as u8);
        buffer.extend_from_slice(&a);
    }else{
        buffer.push(false as u8);
    }
    

    //
    buffer
    // let _ = socket.shutdown().await;

}

// async fn handle_connections_tcp_sync(listener : &TcpListener,ardb : Arc<Mutex<Database>>){
//     let (mut socket, addr) = match listener.accept().await{
//         Ok(a) => a,
//         Err(e) => {
//             logerr!("{}",e);
//             return
//         }
//     };
//     //
//     //handle_connections_tcp_inner(&mut socket, ardb).await;
//     // if let Err(e) = socket.shutdown().await{
//     //     logerr!("{}",e);
//     // }
// }
// async fn handle_connections_tcp_parallel(listener : &TcpListener,ardb : Arc<Mutex<Database>>){
//     let (mut socket, addr) = match listener.accept().await{
//         Ok(a) => a,
//         Err(e) => {
//             logerr!("{}",e);
//             return
//         }
//     };
//     //
//     tokio::task::spawn(async move {
//         handle_connections_tcp_inner(&mut socket, ardb).await;
//     });
    
//     // if let Err(e) = socket.shutdown().await{
//     //     logerr!("{}",e);
//     // }
// }


use aes_gcm::{aead::{Aead, KeyInit, OsRng}, aes::cipher::{self}, AeadCore, Key};
use aes_gcm::Aes256Gcm;

lazy_static!{
    static ref cipher_map : Arc<Mutex<AHashMap<[u8;32],aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>>>>> = Arc::new(Mutex::new(AHashMap::new()));
    static ref session_secret_rel : Arc<Mutex<AHashMap<[u8;32],Vec<u8>>>> = Arc::new(Mutex::new(AHashMap::new())); 
}

async fn encrypt(content : &[u8],secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
    let cm = cipher_map.lock().await;
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(secret_key){
        a
    } else{
        return Err(())
    };
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); 
    let mut result : Vec<u8> = Vec::new();
    //
    result.extend_from_slice(&nonce.to_vec());
    result.extend_from_slice(&cipher.encrypt(&nonce, content.as_ref()).unwrap());
    Ok(result)
}
async fn decrypt(cipher_text : &[u8],secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
    let nonce = &cipher_text[0..12];
    let cipher_b = &cipher_text[12..];
    let cm = cipher_map.lock().await;
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(secret_key){
        a
    } else{
        return Err(())
    };
    match cipher.decrypt(nonce.into(), cipher_b.as_ref()){
        Ok(a) => Ok(a),
        Err(e) => {
            logerr!("{}",e);
            Err(())
        }
    }
}


#[derive(Deserialize)]
struct DataConnection{
    command : String,
    arguments : Vec<String>
}

#[derive(Serialize)]
struct QueryResponse{
    rows : Vec<Vec<AlbaTypes>>
}

#[derive(Serialize,Default)]
struct TytoDBResponse{
    #[serde(rename = "?")]
    content : String,
    #[serde(rename = "!")]
    success : u8
}

impl TytoDBResponse {
    async fn to_bytes(self,secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
        let bytes = serde_json::to_vec(&self).unwrap();
        return if let Ok(a) = encrypt(&bytes, secret_key).await{
            Ok(a)
        }else{
            Err(())
        };
    }
}


async fn handle_data_tcp_inner(dbref: Arc<Mutex<Database>>,rc_payload:Vec<u8>) -> Vec<u8>{
    let size = rc_payload.len();
    if size <= 0{
        logerr!("the payload is too short | size :{}",size);
        return (0 as u64).to_be_bytes().as_slice().to_vec()
    }
    //
    let mut session_id : [u8;32] = [0u8;32];
    session_id.clone_from_slice(&rc_payload[..32]);
    //


    let ssr = session_secret_rel.lock().await;
    let mut db = dbref.lock().await;
    let mut payload: Vec<u8> = Vec::with_capacity(512);
    payload.extend_from_slice(&rc_payload[32..]);
    if let Some(_) = ssr.get(&session_id) {
        //
        
        
        payload = match decrypt(&payload, &session_id).await{
            Ok(a) => a,
            Err(_) => {
                return (0 as u64).to_be_bytes().as_slice().to_vec()
            }
        };
        //
        
                 
    } else {
        logerr!("No session secret found for session_id");
        payload.clear();
        return (0 as u64).to_be_bytes().as_slice().to_vec();
    }
    //
    let mut response: Vec<u8> = Vec::with_capacity(510);
    match serde_json::from_slice::<DataConnection>(&payload) {
        Ok(v) => {
            //
            match db.execute(&v.command,v.arguments).await {
                Ok(query_result) => {
                    //
                    //
                    let l = query_result.rows.1.len();
                    let mut qr = QueryResponse{
                        rows : Vec::with_capacity(l)
                    };
                    for i in query_result.rows.1{
                        qr.rows.push(i)
                    }
                    match serde_json::to_string(&qr) {
                        
                        Ok(q) => {
                            //
                            if let Ok(b) = (TytoDBResponse{
                                content:q,
                                success:1
                            }).to_bytes(&session_id).await{
                                let size = b.len() as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                                response.extend_from_slice(&b);
                            }else{
                                let size = 0 as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                            };
                            //
                            
                        }
                        Err(e) => {
                            logerr!("Failed to serialize query result: {}", e);
                            if let Ok(b) = (TytoDBResponse{
                                content:format!("Failed to serialize query result: {}", e),
                                success:0
                            }.to_bytes(&session_id).await){
                                let size = b.len() as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                                response.extend_from_slice(&b);
                            }else{
                                let size = 0 as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                            }
                            
                        }
                    }
                }
                Err(e) => {
                    logerr!("Failed to execute command '{}': {}", v.command, e);
                    if let Ok(b) = (TytoDBResponse{
                        content:format!("Failed to execute command '{}': {}", v.command, e),
                        success:0
                    }.to_bytes(&session_id).await){
                        let size = b.len() as u64;
                        response.extend_from_slice(&size.to_be_bytes());
                        response.extend_from_slice(&b);
                        //
                    }else{
                        if let Ok(b) = (TytoDBResponse{
                            content:e.to_string(),
                            success:1
                        }).to_bytes(&session_id).await{
                            let size = b.len() as u64;
                            response.extend_from_slice(&size.to_be_bytes());
                            response.extend_from_slice(&b);
                        }else{
                            let size = 0 as u64;
                            response.extend_from_slice(&size.to_be_bytes());
                        };
                    }
                }
            }
        }
        Err(e) => {
            if let Ok(b) = (TytoDBResponse{
                content:format!("Failed to deserialize payload '{}'", e),
                success:0
            }.to_bytes(&session_id).await){
                let size = b.len() as u64;
                response.extend_from_slice(&size.to_be_bytes());
                response.extend_from_slice(&b)
            }else{
                let size = 0 as u64;
                response.extend_from_slice(&size.to_be_bytes());
            }
        }
    }
    //
    if response.len() < 1{
        logerr!("empty response");
        return (0 as u64).to_be_bytes().as_slice().to_vec()
    }
    //
    return response;
}


use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, Method, StatusCode};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

async fn handle_data(req: Request<hyper::body::Incoming>,dbref: Arc<Mutex<Database>>) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().to_owned();
    let frame_stream = match req.collect().await{
        Ok(v)=> {v.to_bytes().to_vec()},
        Err(e) => {
            logerr!("{}",e);
            let r = Response::builder().status(StatusCode::BAD_REQUEST).body(Full::from(Bytes::from("Invalid input"))).unwrap();
            return Ok(r)
        }
    };
    if method == Method::POST{
        return Ok(Response::new(Full::new(Bytes::from(handle_data_tcp_inner(dbref, frame_stream).await))))
    }else{
        Ok(Response::new(Full::new(Bytes::from(handle_connections_tcp_inner(frame_stream, dbref).await))))
    }

}
impl Database{
    pub async fn run_database(self) -> Result<(), Error>{
        let crazy_config = engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_encode_padding(true)
        .with_decode_padding_mode(engine::DecodePaddingMode::Indifferent);
        let eng = base64::engine::GeneralPurpose::new(&alphabet::Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/").unwrap(), crazy_config);

        if fs::exists(secret_key_path()).unwrap(){
            let mut buffer : Vec<u8> = Vec::new();
            fs::File::open(secret_key_path()).unwrap().read_to_end(&mut buffer)?;
            let val = match serde_yaml::from_slice::<Vec<String>>(&buffer){Ok(a)=>a,Err(e)=>{return Err(gerr(&e.to_string()))}};
            // let bv : Vec<Vec<u8>> = val.iter().map(|s|{
            //     match eng.decode(s){
            //         Ok(a)=>a,
            //         Err(e)=>{
            //             logerr!("{}",e);
            //         }
            //     }
            // }).collect();
            let mut bv : Vec<Vec<u8>> = Vec::new();
            for i in val{
                match eng.decode(i) {
                    Ok(a)=>{bv.push(a);},
                    Err(e)=>{
                        return Err(gerr(&e.to_string()))
                    }
                };
            }

            let mut sk = self.secret_keys.lock().await;
            for i in bv{
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i);
            }
        }else{
            let mut file = fs::File::create_new(secret_key_path()).unwrap();
            let mut keys : Vec<Vec<u8>> = Vec::new();
            for _ in 0..self.settings.secret_key_count{
                keys.push(Aes256Gcm::generate_key(OsRng).to_vec());
            }
            let mut sk = self.secret_keys.lock().await;
            for i in keys.iter(){
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i.clone());
            }

            let mut b64_list : Vec<String> = Vec::new();
            for i in keys{
                b64_list.push(eng.encode(i))
            }
            if let Err(e) = serde_yaml::to_writer(&mut file, &b64_list){
                logerr!("{}",e);
                return Err(gerr(&e.to_string()))
            };
            file.flush()?;
            file.sync_all()?;
        }
        let settings = &self.settings;
        //let connection_tcp_url = format!("{}:{}",settings.ip,settings.connections_port);
        let data_tcp_url = format!("{}:{}",settings.ip,settings.data_port);
        //
        //
        
        let mtx_db = Arc::new(Mutex::new(self));
        // loop {
            
        //     handle_connections_tcp_sync(&connections_tcp,mtx_db.clone()).await;
        //     handle_data_tcp(&rrr,mtx_db.clone()).await;
        // }

        let addr = SocketAddr::from_str(&data_tcp_url).unwrap();
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let c = mtx_db.clone();
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| {
                        let b = c.to_owned();
                        async move {
                            handle_data(req, b).await
                        }
                    }))
                    .await
                {
                    logerr!("Error serving connection: {:?}", err);
                }
            });
        }

    }
}