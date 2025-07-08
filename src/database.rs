use std::{collections::HashMap, fs::{self, File}, io::{Error, ErrorKind, Read, Write}, os::{fd::AsRawFd, raw::c_int, unix::fs::FileExt}, path::PathBuf, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_yaml;
use crate::{alba_types::AlbaTypes, container::Container, gerr, logerr, query::{search, Query, SearchArguments}, query_conditions::QueryConditions, row::Row, AstCommit, AstCreateRow, AstDeleteContainer, AstDeleteRow, AstEditRow, AstRollback, AstSearch, Token, AST};
use rand::{rngs::OsRng, RngCore};
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


#[repr(C)]
pub struct WriteEntryC{
    pub buffer : *const u8,
    pub length : usize,
    pub offset : i64,
}

#[repr(C)]
pub struct ReadInstance{
    pub size : u64,
    pub offset : u64,
    pub buffer : *mut u8,
}

#[repr(C)]
pub struct ReadEntry{
    pub buffer_array : *mut ReadInstance,
    pub len : u64
}

pub struct WriteEntry{
    pub buffer : Arc<Vec<u8>>,
    pub length : usize,
    pub offset : i64,
}
impl WriteEntry{
    fn to_c(&self) -> WriteEntryC{
        WriteEntryC{
            buffer : self.buffer.as_slice().as_ptr(),
            length : self.length,
            offset : self.offset,
        }
    }
}

#[link(name = "io", kind = "static")]
unsafe extern "C" {
    pub unsafe fn batch_write_data_c(buffer: *const WriteEntryC, len: usize, file: c_int) -> i32;
    unsafe fn batch_reads(re : *mut ReadEntry,file : i32) -> i32;
}

pub fn batch_reads_abs(mut read_instances : Vec<ReadInstance>,file : &File) -> Result<(),Error>{
    let mut r = ReadEntry{
        len : read_instances.len() as u64,
        buffer_array: read_instances.as_mut_ptr()
    };
    let a : i32 = unsafe{batch_reads(&mut r, file.as_raw_fd().clone())};

    match a {
        0 => Ok(()),
        -1 => Err(Error::new(ErrorKind::Other, "Failed to get SQE")),
        -2 => Err(Error::new(ErrorKind::Other, "Failed to init queue")),
        -3 => Err(Error::new(ErrorKind::Other, "Failed to submit io_uring_submit")),
        _ => Err(Error::new(ErrorKind::Other, "Something failed :P")),
    }
}

pub async fn batch_write_data(entries: Vec<WriteEntry>, len: usize, file: c_int) -> i32 {
    let c_buffer: Vec<WriteEntryC> = entries.iter().map(|f| f.to_c()).collect();
    
    unsafe {
        batch_write_data_c(c_buffer.as_ptr(), len, file)
    }
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


fn create_container_headers(column_names : Vec<String>,column_values : Vec<AlbaTypes>) -> Vec<u8>{
    let mut byteload : Vec<u8> = Vec::new();
    let len = column_names.len();
    byteload.extend_from_slice(&(len as u64).to_le_bytes());
    for i in column_names.into_iter().zip(column_values){
        let size = i.0.len() as u64;
        let mut b = Vec::new();
        b.extend_from_slice(&size.to_le_bytes());
        b.extend_from_slice(&i.0.as_bytes());
        b.push(i.1.get_id());
        byteload.extend_from_slice(&b);
    }
    byteload
}
fn get_container_headers(file : &File) -> Result<(Vec<String>,Vec<AlbaTypes>,u64),Error>{
    let mut offset = 0u64;
    let column_count = {
        let mut buf = [0u8;8];
        file.read_exact_at(&mut buf, offset)?;
        offset += 8;
        u64::from_le_bytes(buf)
    };

    let mut col_nam = Vec::new();
    let mut col_val = Vec::new();

    for _ in 0..column_count{
        let mut size_len = [0u8;8];
        file.read_exact_at(&mut size_len, offset)?;
        let str_size = u64::from_le_bytes(size_len);
        offset += 8;

        let mut str_buff = vec![0u8;str_size as usize];
        file.read_exact_at(&mut str_buff, offset)?;
        offset += str_size;

        let mut column_type_buffer = [0u8;1];
        file.read_exact_at(&mut column_type_buffer, offset)?;
        offset += 1;

        let column_name = String::from_utf8_lossy(&str_buff).to_string();
        let column_type = AlbaTypes::from_id(column_type_buffer[0])?;
        col_nam.push(column_name);
        col_val.push(column_type);
    }
    Ok((col_nam,col_val,offset))
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
                    &format!("{}/{}", self.location, contain),
                    element_size,
                    he.1,
                    header_offset,
                    he.0
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
            let mut file = fs::File::open(&path)?;
            let val = get_container_headers(&mut file)?;
            return Ok(((val.0,val.1),val.2 as u64))
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
                let mut el : usize = 0;
                for i in structure.col_val.iter(){
                    el += i.size()
                }

                file.write_all(&create_container_headers( structure.col_nam.clone(), structure.col_val.clone())).unwrap();
                self.containers.push(structure.name.clone());
                
                let c = Container::new(
                    &path,
                    el,
                    structure.col_val,
                    file.metadata()?.len(),
                    structure.col_nam
                ).await.unwrap();
                self.container.insert(structure.name, c);
                self.save_containers().unwrap();
            },
            AST::CreateRow(structure) => {
                println!("{:?}",structure);
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

                let mut val  = container.columns();

                let mut id_map = HashMap::new();
                for i in container.column_names().into_iter().enumerate(){
                    id_map.insert(i.1, i.0);
                }

                for i in structure.col_nam.into_iter().enumerate(){
                    let val1 = &structure.col_val[i.0];
                    if let Some(a) = id_map.get(&i.1){
                        val[*a] = val1.clone();
                    }
                }

                container.push_row(val).await?;
                if self.settings.auto_commit {
                    container.commit().await?;
                }
                
            },
            AST::Search(structure) => {
                let container = if let Some(a) = self.container.get(&structure.container){
                    a
                }else{
                    return Err(gerr("There is no container with the given name"))
                };
                let sa = {
                    let c = container.clone();
                    let sa = c.lock().await;

                    let col_prop = {
                        let mut h = HashMap::new();
                        for i in sa.headers.clone(){
                            h.insert(i.0,i.1);
                        }
                        h
                    };
                    let pk = sa.headers[0].0.clone();
                    SearchArguments { 
                        element_size: sa.element_size.clone(),
                        header_offset: sa.headers_offset.clone() as usize,
                        file: sa.file.clone(),
                        conditions: QueryConditions::from_primitive_conditions(structure.conditions,&col_prop,pk)?
                    }
                };
                println!("{:?}",sa);
                let rows = search(container.clone(), sa).await?.0;
                println!("rows: {:?}",rows);
                return Ok(Query { rows: (container.lock().await.column_names(),rows) })
            },
            AST::EditRow(structure) => {
                let container = if let Some(a) = self.container.get(&structure.container){
                    a
                }else{
                    return Err(gerr("There is no container with the given name"))
                };
                let sa = {
                    let c = container.clone();
                    let sa = c.lock().await;

                    let col_prop = {
                        let mut h = HashMap::new();
                        for i in sa.headers.clone(){
                            h.insert(i.0,i.1);
                        }
                        h
                    };
                    let pk = sa.headers[0].0.clone();
                    SearchArguments { 
                        element_size: sa.element_size.clone(),
                        header_offset: sa.headers_offset.clone() as usize,
                        file: sa.file.clone(),
                        conditions: QueryConditions::from_primitive_conditions(structure.conditions,&col_prop,pk)?
                    }
                };
                let mut rows = search(container.clone(), sa).await?;

                let c = container.lock().await;
                let mut indexes = Vec::new();
                for i in structure.col_nam.iter().enumerate(){
                    for j in c.headers.iter().enumerate(){
                        if *j.1.0 == *i.1{
                            indexes.push((j.0,structure.col_val[i.0].clone()));
                        }
                    }
                }

                for i in rows.0.iter_mut(){
                    for j in indexes.iter(){
                        i.data[j.0] = j.1.clone();
                    }
                }
                for i in rows.0.iter().zip(rows.1.iter()){
                    c.mvcc.lock().await.0.insert(*i.1, (false,i.0.data.clone()));
                }
                
                return Ok(Query { rows: (vec![],vec![]) })
            },
            AST::DeleteRow(structure) => {
                let container = if let Some(a) = self.container.get(&structure.container){
                    a
                }else{
                    return Err(gerr("There is no container with the given name"))
                };
                let sa = {
                    let c = container.clone();
                    let sa = c.lock().await;

                    let col_prop = {
                        let mut h = HashMap::new();
                        for i in sa.headers.clone(){
                            h.insert(i.0,i.1);
                        }
                        h
                    };
                    let pk = sa.headers[0].0.clone();
                    SearchArguments { 
                        element_size: sa.element_size.clone(),
                        header_offset: sa.headers_offset.clone() as usize,
                        file: sa.file.clone(),
                        conditions: QueryConditions::from_primitive_conditions(if let Some(a) = structure.conditions{a}else{(Vec::new(),Vec::new())},&col_prop,pk)?
                    }
                };
                
                let (_,indexes) = search(container.clone(), sa).await?;
                let container = container.lock().await;
                let mut mvcc = container.mvcc.lock().await;
                for i in indexes{
                    mvcc.0.insert(i,(true,Vec::new()));
                }
                return Ok(Query{rows:(Vec::new(),Vec::new())})
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
                                
                                return Ok(Query{rows:(Vec::new(),Vec::new())});
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
                                
                                return Ok(Query{rows:(Vec::new(),Vec::new())});
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
        
        Ok(Query{rows: (Vec::new(),Vec::new())})
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


use tytodb_conn::{commands::Commands as commands, db_response::{DBResponse, Row as NetRow}, logical_operators::LogicalOperator};
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
fn ab_to_nat(a : AlbaTypes) -> NetworkAlbaTypes{
    match a{
        AlbaTypes::Text(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::Int(a) => NetworkAlbaTypes::I32(a),
        AlbaTypes::Bigint(a) => NetworkAlbaTypes::I64(a),
        AlbaTypes::Float(a) => NetworkAlbaTypes::F64(a),
        AlbaTypes::Bool(a) => NetworkAlbaTypes::Bool(a),
        AlbaTypes::Char(a) => NetworkAlbaTypes::String(a.to_string()),
        AlbaTypes::NanoString(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::SmallString(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::MediumString(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::BigString(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::LargeString(a) => NetworkAlbaTypes::String(a),
        AlbaTypes::NanoBytes(a) => NetworkAlbaTypes::Bytes(a),
        AlbaTypes::SmallBytes(a) => NetworkAlbaTypes::Bytes(a),
        AlbaTypes::MediumBytes(a) => NetworkAlbaTypes::Bytes(a),
        AlbaTypes::BigSBytes(a) => NetworkAlbaTypes::Bytes(a),
        AlbaTypes::LargeBytes(a) => NetworkAlbaTypes::Bytes(a),
        AlbaTypes::NONE => NetworkAlbaTypes::U8(0),
    }
}
fn abl_to_nat(a : Vec<AlbaTypes>) -> Vec<NetworkAlbaTypes>{
    println!("input abl: {:?}",a);
    a.iter().map(|f|ab_to_nat(f.to_owned())).collect()
}

fn query_to_bytes(q : Query) -> Vec<u8>{
    println!("input query: {:?}",q);
    let a = row_list_to_bytes(q.rows.1.iter().map(|f|NetRow::new(abl_to_nat(f.data.to_owned()))).collect());
    println!("returning bytes: {:?}",a);
    a
}

fn row_list_to_bytes(a : Vec<tytodb_conn::db_response::Row>) -> Vec<u8>{
    println!("rows_being_encoded: {:?}",a);
   DBResponse::new(a).encode()
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
    let a = (t.0.iter().map(|f| {
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
    }).collect(), t.1.iter().map(|f|{(f.0 , f.1)}).collect());
    println!("conditions: {:?}",a);
    a
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
            let mut val = vec![0u8];
            val.extend_from_slice(&query_to_bytes(match commands::decompile(&input){
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
                                Ok(mut q) => {
                                    q.rows.0.push("s!".to_string());
                                    q.rows.1.push(Row{data:vec![AlbaTypes::Bool(true)]});
                                    q
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
                                Ok(a) => a,
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
                                conditions: conditions_to_tyto_db((edit_row.conditions.0,edit_row.conditions.1.iter().map(|f|{(f.0 as usize,f.1)}).collect()))
                            })).await{
                                Ok(a) => a,
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
                                Ok(a) => a,
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
                                Ok(a) => a,
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::Search(search) => {
                            println!("command : Search");
                            let mtx_db = &mtx_db;
                            match mtx_db.lock().await.run(AST::Search(AstSearch{
                                col_nam: search.col_nam,
                                container: search.container,
                                conditions: conditions_to_tyto_db((search.conditions.0,search.conditions.1.iter().map(|f|{(f.0 as usize ,f.1)}).collect()))
                            })).await{
                                Ok(a) => a,
                                Err(e) => {
                                    let mut b = vec![1u8,73, 110, 118, 97, 108, 105, 100, 32, 104, 101, 97, 100, 101, 114, 115, 32];
                                    b.extend_from_slice(&e.to_string().as_bytes());
                                    return b
                                }
                            }
                        },
                        commands::Commit(commit) => {
                            match mtx_db.lock().await.run(AST::Commit(AstCommit{
                                container: commit.container
                            })).await{
                                Ok(a) => a,
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
                                Ok(a) => a,
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
            }));
            val
        })});
        Server::new(host, password, message_handler, workers).await
    }
}
