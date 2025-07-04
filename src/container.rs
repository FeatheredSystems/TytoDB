
use std::{collections::{BTreeSet, HashMap}, io::{Error, ErrorKind}, os::fd::AsRawFd, sync::Arc};
use ahash::AHashMap;
use tokio::sync::Mutex;
use crate::{alba_types::AlbaTypes, database::{batch_write_data, WriteEntry}, gerr};


type MvccType = Arc<Mutex<(AHashMap<u64,(bool,Vec<AlbaTypes>)>,HashMap<String,(bool,String)>)>>;
#[derive(Debug)]
pub struct Container{
    pub file : Arc<Mutex<std::fs::File>>,
    pub element_size : usize,
    pub headers : Vec<(String,AlbaTypes)>,
    pub str_size : usize,
    pub mvcc : MvccType,
    pub headers_offset : u64,
    pub graveyard : Arc<Mutex<BTreeSet<u64>>>

}
fn serialize_closed_string(item : &AlbaTypes,s : &String,buffer : &mut Vec<u8>){
    let mut bytes = Vec::with_capacity(item.size());
    let mut str_bytes = s.as_bytes().to_vec();
    let str_length = str_bytes.len().to_le_bytes().to_vec();
    str_bytes.truncate(item.size()-size_of::<usize>());
    bytes.extend_from_slice(&str_length);
    bytes.extend_from_slice(&str_bytes);
    bytes.resize(item.size(),0);
    buffer.extend_from_slice(&bytes);
}
fn serialize_closed_blob(item : &AlbaTypes,blob : &mut Vec<u8>,buffer : &mut Vec<u8>){
    let mut bytes: Vec<u8> = Vec::with_capacity(item.size());
    let blob_length: Vec<u8> = blob.len().to_le_bytes().to_vec();
    blob.truncate(item.size()-size_of::<usize>());
    bytes.extend_from_slice(&blob_length);
    bytes.extend_from_slice(blob);
    bytes.resize(item.size(),0);
    buffer.extend_from_slice(&bytes);
}


impl Container {
    pub async fn new(path : &str,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Arc<Mutex<Self>>,Error> {
        let mut  headers = Vec::new();
        for index in 0..((columns.len()+column_names.len())/2){
            let name = match column_names.get(index){
                Some(nm) => nm,
                None => {
                    return Err(gerr("Failed to create container, the size of column types and names must be equal. And this error is a consequence of that property not being respected."))
                } 
            };
            let value = match columns.get(index){
                Some(vl) => vl,
                None => {
                    return Err(gerr("Failed to create container, the size of column types and names must be equal. And this error is a consequence of that property not being respected."))
                }
            };
            if name.is_empty(){
                continue;
            }
            if let AlbaTypes::NONE = value{
                continue
            }
            headers.push((name.to_owned(), value.to_owned()));
        }
        let file = Arc::new(Mutex::new(std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap()));
        let mut hash_header = HashMap::new();
        for i in headers.iter(){
            hash_header.insert(i.0.clone(),i.1.clone());
        }
        let container = Arc::new(Mutex::new(Container{
            file:file.clone(),
            element_size: element_size.clone(),
            str_size,
            mvcc: Arc::new(Mutex::new((AHashMap::new(),HashMap::new()))),
            headers_offset: headers_offset.clone() ,
            headers,
            graveyard: Arc::new(Mutex::new(BTreeSet::new()))
        }));
        Ok(container)
    }
    
}
impl Container{
    pub fn column_names(&self) -> Vec<String>{
        self.headers.iter().map(|v|v.0.to_string()).collect()
    }
}

fn handle_fixed_string(buf: &[u8],index: &mut usize,instance_size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = &buf[*index..*index+instance_size];
    let mut size : [u8;8] = [0u8;8];
    size.clone_from_slice(&bytes[..8]); 
    if bytes.len() <= 8 {
        return Err(gerr("Not insuficient string size"));
    }
    let string_length = usize::from_le_bytes(size);
    let string_bytes = if string_length > 0 {
        let l = bytes.len();
        if (string_length+8) >= l {
            &bytes[8..]
        }else{
           &bytes[8..(8+string_length)] 
        }
        
    }else{
        
        let s = String::new();
        match instance_size {
            18 => values.push(AlbaTypes::NanoString(s)),
            108 => values.push(AlbaTypes::SmallString(s)),
            508 => values.push(AlbaTypes::MediumString(s)),
            2_008 => values.push(AlbaTypes::BigString(s)),
            3_008 => values.push(AlbaTypes::LargeString(s)),
            _ => unreachable!(),
        }
        return Ok(())
    };
    
    *index += instance_size;
    let trimmed: Vec<u8> = string_bytes.iter()
        .take_while(|&&b| b != 0)
        .cloned()
        .collect();
    let s = String::from_utf8_lossy(&trimmed).to_string();
    
    match instance_size {
        18 => values.push(AlbaTypes::NanoString(s)),
        108 => values.push(AlbaTypes::SmallString(s)),
        508 => values.push(AlbaTypes::MediumString(s)),
        2_008 => values.push(AlbaTypes::BigString(s)),
        3_008 => values.push(AlbaTypes::LargeString(s)),
        _ => unreachable!(),
    }
    Ok(())
}

fn handle_bytes(buf: &[u8],index: &mut usize,size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = buf[*index..*index+size].to_vec();
    let mut blob_size : [u8;8] = [0u8;8];
    blob_size.clone_from_slice(&bytes[..8]); 
    let blob_length = usize::from_le_bytes(blob_size);
    let blob : Vec<u8> = if blob_length > 0 {
        if blob_length >= bytes.len() {
            bytes[8..].to_vec()
        }else{
           bytes[8..(8+blob_length)].to_vec() 
        }
        
    }else{
        
        let blob = Vec::new();
        match size {
            18 => values.push(AlbaTypes::NanoBytes(blob)),
            1008 => values.push(AlbaTypes::SmallBytes(blob)),
            10_008 => values.push(AlbaTypes::MediumBytes(blob)),
            100_008 => values.push(AlbaTypes::BigSBytes(blob)),
            1_000_008 => values.push(AlbaTypes::LargeBytes(blob)),
            _ => unreachable!(),
        }
        return Ok(())
    };

    *index += size;
    
    match size {
        18 => values.push(AlbaTypes::NanoBytes(blob)),
        1008 => values.push(AlbaTypes::SmallBytes(blob)),
        10_008 => values.push(AlbaTypes::MediumBytes(blob)),
        100_008 => values.push(AlbaTypes::BigSBytes(blob)),
        1_000_008 => values.push(AlbaTypes::LargeBytes(blob)),
        _ => unreachable!(),
    }
    Ok(())
}

impl Container{
    pub async fn len(&self) -> Result<u64,Error>{
        Ok(self.file.lock().await.metadata()?.len())
    }
    pub async fn arrlen(&self) -> Result<u64, Error> {
        let file_len = self.len().await?;
        let file_rows = if file_len > self.headers_offset {
            (file_len - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        let mvcc_max = {
            let mvcc = self.mvcc.lock().await;
            mvcc.0.keys().copied().max().map_or(0, |max_index| max_index + 1)
        };
        Ok(file_rows.max(mvcc_max))
    }
    // pub fn get_alba_type_from_column_name(&self,column_name : &String) -> Option<AlbaTypes>{
    //     for i in self.headers.iter(){
    //         if *i.0 == *column_name{
    //             let v = i.1.clone();
    //             return Some(v)
    //         }
    //     }
    //     None
    // }

    pub async fn get_next_addr(&self) -> Result<u64, Error> {
        let mut graveyard = self.graveyard.lock().await;
        if graveyard.len() > 0{
            if let Some(id) = graveyard.pop_first(){
                return Ok(id)
            }
        }
        let current_rows = self.arrlen().await?;
        let mvcc = self.mvcc.lock().await;
        for (&key, (deleted, _)) in mvcc.0.iter() {
            if *deleted {
                return Ok(key);
            }
        }
        Ok(current_rows)
    } 
    pub async fn push_row(&mut self, data : &Vec<AlbaTypes>) -> Result<(),Error>{
        let ind = self.get_next_addr().await?;
        let mut mvcc_guard = self.mvcc.lock().await;
        mvcc_guard.0.insert(ind, (false,data.clone()));
        Ok(())
    }
    pub async fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.lock().await;
        mvcc_guard.0.clear();
        mvcc_guard.1.clear();
        drop(mvcc_guard);
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), Error> {
        //let mut virtual_ward : AHashMap<usize, DataReference> = AHashMap::new();
        let mut mvcc = self.mvcc.lock().await;
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc.0.iter() {
            
            let offset = (index * self.element_size as u64) + self.headers_offset;
            let v = (offset, value.1.clone());
            if value.0 {
                deletes.push(v);
            } else {
                insertions.push(v);
            }
        }
        mvcc.0.clear();
        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);
        let buf = vec![0u8; self.element_size];

        let mut writting : Vec<(u64,Vec<u8>)> = Vec::with_capacity(insertions.len());

        for (row_index, row_data) in insertions {
            let serialized = self.serialize_row(&row_data)?;
            let offset = row_index;
            writting.push((offset,serialized));
        }


        let mut graveyard = self.graveyard.lock().await;

        for del in &deletes {
            let offset = del.0;
            let row_index = (offset - self.headers_offset) / self.element_size as u64;
            writting.push((offset,buf.clone()));
            graveyard.insert(row_index);  
        }
        
        mvcc.1.clear();
        mvcc.1.shrink_to_fit();
        // if let Some(s) = STRIX.get(){
        //     let mut l = s.lock().await;
        //     l.wards.push(Mutex::new((std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path)?,virtual_ward)));
        // }
        
        let mut l = Vec::new();
        for i in writting{
            let len = i.1.len();
            l.push(WriteEntry{
                buffer: Arc::new(i.1),
                length: len,
                offset: i.0 as i64
            });
        }
        let l_1 = l.len();
        let c= self.file.clone();
        tokio::spawn(async move{
            batch_write_data(l, l_1, c.lock().await.as_raw_fd()).await;
        });
        
        
        Ok(())
    }
    
    pub fn columns(&self) -> Vec<AlbaTypes>{
        self.headers.iter().map(|v|v.1.clone()).collect()
    }
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(self.element_size);
    
        for (item, ty) in row.iter().zip(self.columns().iter()) {
            
            let item = &AlbaTypes::to_another(item, ty);
            match (item, ty) {
                (AlbaTypes::Bigint(v), AlbaTypes::Bigint(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Int(v), AlbaTypes::Int(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Float(v), AlbaTypes::Float(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Bool(v), AlbaTypes::Bool(_)) => {
                    buffer.push(if *v { 1 } else { 0 });
                },
                (AlbaTypes::Char(c), AlbaTypes::Char(_)) => {
                    let code = *c as u32;
                    buffer.extend_from_slice(&code.to_le_bytes());
                },
                (AlbaTypes::Text(s), AlbaTypes::Text(_)) => {
                    let mut bytes = s.as_bytes().to_vec();
                    bytes.resize(self.str_size, 0);
                    buffer.extend_from_slice(&bytes);
                },
                (AlbaTypes::NanoString(s), AlbaTypes::NanoString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::SmallString(s), AlbaTypes::SmallString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::MediumString(s), AlbaTypes::MediumString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::BigString(s), AlbaTypes::BigString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::LargeString(s), AlbaTypes::LargeString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::NanoBytes(v ), AlbaTypes::NanoBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::SmallBytes(v), AlbaTypes::SmallBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::MediumBytes(v), AlbaTypes::MediumBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::BigSBytes(v), AlbaTypes::BigSBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::LargeBytes(v), AlbaTypes::LargeBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::NONE, AlbaTypes::NONE) => {
                    let size = item.size();
                    buffer.extend(vec![0u8; size]);
                },
                _ => return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Type mismatch between value {:?} and column type {:?}", item, ty)
                )),
            }
        }
    
        // Validate buffer size matches element_size
        if buffer.len() != self.element_size {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Serialized size mismatch: expected {}, got {}",
                    self.element_size,
                    buffer.len()
                )
            ));
        }
    
        Ok(buffer)
    }
    pub async fn deserialize_row(&self, buf: &[u8]) -> Result<Vec<AlbaTypes>, Error> {
        let mut index = 0;
        let mut values = Vec::new();
    
        for column_type in &self.columns() {
            match column_type {
                // Primitive types
                AlbaTypes::Bigint(_) => {
                    let size = std::mem::size_of::<i64>();
                    let bytes: [u8; 8] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read bigint: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Bigint(i64::from_be_bytes(bytes)));
                },
                
                AlbaTypes::Int(_) => {
                    let size = std::mem::size_of::<i32>();
                    let bytes: [u8; 4] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read int: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Int(i32::from_be_bytes(bytes)));
                },
    
                AlbaTypes::Float(_) => {
                    let size = std::mem::size_of::<f64>();
                    let bytes: [u8; 8] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read float: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Float(f64::from_be_bytes(bytes)));
                },
    
                AlbaTypes::Bool(_) => {
                    let size = std::mem::size_of::<bool>();
                    let byte = *buf.get(index).ok_or(gerr("Incomplete bool data"))?;
                    index += size;
                    values.push(AlbaTypes::Bool(byte != 0));
                },
    
                AlbaTypes::Char(_) => {
                    let size = std::mem::size_of::<u32>();
                    let bytes: [u8; 4] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read char: {}", e)))?;
                    index += size;
                    let code = u32::from_le_bytes(bytes);
                    values.push(AlbaTypes::Char(match char::from_u32(code){
                        Some(a) => a,
                        None => {
                            return Err(gerr("Invalid Unicode scalar value"))
                        }
                    }));
                },
    
                // Text types
                AlbaTypes::Text(_) => {
                    values.push(AlbaTypes::Text(String::new()));
                },
    
                // Fixed-size string types
                AlbaTypes::NanoString(_) => handle_fixed_string(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::SmallString(_) => handle_fixed_string(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::MediumString(_) => handle_fixed_string(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::BigString(_) => handle_fixed_string(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::LargeString(_) => handle_fixed_string(&buf, &mut index, column_type.size(), &mut values)?,
    
                // Byte array types
                AlbaTypes::NanoBytes(_) => handle_bytes(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::SmallBytes(_) => handle_bytes(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::MediumBytes(_) => handle_bytes(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::BigSBytes(_) => handle_bytes(&buf, &mut index, column_type.size(), &mut values)?,
                AlbaTypes::LargeBytes(_) => handle_bytes(&buf, &mut index, column_type.size(), &mut values)?,
    
                // Null handling
                AlbaTypes::NONE => {
                    values.push(AlbaTypes::NONE);
                }
            }
        }
    
        Ok(values)
    }
    
}