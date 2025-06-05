
use std::{collections::{BTreeSet, HashMap}, ffi::CString, io::{self, Error, ErrorKind, Write}, mem::discriminant, os::unix::fs::FileExt, sync::Arc};
use ahash::AHashMap;
use tokio::{io::AsyncReadExt, sync::Mutex};
use tokio::fs::{File,self};
use crate::{alba_types::AlbaTypes, database::write_data, gerr, indexing::{Add, GetIndex, Indexing, Remove}, logerr, loginfo, strix::DataReference};


type MvccType = Arc<Mutex<(AHashMap<u64,(bool,Vec<AlbaTypes>)>,HashMap<String,(bool,String)>)>>;
#[derive(Debug)]
pub struct Container{
    pub file : Arc<Mutex<std::fs::File>>,
    pub element_size : usize,
    pub headers : Vec<(String,AlbaTypes)>,
    pub str_size : usize,
    pub mvcc : MvccType,
    pub headers_offset : u64,
    pub location : String,
    pub graveyard : Arc<Mutex<BTreeSet<u64>>>,
    pub indexing : Arc<Indexing>,
    file_path : String

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
    pub async fn new(container_name : String,path : &str,location : String,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Arc<Mutex<Self>>,Error> {
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
            location,
            graveyard: Arc::new(Mutex::new(BTreeSet::new())),
            indexing:Indexing::load_index(&container_name).await.unwrap(),
            file_path: path.to_string()
        }));
        Ok(container)
    }
    
}
impl Container{
    pub fn column_names(&self) -> Vec<String>{
        self.headers.iter().map(|v|v.0.to_string()).collect()
    }
}

async fn try_open_file(path: &str) -> io::Result<Option<File>> {
    match File::open(path).await {
        Ok(file) => Ok(Some(file)),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
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
    pub async fn arrlen_abs(&self) -> Result<u64, Error> {
        let file_len = self.len().await?;
        let file_rows = if file_len > self.headers_offset {
            (file_len - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        println!("header_offset: {:?}",self.headers_offset);
        Ok(file_rows)
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
        let mut total = self.arrlen().await?;
        //let mut virtual_ward : AHashMap<usize, DataReference> = AHashMap::new();
        let mut mvcc = self.mvcc.lock().await;
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc.0.iter() {
            loginfo!("{}",index);
            let v = (*index, value.1.clone());
            if value.0 {
                deletes.push(v);
            } else {
                insertions.push(v);
            }
        }
        mvcc.0.clear();
        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);
        let hdr_off = self.headers_offset;
        let row_sz = self.element_size as u64;
        let buf = vec![0u8; self.element_size];
        let fi = self.file.lock().await;
        for (row_index, row_data) in insertions {
            let serialized = self.serialize_row(&row_data)?;
            let offset = hdr_off + row_index * row_sz;
            if let Some(arg) = row_data.first(){
                let i = self.indexing.clone();
                let j = offset.clone();
                let idx = arg.get_index();
                i.add(idx, j).await?;
            }
            fi.write_all_at(serialized.as_slice(), offset)?;
            //virtual_ward.insert(offset as usize, (const_xxh3::xxh3_64(serialized.as_slice()),serialized));
        }
        
        let mut graveyard = self.graveyard.lock().await;
        for del in &deletes  {
            loginfo!("{}",del.0/row_sz);
            let from = hdr_off + del.0;
            if let Some(arg) = del.1.first(){
                let i = self.indexing.clone();
                let j = from.clone();
                let idx = arg.get_index();
                i.remove(idx, j).await.unwrap();
            }
            
            fi.write_all_at(&buf, from).unwrap();
            //virtual_ward.insert(from as usize, (const_xxh3::xxh3_64(&buf),buf.clone()));
            total -= 1;
            graveyard.insert(del.0);
        }
        

        for (i, txt) in  mvcc.1.iter(){
            let path = format!("{}/rf/{}", self.location, i); 
            if !txt.0 {
                let mut file: std::fs::File = std::fs::File::create_new(&path)?;
                if let Err(e) = file.write_all(txt.1.as_bytes()){
                    return Err(gerr(&format!("Failed to write in text file: {}",e)))
                };
                
                let buffer = txt.1.as_bytes();
                let c_path = match CString::new(path).map_err(|e| e.to_string()){Ok(a) => a, Err(e) => return Err(gerr(&e))};
                    let result = unsafe {
                        write_data(buffer.as_ptr(), buffer.len(), c_path.as_ptr())
                    };

                    if result != 1 {
                        logerr!("C write_data failed")
                    }
            } else if std::fs::exists(&path)?{
                fs::remove_file(&path).await?
            }
        
        }

        mvcc.1.clear();
        mvcc.1.shrink_to_fit();
        // if let Some(s) = STRIX.get(){
        //     let mut l = s.lock().await;
        //     l.wards.push(Mutex::new((std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path)?,virtual_ward)));
        // }
        fi.sync_all().unwrap();
        Ok(())
    }
    pub async fn get_rows(&self, index: (u64, u64)) -> Result<Vec<Vec<AlbaTypes>>, Error> {
        // INDEXES WILL BE TREATED AS RELATIVE, EACH BEING A REFERENCE TO (X*ELEMENT_SIZE) + header_size
        let arrlen = self.arrlen_abs().await.unwrap();
        let max = index.1.min(arrlen);
        if index.0 > max{
            return Err(gerr(&format!("Failed to get rows, the first index should be lower than the second. Review the arguments: (index0:{},index1:{})",index.0,index.1)))
        }

        let mut buffer = vec![0u8;(max.abs_diff(index.0)).max(1) as usize * self.element_size];
        self.file.lock().await.read_exact_at(&mut buffer, (index.0 * self.element_size as u64)+self.headers_offset).unwrap();
        println!("{}//{}",buffer.len(),self.len().await.unwrap());
        
        let mvcc = self.mvcc.lock().await;
        let mut result : Vec<Vec<AlbaTypes>> = Vec::with_capacity((max-index.0) as usize);
        for i in index.0..=max{
            let index = i as usize;
            if let Some(val) = mvcc.0.get(&i){
                if !val.0{
                    result.push(val.1.clone());
                    continue;
                }
            }
            if buffer.len() > self.element_size*index{
                result.push(
                    self.deserialize_row(
                        &buffer[index*self.element_size .. (index+1)*self.element_size] // row-bytes
                    ).await.unwrap() // row
                );
            }
        }
        Ok(result)
    }
    
    /* 
    pub async fn get_spread_rows(&mut self, index: &mut Vec<u64>) -> Result<Vec<Vec<AlbaTypes>>, Error> {
        let maxl = if self.len().await? > self.headers_offset {
            (self.len().await? - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        for i in index.iter_mut(){
            if *i > maxl {
                *i = maxl;
            }
        }
        index.sort();
        let mut buffers : BTreeMap<u64,Vec<u8>> = BTreeMap::new();
        let mut result : Vec<Vec<AlbaTypes>> = Vec::new();
        let mvcc = self.mvcc.lock().await;
        for i in index{
            if let Some(g) = mvcc.0.get(&i){
                if !g.0{
                    result.push(g.1.clone());
                    continue;
                }
            }
            buffers.insert(*i, vec![0u8;self.element_size]);
        }
        drop(mvcc);
        let mut it = buffers.iter_mut();
        while let Some(b) = it.next(){
            
            if let Some(c) = it.next(){
                if *c.0-*b.0 < 10{
                    let buff_size = ((*c.0 - *b.0 + 1) * self.element_size as u64) as usize;
                    let mut buff = vec![0u8;buff_size];
                    self.file.read_exact_at(&mut buff, (b.0*self.element_size as u64)+self.headers_offset)?;
                    *b.1 = buff[0..self.element_size].to_vec();
                    let c_off = ((*c.0-*b.0)*self.element_size as u64) as usize;
                    *c.1 = buff[c_off..(c_off+self.element_size)].to_vec();
                    result.push(self.deserialize_row(b.1.to_vec()).await?);
                    result.push(self.deserialize_row(c.1.to_vec()).await?);
                    continue;
                }else{
                    self.file.read_exact_at(b.1, (b.0*self.element_size as u64)+self.headers_offset)?;
                    self.file.read_exact_at(c.1, (c.0*self.element_size as u64)+self.headers_offset)?;
                    result.push(self.deserialize_row(b.1.to_vec()).await?);
                    result.push(self.deserialize_row(c.1.to_vec()).await?);

                    continue;
                }
            }
            self.file.read_exact_at(b.1, (b.0*self.element_size as u64)+self.headers_offset)?;
            result.push(self.deserialize_row(b.1.to_vec()).await?);
        }
        
        result.shrink_to_fit();
        Ok(result)
    }
    */
    pub fn columns(&self) -> Vec<AlbaTypes>{
        self.headers.iter().map(|v|v.1.clone()).collect()
    }
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(self.element_size);
    
        for (item, ty) in row.iter().zip(self.columns().iter()) {
            loginfo!("item: {:?} | ty: {:?}",discriminant(item),discriminant(ty));
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
                    let size = self.str_size;
                    let bytes = buf[index..index+size].to_vec();
                    index += size;
                    let trimmed: Vec<u8> = bytes.into_iter()
                        .take_while(|&b| b != 0)
                        .collect();
                    let str_id = String::from_utf8(trimmed)
                        .map_err(|e| gerr(&format!("Text decoding failed: {}", e)))?;
    
                    // Check external text storage
                    let mut file = match try_open_file(&format!("{}/rf/{}", self.location, str_id)).await? {
                        Some(f) => f,
                        None => {
                            values.push(AlbaTypes::Text(str_id));
                            continue;
                        }
                    };
    
                    let mut content = Vec::new();
                    file.read_to_end(&mut content).await?;
                    values.push(AlbaTypes::Text(String::from_utf8(content)
                        .map_err(|e| gerr(&format!("Text file corrupt: {}", e)))?));
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