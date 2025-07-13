
use std::{collections::{BTreeMap, BTreeSet, HashMap}, fs::{self, File, OpenOptions}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, ErrorKind, Read, Write}, os::{fd::AsRawFd, unix::fs::{FileExt, MetadataExt}}, sync::Arc};
use tokio::sync::Mutex;
use crate::{alba_types::{into_schema,AlbaTypes}, database::{batch_write_data, WriteEntry}, gerr, indexing:: Hashmap as IndexingHashMap};
use bitvec::prelude::*;
pub const MAX_GRAVEYARD_LENGTH_IN_MEMORY : usize = 1250;

type MvccType = Arc<Mutex<(BTreeMap<u64,(MvccState,Vec<AlbaTypes>)>,HashMap<String,(bool,String)>)>>;

#[derive(Debug)]
pub struct MvccRecord(Arc<Mutex<File>>);
impl MvccRecord{
    fn new(name : String) -> Result<Self,Error>{
        let file = OpenOptions::new().read(true).write(true).append(true).create(!fs::exists(&name)?).open(name)?;
        Ok(MvccRecord(Arc::new(Mutex::new(file))))
    }
    async fn put(&mut self,bytes : Vec<u8>) -> Result<(),Error>{
        let reference = self.0.clone();
        let _ = tokio::task::spawn_blocking(async move || -> Result<(),Error> {
            let mut bibi = reference.lock().await;
            let e0 = bibi.write_all(&bytes);
            let e1 = bibi.sync_all();
            if let Err(e) = e0{eprintln!("ERROR{:?}",e)}
            if let Err(e) = e1{eprintln!("ERROR {:?}",e)}
            Ok(())
        }).await;
        Ok(())
    }
    async fn yield_(&mut self) -> Result<Vec<u8>,Error>{
        let mut buffer = Vec::new();
        self.0.lock().await.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
    async fn clear(&mut self) -> Result<(),Error> {
        self.0.lock().await.set_len(0)?; self.sync().await?;Ok(())
    }
    async fn sync(&mut self) -> Result<(),Error>{
        let reference = self.0.clone();
        tokio::task::spawn_blocking(async move ||{    
            let n = reference.lock().await;
            let _ = n.sync_data();
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct Container{
    pub file : Arc<Mutex<std::fs::File>>,
    pub element_size : usize,
    pub headers : Vec<(String,AlbaTypes)>,
    pub mvcc : MvccType,
    pub headers_offset : u64,
    pub graveyard : Arc<Mutex<BTreeSet<u64>>>,
    pub index_map : Arc<Mutex<IndexingHashMap>>,
    pub mvcc_record : Arc<Mutex<MvccRecord>>

}
#[derive(Debug,Copy,Clone)]
pub enum MvccState{
    Delete,
    Insert,
    Edit
}

pub fn get_index(i : AlbaTypes) -> u64{
    match i{
        AlbaTypes::Int(b) => b as u64,
        AlbaTypes::Bigint(b) => b as u64,
        AlbaTypes::Float(b) => b as u64,
        AlbaTypes::Char(b) => b as u64,
        AlbaTypes::Bool(b) => b as u64,
        AlbaTypes::NanoBytes(b)|AlbaTypes::SmallBytes(b)|AlbaTypes::MediumBytes(b)|AlbaTypes::BigSBytes(b)|AlbaTypes::LargeBytes(b) => {
            let mut hasher = DefaultHasher::new();
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NanoString(b)|AlbaTypes::SmallString(b)|AlbaTypes::MediumString(b)|AlbaTypes::BigString(b)|AlbaTypes::LargeString(b)|AlbaTypes::Text(b) => {
            let mut hasher = DefaultHasher::new();
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NONE => 0u64

    }
}

impl Container {
    pub async fn new(path : &str,element_size : usize, columns : Vec<AlbaTypes>,headers_offset : u64,column_names : Vec<String>) -> Result<Arc<Mutex<Self>>,Error> {
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
        let file = Arc::new(Mutex::new(std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap()));
        let mut hash_header = HashMap::new();
        for i in headers.iter(){
            hash_header.insert(i.0.clone(),i.1.clone());
        }
        let container = Arc::new(Mutex::new(Container{
            element_size,
            mvcc: Arc::new(Mutex::new((BTreeMap::new(),HashMap::new()))),
            headers_offset,
            headers,
            graveyard: Arc::new(Mutex::new(BTreeSet::new())),
            file,
            mvcc_record: Arc::new(Mutex::new(MvccRecord::new(format!("{}.mr",path))?)),
            index_map: Arc::new(Mutex::new(IndexingHashMap::new(format!("{}.hm",path))?))
        }));
        container.lock().await.load_mvcc().await?;
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
    let mut size_bytes : [u8;8] = [0u8;8];
    size_bytes.clone_from_slice(&bytes[..8]); 

    let string_length = u64::from_be_bytes(size_bytes) as usize;

    if 8 + string_length > instance_size {
        return Err(gerr(&format!("Invalid string length in data, expected at most {} but got {}", instance_size - 8, string_length)));
    }

    let string_bytes = &bytes[8..(8 + string_length)];
    
    *index += instance_size;
    let s = String::from_utf8_lossy(string_bytes).to_string();
    
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
    let blob_length = u64::from_le_bytes(blob_size);
    let blob : Vec<u8> = if blob_length > 0 {
        if blob_length >= bytes.len() as u64{
            bytes[8..].to_vec()
        }else{
           bytes[8..(8+blob_length as usize)].to_vec() 
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
const VACCUM_SIZE : u64 = 4194304;
const MAX_VACUUM_LENGTH : usize = 625000;
impl Container{
    pub async fn get_next_addr(&self) -> Result<u64, Error> {
        let mv = self.mvcc.lock().await;
        let mut gy = self.graveyard.lock().await;
        if let Some(s) = gy.pop_first(){
            return Ok(s)
        }
        let m = mv.0.keys().max();
        let size = self.file.lock().await.metadata()?.size();
        if let Some(m) = m{
            return Ok(*m+self.element_size as u64)
        }
        Ok(size)
    }
    pub async fn vacuum(&mut self) -> Result<(),Error> {
        self.graveyard.lock().await.clear();
        let mut mvcc = self.mvcc.lock().await;
        mvcc.0.clear(); mvcc.1.clear();

        let fi = self.file.lock().await;
        let element_size = self.element_size as u64;
        let length = (fi.metadata()?.size()-self.headers_offset)/element_size;

        if length == 0{
            return Ok(());
        }

        let mut map = bitvec!();
        let mut readen = 0u64;
        let chunk_size : u64 = VACCUM_SIZE/self.element_size as u64;
        let empty = vec![255u8;self.element_size];
        let mut pairs : Vec<(u64,u64)> = Vec::new();
        
        for _ in 0..(length/chunk_size).max(1){
            let etr = (length - readen).min(chunk_size) as u64; //elements to read
            let offset : u64 = self.headers_offset + (readen * element_size);
            readen += etr;
            let mut buffer = vec![0u8;(element_size*etr) as usize];
            fi.read_exact_at(&mut buffer, offset)?;
            for j in buffer.chunks_exact(self.element_size){
                map.push(j != empty)
            }
            drop(buffer); 
        }
        map.shrink_to_fit();
        let mut cursor : usize = 0;
        let mut back_c : usize = map.len()-1;
        let len = map.len();
        let mut run = false; // false ~ forward | true ~ backwards
        
        while cursor < back_c{
            if run == false{
                if let Some(val) = map.get(cursor){
                    if !*val{
                        run = true;
                    }else{
                        cursor += 1;
                    }
                }
            }else if run == true{
                if let Some(val) = map.get(back_c){
                    if *val{
                        pairs.push((cursor as u64, back_c as u64));
                        if pairs.len() > MAX_VACUUM_LENGTH{
                            break;
                        }
                        run = false;
                    }else{
                        back_c = back_c.saturating_sub(1);
                    }
                }
            }
        }
        let mut indexing = self.index_map.lock().await;
        for (dead, alive) in pairs{
            let mut buffer = vec![0u8;self.element_size];
            let alive_offset = (alive*element_size) + self.headers_offset;
            fi.read_exact_at(&mut buffer,alive_offset)?;
            let row_pk = self.deserialize_row(&buffer).await?[0].clone();
            let dead_offset = (dead*element_size)+ self.headers_offset;
            fi.write_all_at(&buffer, dead_offset)?;
            fi.write_all_at(&vec![255u8;self.element_size], alive_offset)?;
            indexing.insert(get_index(row_pk),dead_offset)?;
            fi.sync_all()?;
            indexing.sync()?;
            map.swap(dead as usize, alive as usize);
        }
            
        let mut rows_to_remove = 0u64;
        let mut index = map.len()-1;
        while let Some(val) = map.get(index){
                if *val{break;}else{rows_to_remove+=1;if index==0{break;};index-=1;}
        }

        if rows_to_remove > 0{
            let new_len = fi.metadata()?.size().saturating_sub(rows_to_remove*element_size).max(self.headers_offset);
            fi.set_len(new_len)?;
            fi.sync_all()?;
        }


        
        Ok(())
    }
    pub async fn load_mvcc(&mut self) -> Result<(),Error>{
        let mut mvcc_record = self.mvcc_record.lock().await;
        let b = mvcc_record.yield_().await?;
        let mut mvcc = self.mvcc.lock().await;
        for i in b.chunks_exact(1 + self.element_size){
            let s = match i[0] {0 => MvccState::Insert,1 => MvccState::Edit,_ => MvccState::Delete};
            let row = self.deserialize_row(&i[1..self.element_size]).await?;
            let key = {
                let mut load = [0u8;8];
                load[..].copy_from_slice(&i[self.element_size..]);
                u64::from_le_bytes(load)
            };
            mvcc.0.insert(key, (s,row));
        }
        Ok(())
    }
    pub async fn record_mvcc(&mut self, key : u64, data : Vec<AlbaTypes>,state: MvccState) -> Result<(),Error>{
        let mut b = Vec::new();
        b.push(match state{MvccState::Delete => 2, MvccState::Insert => 0, MvccState::Edit => 1});
        b.extend_from_slice(&self.serialize_row(&data)?);
        b.extend_from_slice(&key.to_le_bytes());
        let mut l = self.mvcc_record.lock().await;
        l.put(b).await?;
        Ok(())
    }
    pub async fn push_row(&mut self, data : Vec<AlbaTypes>) -> Result<(),Error>{
        let mut indexing = self.index_map.lock().await;
        let i = get_index(data[0].clone());
        if indexing.get(i)?.is_some(){
            return Err(Error::new(ErrorKind::AddrInUse,"This primary key is in use, they must be always unique."))
        }
        drop(indexing);
        let ind = self.get_next_addr().await?;
        let mut mvcc_guard = self.mvcc.lock().await;
        //println!("PUSH_ROW - OFFSET : {}",ind);
        let d = data.clone();
        mvcc_guard.0.insert(ind, (MvccState::Insert,data));
        drop(mvcc_guard);
        let _ = self.record_mvcc(ind, d, MvccState::Insert).await;
        Ok(())
    }
    pub async fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.lock().await;
        mvcc_guard.0.clear();
        mvcc_guard.1.clear();
        let mut mvcc_rec = self.mvcc_record.lock().await;
        let _ = mvcc_rec.clear().await;
        drop(mvcc_guard);
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), Error> {
        //let mut virtual_ward : HashMap<usize, DataReference> = HashMap::new();
        let mut mvcc = self.mvcc.lock().await;
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut edits:Vec<(u64,Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc.0.iter() {
            
            let v = (*index, value.1.clone());
            match value.0{
                MvccState::Delete => deletes.push(v),
                MvccState::Insert => insertions.push(v),
                MvccState::Edit => edits.push(v)
            }
        }
        mvcc.0.clear();
        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);

        let mut writting : Vec<(u64,Vec<u8>)> = Vec::new();
        let schema = self.columns();
        //println!("schema {:?}",schema);
        let mut index_batch : Vec<(AlbaTypes,u64)> = Vec::new();
        for (row_index, mut row_data) in insertions {
            //println!("\nrow_data: {:?}\n",row_data);
            into_schema(&mut row_data, &schema)?;
            let serialized = self.serialize_row(&row_data).unwrap();
            index_batch.push((row_data[0].clone(),row_index));
            let offset = row_index;
            writting.push((offset,serialized));
        }
        let mut indexing = self.index_map.lock().await;
        for (row_index, mut row_data) in edits{
            //println!("\nrow_data: {:?}\n",row_data);
            into_schema(&mut row_data, &schema)?;
            let serialized = self.serialize_row(&row_data).unwrap();
            let key = get_index(row_data[0].clone());
            indexing.remove(key)?;
            index_batch.push((row_data[0].clone(),row_index));
            let offset = row_index;
            writting.push((offset,serialized)); 
        }

        drop(schema);


        let buf = vec![255u8; self.element_size];
        let mut gy = self.graveyard.lock().await;
        let mut gyl = gy.len();
        for del in &deletes {
            let offset = del.0;
            if gyl < MAX_GRAVEYARD_LENGTH_IN_MEMORY{
                gy.insert(offset);
                gyl += 1;
            }
            let key = get_index(del.1[0].clone());

            indexing.remove(key)?;
            writting.push((offset,buf.clone()));
        }
       
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
;
        let f = self.file.lock().await;
        let c = f.as_raw_fd();

        for (alb,off) in index_batch{
            let key = get_index(alb);
            indexing.insert(key,off)?;    
        };
        indexing.sync()?; 

        for l in l.chunks(3000){
            let l_1 = l.len();
            batch_write_data(l.to_vec(), l_1, c).await;
        }

        
        
        let mut mvcc_record = self.mvcc_record.lock().await;
        mvcc_record.clear().await?;
        mvcc.1.clear(); mvcc.0.clear(); 
        Ok(())
    }
    
    pub fn columns(&self) -> Vec<AlbaTypes>{
        self.headers.iter().map(|v|v.1.clone()).collect()
    }
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        for i in row{
            i.serialize_into(&mut buffer);
        }
        //println!("data: {:?}",buffer);
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
