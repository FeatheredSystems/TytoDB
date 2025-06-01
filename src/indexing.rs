use tokio::sync::RwLock;
use crate::{alba_types::AlbaTypes, database::database_path, gerr, logerr, loginfo};
use std::{collections::BTreeSet, fs::{self, File}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, Read}, ops::{RangeInclusive,Range}, os::unix::fs::{FileExt, MetadataExt}, sync::Arc, time::Duration};

const INDEX_CHUNK_SIZE : u64 = GERAL_DISK_CHUNK as u64;
const GERAL_DISK_CHUNK : usize = 4096;


//type IndexElement = (u64,u64); // index value , offset value
type MetadataElement = (u64,u64,u16); // minimum index value, maximum index value , items in chunk


pub trait Add{
    /// Insert a index value into indexes
    async fn add(&self, arg: u64,arg_offset : u64) -> Result<(),Error>; // direct index value
}
pub trait Remove{
    /// Remove a index value from indexes
    async fn remove(&self, arg: u64,arg_offset : u64) -> Result<(),Error>;
}
/// Types that can be used as search inputs.
pub trait SearchQuery {}
impl SearchQuery for std::ops::Range<u64> {}
impl SearchQuery for std::ops::RangeInclusive<u64> {}
impl SearchQuery for u64 {}
pub trait Search <T:SearchQuery>{
    /// Look for offset values from a range of indexes, index or IncludeRange of indexes
    /// Returns a BTreeSet with offsets of the matched rows in a container.
    async fn search(&self, arg:T) -> Result<BTreeSet<u64>,Error>;
}

#[derive(Debug)]
pub struct Indexing{
    indexes_file : Arc<RwLock<File>>,
    indexes_metadata_file : Arc<RwLock<File>>,
    metadata : Arc<RwLock<Vec<(u64,u64,u16)>>>,
    changes : Arc<RwLock<bool>>,
    destroyed : Arc<RwLock<bool>>
}
impl Indexing{
    pub async fn create_index(container_name : &String) -> Result<(),Error>{

        let ifp = format!("{}/{}.cindex",database_path(),container_name);
        let mtp = format!("{}/{}.cimeta",database_path(),container_name);
        if match fs::exists(&ifp){Ok(a)=>a,Err(e)=>{logerr!("{}",e);return Err(e);}} 
        || match fs::exists(&mtp){Ok(a)=>a,Err(e)=>{logerr!("{}",e);return Err(e);}}{
            return Ok(())
        }

        File::create_new(ifp)?;
        File::create_new(mtp)?;
        Ok(())
    }
    pub async fn load_index(container_name : &String) -> Result<Arc<Self>,Error>{
        Indexing::create_index(container_name).await?;


        // "ci" stands for container index
        let ifp = format!("{}/{}.cindex",database_path(),container_name);
        let mtp = format!("{}/{}.cimeta",database_path(),container_name);

        if !match fs::exists(&ifp){Ok(a)=>a,Err(e)=>{logerr!("{}",e);return Err(e);}} 
        || !match fs::exists(&mtp){Ok(a)=>a,Err(e)=>{logerr!("{}",e);return Err(e);}}{
            return Err(gerr("One of the indexing files are missing"))
        }

        let indexes_file = File::open(&ifp)?;
        let mut metadata_file = File::open(&mtp)?;

        let index_metadata  = {
            let size = metadata_file.metadata()?.size() as usize;
            let mut buffer = vec![0u8;size];
            metadata_file.read_to_end(&mut buffer)?;
            let mut elements : Vec<MetadataElement> = Vec::with_capacity(size/18);
            for i in buffer.chunks_exact(18){
                let minimum_index_value = u64::from_be_bytes(i[0..8].try_into().unwrap());
                let maximum_index_value = u64::from_be_bytes(i[8..16].try_into().unwrap());
                let length_of_chunk     = u16::from_be_bytes(i[16..18].try_into().unwrap());
                elements.push((minimum_index_value,maximum_index_value,length_of_chunk));
            }
            elements
        };
        let me = Arc::new(Indexing { indexes_file: Arc::new(RwLock::new(indexes_file)), indexes_metadata_file: Arc::new(RwLock::new(metadata_file)), metadata: Arc::new(RwLock::new(index_metadata)), changes: Arc::new(RwLock::new(false)), destroyed:Arc::new(RwLock::new(false)) });
        let virt_me = me.clone();
        tokio::spawn(async move{
            let me = virt_me;
            loop{
                tokio::time::sleep(Duration::from_secs(500)).await;
                if *me.destroyed.read().await{
                    break;
                }
                let c = me.changes.read().await;
                if *c{
                    drop(c);
                    let mut file = me.indexes_file.read().await;
                    let _ = file.sync_data();
                    drop(file);
                    file = me.indexes_metadata_file.read().await;
                    let _ = file.sync_data();
                    *me.changes.write().await = false;
                }
            }
        });
        Ok(me)
    }
    pub async fn create_index_chunk(&self,arg : u64,arg_offset : u64) -> Result<(),Error>{
        let mut metadata = self.metadata.write().await;
        let metadata_file = self.indexes_metadata_file.write().await;
        let index_file = self.indexes_file.write().await;
        metadata.push((arg.clone(),arg+INDEX_CHUNK_SIZE,1));
        
        let metadata_size = metadata_file.metadata()?.size();
        metadata_file.set_len(metadata_size + 18)?;
        let value = (arg.clone().to_be_bytes(),(arg+INDEX_CHUNK_SIZE as u64).to_be_bytes(),(1 as u16).to_be_bytes());
        let mut buffer = [0u8; 18];
        buffer[0..8].copy_from_slice(&value.0);
        buffer[8..16].copy_from_slice(&value.1);
        buffer[16..18].copy_from_slice(&value.2);
        metadata_file.write_all_at(&mut buffer, metadata_size)?;
        let _ = buffer;
        let _ = metadata_size;
        
        let index_file_size = index_file.metadata()?.size();
        index_file.set_len(index_file_size+INDEX_CHUNK_SIZE * 16)?;
        let mut buffer = [0u8;(INDEX_CHUNK_SIZE*16) as usize];
        let mut ib = [0u8;16];
        ib[..8].copy_from_slice(&arg.to_be_bytes());
        ib[8..].copy_from_slice(&arg_offset.to_be_bytes());
        buffer[..16].copy_from_slice(&ib);
        index_file.write_all_at(&mut buffer,index_file_size)?;
        *self.changes.write().await = true;
        Ok(())
    }
    pub async fn insert_index(&self,arg : u64, arg_offset : u64,meta : (usize, (u64, u64, u16))) -> Result<(),Error>{
        let meta_file = self.indexes_metadata_file.write().await;
        let index_file = self.indexes_file.write().await;
        
        let mut index_buff = [0u8;16];
        let mut meta_buff = [0u8;18];
        index_buff[..8].copy_from_slice(&arg.to_be_bytes());
        index_buff[8..].copy_from_slice(&arg_offset.to_be_bytes());
        meta_buff[..8].copy_from_slice(&meta.1.0.to_be_bytes());
        meta_buff[8..16].copy_from_slice(&meta.1.1.to_be_bytes());
        meta_buff[16..].copy_from_slice(&meta.1.2.to_be_bytes());
        index_file.write_all_at(&index_buff, (meta.0 as u64 * INDEX_CHUNK_SIZE * 16) + (meta.1.2 as u64 * 16))?;
        meta_file.write_all_at(&meta_buff, meta.0 as u64*18)?;
        drop(index_file);
        drop(meta_file);
        *self.changes.write().await = true;
        Ok(())
    }
    pub async fn remove_index(&self,arg : u64,arg_offset : u64) -> Result<(),Error>{
        let mut metadata = self.metadata.write().await;
        let meta_file = self.indexes_metadata_file.write().await;
        let index_file = self.indexes_file.write().await;
        let mut slots = Vec::with_capacity(5);

        for (index,value) in metadata.iter_mut().enumerate(){
            if arg >= value.0 && arg <= value.1{
                slots.push((index,value));
            }
        }

        for (idx,v) in slots{
            let mut buffer = [0u8;INDEX_CHUNK_SIZE as usize * 16];
            let offset = idx * INDEX_CHUNK_SIZE as usize;
            index_file.read_exact_at(&mut buffer, offset as u64)?;
            let mut index_value_vector = Vec::with_capacity(INDEX_CHUNK_SIZE as usize);
            for i in buffer.chunks_exact(16){
                let index_value = u64::from_be_bytes(i[..8].try_into().unwrap());
                let offset_value = u64::from_be_bytes(i[8..].try_into().unwrap());
                if index_value == arg && arg_offset == offset_value{
                    continue;
                }else{
                    index_value_vector.push((index_value,offset_value))
                }
            }
            buffer = [0u8;INDEX_CHUNK_SIZE as usize * 16];
            for i in index_value_vector.iter().enumerate(){
                let index = i.0 * 16;
                buffer[index..index+8].copy_from_slice(&i.1.0.to_be_bytes());
                buffer[index+8..index+16].copy_from_slice(&i.1.1.to_be_bytes());
            }
            index_file.write_all_at(&mut buffer, INDEX_CHUNK_SIZE *(idx as u64 *16))?;
            v.2 -= 1;
            let mut metadata_buffer = [0u8;18];
            metadata_buffer[..8].copy_from_slice(&v.0.to_be_bytes());
            metadata_buffer[8..16].copy_from_slice(&v.1.to_be_bytes());
            metadata_buffer[16..].copy_from_slice(&v.2.to_be_bytes());
            meta_file.write_all_at(&mut metadata_buffer, idx as u64*18)?;
            *self.changes.write().await = true;
        }
        Ok(())
    }
}

impl Add for Indexing {
    async fn add(&self, arg: u64,arg_offset : u64) -> Result<(),Error> {
        let meta = {
            let mut metadata = self.metadata.write().await;
            let mt: (usize, (u64, u64, u16)) = {
                let mut index : (usize,(u64,u64,u16)) = (0,(0,0,0));
                let mut alloc = false;
                for (i,v) in metadata.iter_mut().enumerate(){
                    if v.2 < u16::MAX && arg <= v.1 && arg >= v.0{
                        v.2 += 1;
                        index = (i,v.to_owned());
                        alloc = true;
                        break;
                    }
                }
                if alloc{index}else{drop(metadata);return self.create_index_chunk(arg, arg_offset).await}
            };
            mt
        };
        self.insert_index(arg, arg_offset,meta).await
    }
}
impl Remove for Indexing{
    async fn remove(&self, arg: u64,arg_offset : u64) -> Result<(),Error> {
        self.remove_index(arg, arg_offset).await
    }
}
impl Search<Range<u64>> for Indexing {
    async fn search(&self, arg: Range<u64>) -> Result<BTreeSet<u64>, Error> {
        loginfo!("search<Range<u64>>");
        let mut groups = {
            let mut groups = Vec::new();
            let metadata = self.metadata.read().await;
            for (idx,i) in metadata.iter().enumerate(){
                if arg.contains(&i.0) && arg.contains(&i.1){
                    groups.push(idx);
                }
            }
            groups
        };
        groups.sort();

        let mut offsets : BTreeSet<u64> = BTreeSet::new();
        
        let indexes_file = self.indexes_file.read().await;
        for i in groups{
            let mut buffer = [0u8;INDEX_CHUNK_SIZE as usize * 16];
            indexes_file.read_exact_at(&mut buffer, i as u64*INDEX_CHUNK_SIZE*16)?;
            for chunk in buffer.chunks_exact(16){
                let index_value = u64::from_be_bytes(chunk[..8].try_into().unwrap());
                let index_offset = u64::from_be_bytes(chunk[8..].try_into().unwrap());
                if arg.contains(&index_value) {
                    offsets.insert(index_offset);
                }

            }
        }
        
        Ok(offsets)
    }
}

impl Search<RangeInclusive<u64>> for Indexing {
    async fn search(&self, arg: RangeInclusive<u64>) -> Result<BTreeSet<u64>, Error> {
        loginfo!("search<RangeInclusive<u64>>");
        let mut groups = {
            let mut groups = Vec::new();
            let metadata = self.metadata.read().await;
            let (start, end) = (*arg.start(), *arg.end());
            for (idx, i) in metadata.iter().enumerate() {
                if start <= i.1 && end >= i.0 {
                    groups.push(idx);
                }
            }
            groups
        };
        groups.sort();
        

        let indexes_file = self.indexes_file.read().await;
        let mut offsets : BTreeSet<u64> = BTreeSet::new();
        loginfo!("search-2");
        for i in groups{
            let mut buffer = [0u8;INDEX_CHUNK_SIZE as usize * 16];
            indexes_file.read_exact_at(&mut buffer, i as u64*INDEX_CHUNK_SIZE*16)?;
            for chunk in buffer.chunks_exact(16){
                let index_value = u64::from_be_bytes(chunk[..8].try_into().unwrap());
                let index_offset = u64::from_be_bytes(chunk[8..].try_into().unwrap());
                if arg.contains(&index_value) {
                    offsets.insert(index_offset);
                }

            }
        }
        Ok(offsets)
    }
}

impl Search<u64> for Indexing {
    async fn search(&self, arg: u64) -> Result<BTreeSet<u64>, Error> {
        loginfo!("search<u64>");
        let mut groups = {
            let mut groups = Vec::new();
            let metadata = self.metadata.read().await;
            for (idx, i) in metadata.iter().enumerate() {
                if i.1 >= arg && i.0 <= arg {
                    groups.push(idx);
                }
            }
            groups  
        };
        groups.sort();
        let mut offsets : BTreeSet<u64> = BTreeSet::new();
        

        let indexes_file = self.indexes_file.read().await;
        for i in groups{
            let mut buffer = [0u8;INDEX_CHUNK_SIZE as usize * 16];
            indexes_file.read_exact_at(&mut buffer, i as u64*INDEX_CHUNK_SIZE*16)?;
            for chunk in buffer.chunks_exact(16){
                let index_value = u64::from_be_bytes(chunk[..8].try_into().unwrap());
                let index_offset = u64::from_be_bytes(chunk[8..].try_into().unwrap());
                if arg == index_value {
                    offsets.insert(index_offset);
                }

            }
        }

        
        Ok(offsets)
    }
}


pub trait GetIndex{
    fn get_index(&self) -> u64;
}

impl GetIndex for i32{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for i64{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for i16{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for i128{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for u128{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for u64{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for u32{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for u16{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for u8{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl GetIndex for f64{
    fn get_index(&self) -> u64{
        if self.is_nan(){
            return 0
        }
        (self.abs() as u64) / INDEX_CHUNK_SIZE
    }
}
impl GetIndex for bool{
    fn get_index(&self) -> u64{
        if *self{
            return 1
        }
        0
    }
}
impl GetIndex for String{
    fn get_index(&self) -> u64{
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()/INDEX_CHUNK_SIZE
    }
}


impl GetIndex for AlbaTypes {
    fn get_index(&self) -> u64 {
        match self {
            AlbaTypes::Text(s) => s.get_index(),
            AlbaTypes::Int(i) => i.get_index(),
            AlbaTypes::Bigint(i) => i.get_index(),
            AlbaTypes::Float(f) => f.get_index(),
            AlbaTypes::Bool(b) => b.get_index(),
            AlbaTypes::Char(c) => (*c as u64).get_index(),
            AlbaTypes::NanoString(s) => s.get_index(),
            AlbaTypes::SmallString(s) => s.get_index(),
            AlbaTypes::MediumString(s) => s.get_index(),
            AlbaTypes::BigString(s) => s.get_index(),
            AlbaTypes::LargeString(s) => s.get_index(),
            AlbaTypes::NanoBytes(bytes) => {
                // For Vec<u8>, hash it and then get index
                use std::hash::{Hash, Hasher};
                use std::collections::hash_map::DefaultHasher;

                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::SmallBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::MediumBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::BigSBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::LargeBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::NONE => 0,
        }
    }
}
