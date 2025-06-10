use tokio::sync::{Mutex, RwLock};
use crate::{alba_types::AlbaTypes, database::database_path, gerr, logerr, loginfo};
use std::{collections::{BTreeMap, BTreeSet, HashMap}, fs::{self, File, OpenOptions}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, Write}, ops::{Range, RangeInclusive}, os::unix::fs::{FileExt, MetadataExt}, sync::Arc, time::Duration};


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
    file : Arc<Mutex<File>>,
    metadata : Arc<Mutex<Vec<(RangeInclusive<u64>,u64)>>>,
    available_page : Arc<Mutex<usize>>
}
#[derive(Debug)]
struct IndexPage{
    count : u16,
    range : RangeInclusive<u64>,
    elements : Vec<(u64,u64)>
}
fn index_page_from_b(b : [u8;PAGE_SIZE as usize]) -> IndexPage{
    let mut min_be_bytes = [0u8;8];
    let mut max_be_bytes = [0u8;8];
    let mut count_be_bytes = [0u8;2];

    min_be_bytes.clone_from_slice(&b[0..8]);
    max_be_bytes.clone_from_slice(&b[8..16]);
    count_be_bytes.clone_from_slice(&b[16..18]);
    
    let min = u64::from_be_bytes(min_be_bytes);
    let max = u64::from_be_bytes(max_be_bytes);
    let count = u16::from_be_bytes(count_be_bytes);

    let mut elements : Vec<(u64,u64)> = Vec::new();
    let a = &b[18..18+(count*16) as usize];

    for i in a.chunks_exact(16){
        let mut key = [0u8;8];
        let mut address = [0u8;8];
        key.clone_from_slice(&i[..8]);
        address.clone_from_slice(&i[..8]);
        elements.push((u64::from_be_bytes(key),u64::from_be_bytes(address)))
    }

    IndexPage{count,range:min..=max,elements}
}
fn index_page_to_b(page: &IndexPage) -> [u8; PAGE_SIZE as usize] {
    let mut b = [0u8; PAGE_SIZE as usize];
    
    let min_bytes = page.range.start().to_be_bytes();
    b[0..8].copy_from_slice(&min_bytes);
    
    let max_bytes = page.range.end().to_be_bytes();
    b[8..16].copy_from_slice(&max_bytes);
    
    let count_bytes = page.count.to_be_bytes();
    b[16..18].copy_from_slice(&count_bytes);
    
    let mut offset = 18;
    for (key, address) in &page.elements {
        let key_bytes = key.to_be_bytes();
        let address_bytes = address.to_be_bytes();
        
        b[offset..offset + 8].copy_from_slice(&key_bytes);
        b[offset + 8..offset + 16].copy_from_slice(&address_bytes);
        
        offset += 16;
    }
    
    b
}

const PAGE_SIZE : u64 = 102226;
const ELEMENT_COUNT : u16 = 6388;

fn new_empty_page() -> [u8; 102226]{
    let count : u16 = 0;
    let min : u64 = 0;
    let max : u64 = 0;
    let mut buffer: [u8; 102226] = [0u8;PAGE_SIZE as usize];
    buffer[..8].clone_from_slice(&min.to_be_bytes());
    buffer[8..16].clone_from_slice(&max.to_be_bytes());
    buffer[16..18].clone_from_slice(&count.to_be_bytes());
    buffer
}
impl Indexing{
    pub async fn create_index(container_name : &String) -> Result<(),Error>{
        let path = format!("{}/{}.index",database_path(),container_name);
        if fs::exists(&path)?{
            return Ok(())
        }else{
            let file = fs::File::create_new(path)?;
            file.write_all_at(&mut new_empty_page(), 0)?;
            file.sync_all()?;
        }
        Ok(())
    }
    
    pub async fn load_index(container_name : &String) -> Result<Arc<Self>,Error>{
        Indexing::create_index(container_name).await?;
        let path = format!("{}/{}.index",database_path(),container_name);
        let file = File::options().read(true).write(true).open(path)?;
        let size = file.metadata()?.size();
        let pages = size.saturating_div(PAGE_SIZE);
        let mut metadata : Vec<(RangeInclusive<u64>,u64)> = Vec::new();
        let mut available = 0;
        for i in 0..pages{
            let mut buf = [0u8;PAGE_SIZE as usize];
            file.read_exact_at(&mut buf, i*PAGE_SIZE)?;
            let page = index_page_from_b(buf);
            if page.count < 6388{
                available = i as usize;
            }
            metadata.push((page.range,i*PAGE_SIZE));
            
        }
        Ok(Arc::new(Indexing{file:Arc::new(Mutex::new(file)),metadata:Arc::new(Mutex::new(metadata)), available_page: Arc::new(Mutex::new(available))}))
    }
    pub async fn insert_index(&self,arg : u64, arg_offset : u64) -> Result<(),Error>{
        let available = {
            let r = self.available_page.lock().await;
            r.clone()
        };
        let mut metadata = self.metadata.lock().await;
        if let Some(val) = metadata.get(available){
            let offset = val.1;
            let file = self.file.lock().await;
            let mut buf = [0u8;PAGE_SIZE as usize];
            file.read_exact_at(&mut buf, offset)?;
            let mut page: IndexPage = index_page_from_b(buf);
            page.elements.push((arg,arg_offset));
            page.elements.sort_by_key(|f|f.0);
            page.count = page.elements.len() as u16;

            let (f,l) = (page.elements.first(),page.elements.last());
            if page.count > 0 && f.is_some() && l.is_some(){
                let (f,l) = (f.unwrap(),l.unwrap());
                if f.0 <= f.1{
                    page.range = f.0 ..=l.0
                }
            }
            let size = file.metadata()?.size();
            metadata[available] = (page.range.clone(),size.clone());
            let bytes = index_page_to_b(&page);
            file.write_all_at(&bytes, offset)?;
            if page.count == ELEMENT_COUNT{
                let i = metadata.len();
                let nep = new_empty_page();
                metadata.push((page.range,size));
                file.write_all_at(&nep, size)?;
                *self.available_page.lock().await = i;
            }
            drop(metadata);
            file.sync_all()?;
        }
        Ok(())
    }
    
    pub async fn remove_index(&self, arg: u64, arg_offset: u64) -> Result<(), Error> {
        let metadata = self.metadata.lock().await;
        let file = self.file.lock().await;
        
        for (page_idx, (range, offset)) in metadata.iter().enumerate() {
            if range.contains(&arg) || range.start() == &0 && range.end() == &0 {
                let mut buf = [0u8; PAGE_SIZE as usize];
                file.read_exact_at(&mut buf, *offset)?;
                let mut page: IndexPage = index_page_from_b(buf);
                
                let original_len = page.elements.len();
                page.elements.retain(|(key, offset_val)| !(*key == arg && *offset_val == arg_offset));
                
                if page.elements.len() < original_len {
                    page.count = page.elements.len() as u16;
                    
                    // Update range if necessary
                    if page.elements.is_empty() {
                        page.range = 0..=0;
                    } else {
                        let min = page.elements.iter().map(|(k, _)| *k).min().unwrap();
                        let max = page.elements.iter().map(|(k, _)| *k).max().unwrap();
                        page.range = min..=max;
                    }
                    
                    let bytes = index_page_to_b(&page);
                    file.write_all_at(&bytes, *offset)?;
                    
                    if page.count < ELEMENT_COUNT {
                        let mut available = self.available_page.lock().await;
                        if page_idx < *available || (*available >= metadata.len() && !metadata.is_empty()) {
                            *available = page_idx;
                        }
                    }
                    
                    file.sync_all()?;
                    return Ok(());
                }
            }
        }
        
        Ok(())
    }
    
}

impl Add for Indexing {
    async fn add(&self, arg: u64,arg_offset : u64) -> Result<(),Error> {
        self.insert_index(arg, arg_offset).await
    }
}

impl Remove for Indexing{
    async fn remove(&self, arg: u64,arg_offset : u64) -> Result<(),Error> {
        self.remove_index(arg, arg_offset).await
    }
}

impl Search<Range<u64>> for Indexing {
    async fn search(&self, arg: Range<u64>) -> Result<BTreeSet<u64>, Error> {
        let mut results = BTreeSet::new();
        let metadata = self.metadata.lock().await;
        let file = self.file.lock().await;
        
        for (_page_idx, (range, offset)) in metadata.iter().enumerate() {
            // Check if page range overlaps with search range
            if range.start() < &arg.end && range.end() >= &arg.start {
                let mut buf = [0u8; PAGE_SIZE as usize];
                file.read_exact_at(&mut buf, *offset)?;
                let page: IndexPage = index_page_from_b(buf);
                
                // Search within the page elements
                for (key, value) in &page.elements {
                    if arg.contains(key) {
                        results.insert(*value);
                    }
                }
            }
        }
        loginfo!("results: {:?}",results);
        Ok(results)
    }
}

impl Search<RangeInclusive<u64>> for Indexing {
    async fn search(&self, arg: RangeInclusive<u64>) -> Result<BTreeSet<u64>, Error> {
        let mut results = BTreeSet::new();
        let metadata = self.metadata.lock().await;
        let file = self.file.lock().await;
        
        for (_page_idx, (range, offset)) in metadata.iter().enumerate() {
            // Check if page range overlaps with search range
            if range.start() <= arg.end() && range.end() >= arg.start() {
                let mut buf = [0u8; PAGE_SIZE as usize];
                file.read_exact_at(&mut buf, *offset)?;
                let page: IndexPage = index_page_from_b(buf);
                
                // Search within the page elements
                for (key, value) in &page.elements {
                    if arg.contains(key) {
                        results.insert(*value);
                    }
                }
            }
        }
        
        loginfo!("results: {:?}",results);
        Ok(results)
    }
}

impl Search<u64> for Indexing {
    async fn search(&self, arg: u64) -> Result<BTreeSet<u64>, Error> {
        let mut results = BTreeSet::new();
        let metadata = self.metadata.lock().await;
        let file = self.file.lock().await;
        
        for (_page_idx, (range, offset)) in metadata.iter().enumerate() {
            // Check if page range contains the search key
            loginfo!("{:?} :: {}",range,arg);
            if range.contains(&arg) || (range.start() == &0 && range.end() == &0) {
                let mut buf = [0u8; PAGE_SIZE as usize];
                file.read_exact_at(&mut buf, *offset)?;
                let page: IndexPage = index_page_from_b(buf);
                loginfo!("page: {:?}",page);
                
                // Search within the page elements
                for (key, value) in &page.elements {
                    if *key == arg {
                        results.insert(*value);
                    }
                }
            }
        }
        
        loginfo!("results: {:?}",results);
        Ok(results)
    }
}

pub trait IndexHashing {
    fn index_hash(&self) -> u64;
}

impl IndexHashing for i64{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for i32{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for i16{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for i8{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for u128{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for u64{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for u32{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for u16{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for f64{
    fn index_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.to_bits().hash(&mut hasher);
        hasher.finish()
    }
}

impl IndexHashing for u8{
    fn index_hash(&self) -> u64 {
        *self as u64
    }
}

pub trait GetIndex{
    fn get_index(&self) -> u64;
}

impl GetIndex for i32{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for i64{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for i16{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for u128{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for u64{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for u32{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for u16{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for u8{
    fn get_index(&self) -> u64{
        self.index_hash()
    }
}

impl GetIndex for f64{
    fn get_index(&self) -> u64{
        self.index_hash()
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
        hasher.finish()
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
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            },
            AlbaTypes::SmallBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            },
            AlbaTypes::MediumBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            },
            AlbaTypes::BigSBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            },
            AlbaTypes::LargeBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            },
            AlbaTypes::NONE => 0,
        }
    }
}