use std::fs::{self,File};
use std::io::{Error, IoSliceMut, Read, Write,ErrorKind};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::os::fd::AsRawFd;
use blake3::Hasher;

const SIZE_RAISE_FACTOR : u64 = 2;
const MARGIN : f64 = 0.55;
const DEFAULT_SIZE : u64 = 16777216;
const READ_CHUNK_SIZE : usize = 4096;

#[derive(Debug)]
pub struct IndexingHashMap{
    file : File,
    name : String,
    size : u64,
    len : u64
}

struct Cell{
    key : u64,
    value : u64,
    next : u64,
    some : bool,
    some_next: bool,
}
impl Cell{
    fn new(key : u64, value : u64) -> Cell{
        Cell{
            key,
            value,
            next: 0,
            some: true,
            some_next: false,
        }
    }
    
    fn empty() -> Cell {
        Cell {
            key: 0,
            value: 0,
            next: 0,
            some: false,
            some_next: false,
        }
    }
    
    fn serialize(&self) -> [u8;25]{
        let mut byteload = [0u8;25];
        byteload[0..8].copy_from_slice(&self.key.to_le_bytes());
        byteload[8..16].copy_from_slice(&self.value.to_le_bytes());
        byteload[16..24].copy_from_slice(&self.next.to_le_bytes());
        byteload[24] = match (self.some,self.some_next){
            (false,false) => 0,
            (true,false) => 1,
            (false,true) => 2,
            (true,true) => 3
        };
        byteload
    }

    fn deserialize(byteload : &[u8;25]) -> Cell{
        let mut key_load : [u8;8] = [0u8;8];
        let mut value_load : [u8;8] = [0u8;8];
        let mut next_load : [u8;8] = [0u8;8];
        let mut s : [u8;1] = [0u8;1];

        key_load[0..8].copy_from_slice(&byteload[0..8]);
        value_load[0..8].copy_from_slice(&byteload[8..16]);
        next_load[0..8].copy_from_slice(&byteload[16..24]);
        s[0] = byteload[24];

        let key : u64 = u64::from_le_bytes(key_load);
        let value : u64 = u64::from_le_bytes(value_load);
        let next : u64 = u64::from_le_bytes(next_load);
        let (some,some_next) = match s[0]{
            0 => (false,false),
            1 => (true,false),
            2 => (false,true),
            3 => (true,true),
            _ => (false,false)
        };
        Cell{
            key,
            value,
            next,
            some,
            some_next
        }
    }
}

impl IndexingHashMap{
    fn new(name : String) -> Result<Self,Error>{
        IndexingHashMap::ne(name,DEFAULT_SIZE)
    }
    fn ne(name : String,chain_size : u64) -> Result<Self,Error>{
        let exists : bool = fs::exists(format!("{}.hashmap",name))?;
        Ok(if exists{
            let mut file = fs::File::open(format!("{}.hashmap",name))?;
            let mut buffer : [u8;16] = [0u8;16];
            file.read_exact(&mut buffer)?;

            let buf0 : [u8;8] = {
                let mut b = [0u8;8];
                b[0..8].copy_from_slice(&buffer[0..8]);
                b
            };
            let buf1 : [u8;8] = {
                let mut b = [0u8;8];
                b[0..8].copy_from_slice(&buffer[8..16]);
                b
            };
            IndexingHashMap{size:u64::from_le_bytes(buf0),len:u64::from_le_bytes(buf1),file,name}
        }else{
            let mut file = fs::File::create_new(format!("{}.hashmap",name))?;  
            let num: u64 = chain_size;
            let mut buffer: [u8; 16] = [0; 16];
            buffer[0..8].copy_from_slice(&num.to_le_bytes());
            
            
            let mut fully_template : Vec<u8> = vec![0u8;(25*DEFAULT_SIZE) as usize];
            fully_template[0..16].copy_from_slice(&buffer);
            file.write_all(&fully_template)?;
            IndexingHashMap { file, size: DEFAULT_SIZE, len: 0 ,name}
        })
    }
    fn should_rebucket(&self) -> bool{
        self.len > (self.size as f64 * MARGIN) as u64 
    }
    fn hash_key(&self,key : u64) -> u64{
        let mut hasher = Hasher::new();
        hasher.update(&key.to_le_bytes());
        let mut buf : [u8;8] = [0u8;8];
        hasher.finalize_xof().fill(&mut buf);
        u64::from_le_bytes(buf) % self.size
    }
    fn read_cell(&mut self,idx : u64) -> Result<Cell,Error>{
        let mut array = [0u8;25];
        self.file.read_exact_at(&mut array,16 + idx * 25)?;
        Ok(Cell::deserialize(&array))
    }

    fn ffree_cell(&mut self) -> Result<u64, Error> {
        let total_records = self.size as usize;
        let records_per_chunk = 163;  
        let bytes_per_chunk = records_per_chunk * 25;  
    
        let mut records_processed = 0;
    
        while records_processed < total_records {
           let remaining_records = total_records - records_processed;
           let records_this_chunk = records_per_chunk.min(remaining_records);
           let bytes_this_chunk = records_this_chunk * 25;
        
           let mut buf = vec![0u8; bytes_this_chunk];
           let file_offset = 16 + (records_processed * 25);
        
           self.file.read_exact_at(&mut buf, file_offset as u64)?;
        
           for (j, record) in buf.chunks_exact(25).enumerate() {
                if record[24] == 0u8 {
                    return Ok((records_processed + j) as u64);
                }
            }
        
            records_processed += records_this_chunk;
        }
    
        Err(Error::new(ErrorKind::Other, "No free cell found"))
    }


    fn write_cell(&mut self, idx : u64, cell : Cell) -> Result<(),Error>{
        self.file.write_all_at(&cell.serialize(), 16 + (idx * 25))?;
        Ok(())
    }
    
    fn rebucket(&mut self) -> Result<(),Error>{
        if self.should_rebucket(){
            
            let mut hm = IndexingHashMap::ne(format!("{}.temp",self.name), self.size * SIZE_RAISE_FACTOR)?;
            for i in 0..self.size {
                let cell = self.read_cell(i)?;
                if cell.some {
                    hm.insert(cell.key, cell.value)?;
                    let mut current_idx = i;
                    while {
                        let current_cell = self.read_cell(current_idx)?;
                        if current_cell.some_next {
                            current_idx = current_cell.next;
                            let next_cell = self.read_cell(current_idx)?;
                            if next_cell.some {
                                hm.insert(next_cell.key, next_cell.value)?;
                            }
                            current_cell.some_next
                        } else {
                            false
                        }
                    } {}
                }
            }
            
            drop(self.file);
            fs::remove_file(format!("{}.hashmap", self.name))?;
            fs::rename(format!("{}.temp.hashmap", self.name), format!("{}.hashmap", self.name))?;
            
            self.size = hm.size;
            self.len = hm.len;
            self.file = fs::File::options()
                .read(true)
                .write(true)
                .open(format!("{}.hashmap", self.name))?;
        }
        Ok(())
    }
    
    pub fn get(&mut self, key: u64) -> Result<Option<u64>, Error> {
        let mut idx = self.hash_key(key);
        
        loop {
            let cell = self.read_cell(idx)?;
            
            if !cell.some {
                return Ok(None);
            }
            
            if cell.key == key {
                return Ok(Some(cell.value));
            }
            
            if cell.some_next {
                idx = cell.next;
            } else {
                return Ok(None);
            }
        }
    }
    
    pub fn remove(&mut self, key: u64) -> Result<Option<u64>, Error> {
        let hash_idx = self.hash_key(key);
        let mut idx = hash_idx;
        let mut prev_idx: Option<u64> = None;
        
        loop {
            let cell = self.read_cell(idx)?;
            
            if !cell.some {
                return Ok(None);
            }
            
            if cell.key == key {
                let old_value = cell.value;
                
                if cell.some_next {
                    let next_cell = self.read_cell(cell.next)?;
                    let mut replacement = next_cell;
                    replacement.some_next = cell.some_next;
                    replacement.next = cell.next;
                    
                    if next_cell.some_next {
                        replacement.next = next_cell.next;
                    } else {
                        replacement.some_next = false;
                    }
                    
                    self.write_cell(idx, replacement)?;
                    
                    self.write_cell(cell.next, Cell::empty())?;
                } else {
                    if let Some(prev) = prev_idx {
                        let mut prev_cell = self.read_cell(prev)?;
                        prev_cell.some_next = false;
                        prev_cell.next = 0;
                        self.write_cell(prev, prev_cell)?;
                    }
                    
                    self.write_cell(idx, Cell::empty())?;
                }
                
                self.len -= 1;
                self.update_headers()?;
                return Ok(Some(old_value));
            }
            
            if cell.some_next {
                prev_idx = Some(idx);
                idx = cell.next;
            } else {
                return Ok(None);
            }
        }
    }
    
    pub fn insert(&mut self, key : u64, value : u64) -> Result<(),Error>{
        if self.should_rebucket() {
            self.rebucket()?;
        }
        
        let mut idx = self.hash_key(key);
        loop{
            let mut c = self.read_cell(idx)?; 
            if !c.some || c.key == key{
                let was_empty = !c.some;
                self.write_cell(idx, Cell::new(key,value))?;
                if was_empty {
                    self.len += 1;
                    self.update_headers()?;
                }
                return Ok(())
            }else if c.some_next{
                idx = c.next;
            }else{
                let f = self.ffree_cell()?;
                c.next = f;
                c.some_next = true;
                self.write_cell(idx, c)?;
                idx = f;
            }
        }
    }
    pub fn update_headers(&mut self) -> Result<(),Error>{
        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&self.size.to_le_bytes());
        header[8..16].copy_from_slice(&self.len.to_le_bytes());
        self.file.write_all_at(&header, 0)?;
        Ok(())
    }
}
