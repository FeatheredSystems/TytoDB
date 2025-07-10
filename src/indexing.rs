use std::{collections::hash_map::DefaultHasher, fs::{self, File, OpenOptions}, hash::{Hash, Hasher}, io::Error, os::unix::fs::FileExt, path::Path};

const BUCKET_CAPACITY : u64 = 4096;
const BUCKET_SIZE : u64 = 73728; // 4096 cells * 18 bytes/cell

#[derive(PartialEq, Debug)]
enum CellState {
    Empty,
    Occupied,
    Deleted,
}

impl CellState {
    fn from_bytes(bytes: [u8; 2]) -> Self {
        match u16::from_le_bytes(bytes) {
            1 => CellState::Occupied,
            2 => CellState::Deleted,
            _ => CellState::Empty,
        }
    }

    fn to_bytes(&self) -> [u8; 2] {
        match self {
            CellState::Empty => 0u16.to_le_bytes(),
            CellState::Occupied => 1u16.to_le_bytes(),
            CellState::Deleted => 2u16.to_le_bytes(),
        }
    }
}

struct Cell {
    key : u64,
    value : u64,
    state : CellState
}

impl Cell{
    fn from_bytes(byte : [u8;18]) -> Cell{
        let key     = {let mut load = [0u8;8];load[0..8].copy_from_slice(&byte[..8]);u64::from_le_bytes(load)};
        let value   = {let mut load = [0u8;8];load[0..8].copy_from_slice(&byte[8..16]);u64::from_le_bytes(load)};
        let state  = CellState::from_bytes({let mut load = [0u8;2];load[0..2].copy_from_slice(&byte[16..]);load});
        Cell{key,value,state}
    }
    fn as_bytes(&self) -> [u8; 18] {
        let mut bytes = [0u8; 18];
        bytes[0..8].copy_from_slice(&self.key.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.value.to_le_bytes());
        bytes[16..18].copy_from_slice(&self.state.to_bytes());
        bytes
    }
}
#[derive(Debug)]
pub struct Hashmap{
    length : u64,
    bucket_count : u64,
    file : File,
    path: String,
}
impl Hashmap{
    pub fn new(path : String) -> Result<Self,Error> {
        let filepath = format!("{}.hashmap", &path);
        if !Path::new(&filepath).exists(){
            let f = fs::File::create_new(&filepath)?;
            f.set_len(8+BUCKET_SIZE)?;
            f.write_all_at(&0u64.to_le_bytes(), 0)?;
            return Ok(Hashmap { length: 0, bucket_count: 1, file:f, path})
        }
        let file = OpenOptions::new().read(true).write(true).open(filepath)?;
        let length = {
            let mut load = [0u8;8];
            file.read_exact_at(&mut load, 0)?;
            u64::from_le_bytes(load)
        };
        let file_size = file.metadata()?.len();
        let bucket_count = (file_size - 8) / BUCKET_SIZE;
        Ok(Hashmap { length, bucket_count, file, path})
    }

    fn h(&self,key:u64) -> u64{let mut h=DefaultHasher::new();key.hash(&mut h);h.finish()}

    pub fn get_initial_ptr(&self, key: u64) -> (u64, u64) {
        let h = self.h(key);
        let bucket_index = h % self.bucket_count;
        let bucket_start_ptr = 8 + bucket_index * BUCKET_SIZE;

        let cell_index_in_bucket = self.h(h) % BUCKET_CAPACITY;
        let cell_ptr = bucket_start_ptr + cell_index_in_bucket * 18;

        (cell_ptr, bucket_start_ptr)
    }

    pub fn insert(&mut self, key : u64, value : u64) -> Result<(),Error>{
        if self.length * 100 / (self.bucket_count * BUCKET_CAPACITY) > 70 {
            self.rebucket()?;
        }

        let (start_ptr, bucket_start_ptr) = self.get_initial_ptr(key);
        let mut ptr = start_ptr;
        let mut tombstone_ptr = None;

        loop {
            let mut bin = [0u8;18];
            self.file.read_exact_at(&mut bin, ptr)?;
            let cell = Cell::from_bytes(bin);

            if cell.state == CellState::Deleted && tombstone_ptr.is_none() {
                tombstone_ptr = Some(ptr);
            }

            if cell.state == CellState::Empty || (cell.state == CellState::Deleted && tombstone_ptr.is_some()) {
                let write_ptr = tombstone_ptr.unwrap_or(ptr);
                let new_cell = Cell { key, value, state: CellState::Occupied };
                self.file.write_all_at(&new_cell.as_bytes(), write_ptr)?;
                self.length += 1;
                return Ok(());
            }

            if cell.state == CellState::Occupied && cell.key == key {
                let new_cell = Cell { key, value, state: CellState::Occupied };
                self.file.write_all_at(&new_cell.as_bytes(), ptr)?;
                return Ok(());
            }

            ptr += 18;
            if ptr >= bucket_start_ptr + BUCKET_SIZE {
                ptr = bucket_start_ptr;
            }
            if ptr == start_ptr {
                return Err(Error::new(std::io::ErrorKind::Other, "Bucket is full, rebucket failed"));
            }
        }
    }

    pub fn get(&mut self, key : u64) -> Result<Option<u64>,Error>{
        let (start_ptr, bucket_start_ptr) = self.get_initial_ptr(key);
        let mut ptr = start_ptr;

        loop {
            let mut bin = [0u8;18];
            self.file.read_exact_at(&mut bin, ptr)?;
            let cell = Cell::from_bytes(bin);

            if cell.state == CellState::Empty {
                return Ok(None);
            }

            if cell.state == CellState::Occupied && cell.key == key {
                return Ok(Some(cell.value));
            }

            ptr += 18;
            if ptr >= bucket_start_ptr + BUCKET_SIZE {
                ptr = bucket_start_ptr;
            }
            if ptr == start_ptr {
                return Ok(None);
            }
        }
    }

    pub fn remove(&mut self, key: u64) -> Result<bool, Error> {
        let (start_ptr, bucket_start_ptr) = self.get_initial_ptr(key);
        let mut ptr = start_ptr;

        loop {
            let mut bin = [0u8; 18];
            self.file.read_exact_at(&mut bin, ptr)?;
            let cell = Cell::from_bytes(bin);

            if cell.state == CellState::Empty {
                return Ok(false);
            }

            if cell.state == CellState::Occupied && cell.key == key {
                let new_cell = Cell { key: 0, value: 0, state: CellState::Deleted };
                self.file.write_all_at(&new_cell.as_bytes(), ptr)?;
                self.length -= 1;
                return Ok(true);
            }

            ptr += 18;
            if ptr >= bucket_start_ptr + BUCKET_SIZE {
                ptr = bucket_start_ptr;
            }
            if ptr == start_ptr {
                return Ok(false);
            }
        }
    }

    pub fn rebucket(&mut self) -> Result<(), Error> {
        let temp_path_str = format!("{}.temp", self.path);
        let _ = fs::remove_file(format!("{}.hashmap", &temp_path_str));
        let mut new_hm = Hashmap::new(temp_path_str.clone())?;

        let new_bucket_count = self.bucket_count * 10;
        let new_len = 8 + new_bucket_count * BUCKET_SIZE;
        new_hm.file.set_len(new_len)?;
        new_hm.bucket_count = new_bucket_count;

        let old_file_len = self.file.metadata()?.len();
        let mut read_ptr = 8;
        
        loop {
            let mut cell_buffer = [0u8; 18];
            if read_ptr + 18 > old_file_len {
                break;
            }
            self.file.read_exact_at(&mut cell_buffer, read_ptr)?;
            let cell = Cell::from_bytes(cell_buffer);
            if cell.state == CellState::Occupied {
                new_hm.insert(cell.key, cell.value)?;
            }
            read_ptr += 18;
        }

        new_hm.sync()?;

        let old_filepath = format!("{}.hashmap", self.path);
        let temp_filepath = format!("{}.hashmap", temp_path_str);

        self.file = new_hm.file;
        self.bucket_count = new_hm.bucket_count;
        self.length = new_hm.length;

        fs::remove_file(&old_filepath)?;
        fs::rename(temp_filepath, &old_filepath)?;
        
        self.file = OpenOptions::new().read(true).write(true).open(&old_filepath)?;

        Ok(())
    }


    pub fn sync(&mut self) -> Result<(),Error>{
        self.file.write_all_at(&self.length.to_le_bytes(), 0)?;
        self.file.sync_all()
    }
}

