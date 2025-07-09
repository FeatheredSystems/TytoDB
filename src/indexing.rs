use std::{fs::{self, File, OpenOptions}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, Read, Write}, os::unix::fs::FileExt};

use rand::{rngs::OsRng, TryRngCore};


pub fn orng() -> u64 {
    let mut r = OsRng;
    loop{
        if let Ok(oka) = r.try_next_u64(){
            return oka
        }
    }
}


// key | value | exists | next | next_ptr
type Cell = (u64,u64,bool,bool,u64);
fn gc(key : u64,value : u64) -> Cell {(key,value,true,false,0_u64) as Cell}
fn cb(c : &Cell) -> [u8;25]{
    let mut b = [0u8;25];
    b[0..8].copy_from_slice(&c.0.to_le_bytes());
    b[8..16].copy_from_slice(&c.1.to_le_bytes());
    b[16..24].copy_from_slice(&c.4.to_le_bytes());
    b[24] = match (c.2,c.3){
        (false,false) => 0,
        (true,false) => 1,
        (false,true) => 2,
        (true,true) => 3
    };
    b
}
fn fcb(c : [u8;25]) -> Cell{
    let key = {
        let mut l = [0u8;8];
        l[0..8].copy_from_slice(&c[0..8]);
        u64::from_le_bytes(l)
    };
    let value = {
        let mut l=[0u8;8];l[0..8].copy_from_slice(&c[8..16]);u64::from_le_bytes(l)
    };
    let ptr = {
        let mut l=[0u8;8];l[0..8].copy_from_slice(&c[16..24]);u64::from_le_bytes(l)
    };
    let b = match c[24]{0=>(false,false),1=>(true,false),2=>(false,true),3=>(true,true),_=>(true,true)};
    (key,value,b.0,b.1,ptr) as Cell
}
#[derive(Debug)]
pub struct Hashmap{
    size : u64,
    len : u64,
    file : File
}
impl Hashmap{
    pub fn new(name : String) -> Result<Self,Error>{
        let mut file = OpenOptions::new().read(true).write(true).create(!fs::exists(&name)?).open(&name)?;
        let size = file.metadata()?.len();
        if size == 0{
            file.set_len(8 + (25*4096))?;
            Ok(Hashmap{
                size : 4096,
                len : 0,
                file
            })
        }else{
            let len = {
                let mut load = [0u8;8];
                file.read_exact(&mut load)?;
                u64::from_le_bytes(load)
            };
            let size = (size - 8) /25;
            Ok(Hashmap { size, len, file })
        }
    }
    fn h(&self,k:u64) -> u64{
        let mut h = DefaultHasher::new();k.hash(&mut h);h.finish()%self.size
    }
    fn rc(&self,k:u64) -> Result<Cell,Error>{
        let mut b = [0u8;25];
        self.file.read_exact_at(&mut b, k)?;
        Ok(fcb(b))
    }
    fn ie(&self,k:u64) -> Result<bool,Error>{
        Ok(self.rc(k)?.2)
    }
    
    fn wc(&mut self,k : u64, c : Cell) -> Result<(),Error>{
        let b = cb(&c);
        self.file.write_all_at(&b, k)?;
        Ok(())
    }
    pub fn insert(&mut self,key : u64, value : u64) -> Result<(),Error>{
        let h = self.h(key);
        let c : Cell = self.rc((h*25) + 8)?;

        if !c.2 || c.0 == key{
            self.wc((h*25) + 8, gc(key,value))?;
            self.len += 1;
        }else{
            let mut c = c;
            let mut cid = (h * 25) + 8;
            while c.3{
                cid = c.4;
                c = self.rc(c.4)?;
            }
            loop{
                let osjgisdnj = orng();
                c.4 = (self.h(osjgisdnj) * 25) + 8;
                if !self.ie(c.4)?{break;}
            }
            c.3 = true;
            self.len += 1;
            self.wc(cid, c)?;
            self.wc(c.4, gc(key, value))?;
            return Ok(())
        }
        self.rebucket()?;
        Ok(())
    }
    pub fn remove(&mut self, key : u64) -> Result<(), Error>{
        let h = self.h(key);
        let c : Cell = self.rc((h*25) + 8)?;

        if c.2 && c.0 == key{
            self.wc((h*25) + 8, (0u64,0u64,false,false,0u64) as Cell)?;
            self.len -= 1;
        }else{
            let mut c = c;
            let mut cid = (h * 25) + 8;
            while c.3 && c.1 != key{
                cid = c.4;
                c = self.rc(c.4)?;
            }
            c.3 = false;
            self.len -= 1;
            self.wc(c.4, (0u64,0u64,false,false,0u64) as Cell)?;
            c.4 = 0;
            self.wc(cid, c)?;
            return Ok(())
        }
        Ok(())
    }
    pub fn sync(&mut self) -> Result<(),Error>{
        let l = self.len.to_le_bytes();
        self.file.write_all_at(&l, 0)?;
        self.file.sync_all()?;
        Ok(())
    }
    pub fn get(&mut self,key:u64) -> Result<Option<u64>,Error>{
        let h = self.h(key);
        let c : Cell = self.rc((h*25) + 8)?;

        if c.2 && c.0 == key{
            Ok(Some(c.1))
        }else{
            let mut c = c;
            while c.3{
                if c.0 == key{
                    return Ok(Some(c.1))
                }
                c = self.rc(c.4)?;
            };
            Ok(None)
        } 
    }
    fn rebucket(&mut self) -> Result<(),Error>{
        if self.len < self.size.saturating_div(2){
            return Ok(())
        }
        let mut readen = 0;
        let temp_file = format!("{}.tmp",orng());
        let mut tf = OpenOptions::new().read(true).create_new(true).append(true).open(&temp_file)?;
        while readen < self.size{
            let tr = 160000.min(self.size-readen) as usize;
            readen += tr as u64;
            let mut b = vec![0u8;tr * 25];
            self.file.read_at(&mut b, (readen * 25) + 16)?;
            let mut iswuwhgfusevhsuh = Vec::new();
            for psgjkvosehtsuievhmj in b.chunks_exact(25){
                if psgjkvosehtsuievhmj[24] == 1 || psgjkvosehtsuievhmj[24] == 3{
                    iswuwhgfusevhsuh.extend_from_slice(&psgjkvosehtsuievhmj[0..16]);
                }   
            }
            tf.write_all(&b)?;
        }
        self.file.set_len(8)?;
        self.len = 0;
        self.size *= 2;
        self.file.set_len(self.size*25 + 8)?;
        
        readen = 0;
        let s = tf.metadata()?.len()/16;
        while readen < s{
            let sr = 2560000.min(s-readen);
            readen += s;
            let mut b = vec![0u8;(sr*16) as usize];
            tf.read_at(&mut b, readen * 16)?;
            for efisficahnchioueshatyugrehmcrthesihyndroehiteshmrehf in b.chunks_exact(16){
                let mut load = [0u8;8];
                load[..].copy_from_slice(&efisficahnchioueshatyugrehmcrthesihyndroehiteshmrehf[0..8]);
                let key = u64::from_le_bytes(load);
                load[..].copy_from_slice(&efisficahnchioueshatyugrehmcrthesihyndroehiteshmrehf[8..16]);
                let value = u64::from_le_bytes(load);
                self.insert(key,value)?;
            }
        }
        fs::remove_file(&temp_file)?;
        Ok(())

    }
}

