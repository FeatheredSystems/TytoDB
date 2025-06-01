use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use ahash::AHashMap;
use xxhash_rust::const_xxh3;
use tokio::sync::RwLock;

type Checksum = u64;
type BinaryData = Vec<u8>;
pub type DataReference = (Checksum,BinaryData);
pub type Ward = (File,AHashMap<usize,DataReference>);

#[derive(Default)]
pub struct Strix{
    pub wards : Vec<RwLock<Ward>>,
}


pub async fn start_strix(strix : Arc<RwLock<Strix>>){
    //let interval = std::time::Duration::from_millis(100);
    tokio::task::spawn_blocking(async move ||{
        loop{
            // Sleeping
            //sleep(interval).await;
            let fl = strix.read().await;
            let wards_iter = fl.wards.iter().enumerate();
            let mut to_remove : Vec<usize> = Vec::new();
            for (_,i) in wards_iter{
                let mut lock = i.write().await;
                if lock.1.len() > 0{
                    for (k,i) in lock.1.iter(){
                        let mut comp = vec![0u8;i.1.len()];
                        if let Err(_) = lock.0.read_exact_at(&mut comp, i.0.clone() as u64){
                            break;
                        }
                        let untrusty_cs = const_xxh3::xxh3_64(&comp);
                        if untrusty_cs == i.0{
                            to_remove.push(k.clone());
                        }else{
                            let _ = lock.0.write_all_at(&i.1, k.clone() as u64);
                            break;
                        }
                    }
                }
                for tr in &to_remove{
                    lock.1.remove(&tr);
                }
            }
            drop(fl);
            let mut fl = strix.write().await;
            for &idx in to_remove.iter().rev() {
                fl.wards.remove(idx);
            }

        }
    });
}