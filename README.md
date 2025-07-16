# TytoDB

A calm, reliable database for those who value both performance and correctness.

## ▶️ Overview

TytoDB is a non‑relational, column‑row database implemented in Rust.  
It focuses on:

- **ACID compliance**  
- **Fast scans** and **indexed equality search**  
- **Lightweight file‑based containers**  
- **Simple MVCC** with a write‑ahead log  
- **Safe, on‑demand Vacuum** to reclaim space  

## 🛠️ Features

- **Containers**  
  Store data in self‑describing files. Metadata + rows + tombstones.  
- **Alba Types**  
  Fixed and variable‑length types (strings, blobs, ints, bools).  
- **On‑disk Indexing**  
  Primary keys hashed with `ahash` → fast fetches.  
- **MVCC & WAL**  
  In‑memory changes are flushed safely with per‑append `fsync`.  
- **Vacuum**  
  Compacts tombstones via a two‑cursor swap algorithm with safety `fsync`s.  
- **Configurable Scheduling**  
  Automate Vacuum runs via simple cron‑style patterns in `settings.yaml`.
