# TytoDB

TytoDB is a high-performance, non-relational column-row database designed with a strong emphasis on ACID compliance. It excels in equality searches involving the primary key and is optimized for speed in database scans, as well as in insert and delete operations.

🛠️ Features
- **Containers**: Store data in self‑describing files. Metadata + rows + tombstones.
- **Alba Types**: Fixed and variable‑length types (strings, blobs, ints, bools).
- **On‑disk Indexing**: Primary keys hashed with ahash → fast fetches.
- **MVCC & WAL**: In‑memory changes are flushed safely with per‑append fsync.
- **Vacuum**: Compacts tombstones via a two‑cursor swap algorithm with safety fsyncs.
- **Configurable Scheduling**: Automate Vacuum runs via simple cron‑style patterns in settings.yaml.
