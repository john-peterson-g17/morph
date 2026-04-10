# 🧬 Morph

**Morph** is a Go CLI for performing safe, resumable data backfills as part of schema evolution.

It’s designed for production systems where large datasets need to be reshaped without downtime. Morph handles chunking, progress tracking, and controlled load so you can migrate data safely alongside live application traffic.

> [WARNING]
> This project is in early development. Expect breaking changes and missing features. Feedback and contributions are welcome!

---

## 🚨 Why Morph?

Schema migrations are easy—until you need to move **millions or billions of rows**.

Most teams end up writing one-off scripts that:

- overload the database
- can’t be resumed safely
- don’t track progress
- break halfway through
- are hard to verify

Morph exists to solve that.

---

## ✨ What it does

- **Chunked backfills**
  Process large datasets incrementally without overwhelming your database

- **Resumable jobs**
  Safely stop and restart without losing progress

- **Controlled load**
  Throttle execution to avoid impacting production traffic

- **Data reshaping**
  Transform data as it moves to match new schemas

- **Idempotent execution**
  Designed to safely re-run without duplicating or corrupting data

---

## 🧠 What it is (and isn’t)

### ✅ Morph is:

- A backfill engine for **application-level schema evolution**
- Designed for **live production databases**
- Focused on **correctness and safety**

### ❌ Morph is NOT:

- A schema migration tool (use Flyway, Prisma, etc.)
- A general-purpose ETL pipeline
- A data warehouse tool

---

## 🚀 Installation

```bash
go install github.com/your-org/morph@latest
```

_or clone and build:_

```bash
git clone https://github.com/your-org/morph.git
cd morph
go build -o morph
```

---

## ⚡ Quick Example

```bash
morph run backfill.yaml
```

Example config:

```yaml
name: users-backfill

source:
  table: users

target:
  table: users_v2

chunking:
  column: id
  size: 1000

transform:
  sql: |
    SELECT id, email, created_at
    FROM users
```

---

## 🔄 Typical Migration Workflow

1. **Deploy new schema** (e.g. `users_v2`)
2. **Start dual writes** in your application
3. **Run Morph backfill**
   ```bash
   morph run users-backfill.yaml
   ```
4. **Monitor progress**
5. **Verify data consistency**
6. **Cut over reads to new schema**
7. **Remove old schema**

---

## 📊 Core Concepts

### Job

A backfill task that moves and reshapes data from a source to a target.

### Chunking

Data is processed in small batches (e.g. by primary key range) to reduce load and improve reliability.

### Resume

Jobs can be safely restarted without reprocessing completed data.

---

## 🛠 Roadmap (early)

- [ ] Postgres + MySQL support
- [ ] Adaptive throttling
- [ ] Progress + metrics output
- [ ] Data validation / verification
- [ ] CLI improvements (`resume`, `status`)
- [ ] CDC-based catch-up mode

---

## 🤝 Contributing

Contributions are welcome. Open an issue or PR to get started.

---

## 📄 License

MIT License

---

## 💡 Philosophy

Morph is built on a simple idea:

> Schema changes are easy. Safely moving the data is the hard part.
