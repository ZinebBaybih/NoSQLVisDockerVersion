# NoSQL Vis

NoSQL Vis is a desktop application for educational exploration of heterogeneous NoSQL databases. It provides one graphical workflow for four representative data models:

- MongoDB: document database
- Redis: key-value database
- Cassandra: wide-column database
- Neo4j: graph database

The application is designed for learning, comparison, and lightweight visual inspection. It is not intended to be a full database administration console.

## Features

- Unified connection screen for MongoDB, Redis, Cassandra, and Neo4j
- Database/keyspace/label exploration with system objects hidden where possible
- Preview-first browsing with pagination
- Search/filter controls for current previews
- Sampled charts and graph previews
- CSV export of the visible preview/page
- Seed datasets for library, education, ecommerce, and dentist workflow examples
- Optional benchmark instrumentation for research reproducibility

## Repository Structure

```text
app/                         GUI, connectors, views, and utilities
seed.py                      Optional demo and benchmark data seeder
scripts/benchmark_backend.py Optional backend benchmark runner
docker-compose.yml           Database services, plus optional Linux GUI profile
Dockerfile                   Linux GUI application image
```

## Requirements

For all installation modes:

- Docker and Docker Compose
- Git

For running the app directly from Python:

- Python 3.9 or newer
- A virtual environment is recommended

Neo4j demo credentials:

```text
User: neo4j
Password: password
```

## Quick Start on Windows

On Windows, the recommended setup is to run the databases with Docker Compose and run the GUI from a Python virtual environment. This avoids GUI/display issues that are common with desktop applications inside Docker on Windows.

1. Start the databases:

```powershell
docker compose up -d mongodb redis cassandra neo4j
```

2. Create and activate a virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

3. Install dependencies:

```powershell
pip install -r requirements.txt
```

4. Seed the demo data:

```powershell
python seed.py
```

5. Launch the application:

```powershell
python app/main.py
```

Use `localhost` as the host in the connection screen.

## Quick Start on Linux

On Linux, you can run both the databases and the GUI through Docker. The GUI container uses X11 display forwarding and host networking, so this path is Linux-specific.

1. Allow local Docker containers to use your X server:

```bash
xhost +local:docker
```

2. Start the databases:

```bash
docker compose up -d mongodb redis cassandra neo4j
```

3. Seed the demo data from the application image:

```bash
docker compose --profile linux-gui run --rm app python seed.py
```

4. Launch the GUI:

```bash
docker compose --profile linux-gui up app
```

Use `localhost` as the host in the connection screen. The Linux GUI service uses host networking so the container can reach the database ports exposed on the host.

When finished, you can revoke the X server permission:

```bash
xhost -local:docker
```

## Seeding Data

`seed.py` is separate from application launch on purpose. This makes the reset step explicit and avoids silently deleting or replacing data when the app starts.

Run the standard educational seed:

```bash
python seed.py
```

This creates:

- `library_demo`: small beginner dataset
- `education_demo`: students, courses, branches, and enrollments
- `ecommerce_demo`: customers, products, categories, orders, and carts
- `dentist_demo`: patients, dentists, clinics, treatments, and appointments

Redis uses logical databases:

- DB `0`: library demo
- DB `2`: education demo
- DB `3`: ecommerce demo
- DB `4`: dentist demo

The seeder performs a factory reset of app-owned demo scopes before inserting data. Do not point it at production or shared external databases.

## Optional Large Benchmark Seed

Large benchmark data is separate from the educational datasets:

```bash
python seed.py --large --size 1000
python seed.py --large --size 10000
python seed.py --large --size 100000
```

The benchmark seed creates a `benchmark` dataset for MongoDB, Redis DB `1`, Cassandra, and Neo4j. Use it only when reproducing performance experiments or stress-testing pagination behavior.

## Tutorial Walkthrough

After starting the databases, seeding, and launching the app:

1. Select a database type on the connection tab.
2. Keep `localhost` as the host when using the default setup.
3. Use the default port for the selected database.
4. For Neo4j, enter `neo4j` and `password`.
5. Click `Connect`.

Suggested learning path:

- Start with MongoDB and open `library_demo`, then compare it with `education_demo`, `ecommerce_demo`, or `dentist_demo`.
- Open Redis and switch between DB `0`, DB `2`, DB `3`, and DB `4` to compare strings, hashes, sets, sorted sets, lists, and streams.
- Open Cassandra and compare keyspaces/tables across the educational domains.
- Open Neo4j and select labels such as `Student`, `Customer`, or `Patient` to view sampled multi-node graph neighborhoods.

The app intentionally shows controlled previews. Larger tables and graphs should be explored through pagination and sampled visual summaries rather than full-dataset rendering.

## Benchmarks and Performance Reproducibility

Benchmarking is optional and mainly intended for reproducing results discussed in the related research article. It is not required for classroom or self-learning use.

Backend benchmark example:

```powershell
.\venv\Scripts\python.exe scripts\benchmark_backend.py --size-label 1K --repeats 5
```

GUI instrumentation can be enabled before launching the app:

```powershell
$env:NOSQL_VIS_BENCHMARK_UI="1"
$env:NOSQL_VIS_DATASET_SIZE="1K"
python app/main.py
```

Generated CSV outputs are written under `results/benchmarks/`. The `results/` directory is ignored by Git because these files are generated artifacts. Commit scripts and methodology; regenerate raw benchmark outputs when needed.

## Stopping Services

Stop containers while keeping volumes:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

## Notes

- Cassandra may need extra time to become ready after containers start.
- The seeder waits for Cassandra and Neo4j, but if a service is still starting, rerun `python seed.py`.
- Export actions export the current visible preview/page, not the full database.
- Graph visualizations are sampled previews, not full graph renderings.

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

## Citation

If you use NoSQL Vis in academic work, please cite the corresponding publication when available.

