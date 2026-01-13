<h1 align="center">NOSQLVIS</h1>

<p align="center">
  <em>A Unified Visualization and Exploration Platform for Heterogeneous NoSQL Datastores</em>
</p>

<p align="center">
  <img src="https://img.shields.io/github/last-commit/UserLadid/NoSQLVis?style=flat-square" />
  <img src="https://img.shields.io/github/issues/UserLadid/NoSQLVis?style=flat-square" />
  <img src="https://img.shields.io/github/license/UserLadid/NoSQLVis?style=flat-square" />
</p>

<p align="center">
  <strong>Built with the tools and technologies:</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-blue?style=flat-square&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Tkinter-orange?style=flat-square" />
  <img src="https://img.shields.io/badge/CustomTkinter-orange?style=flat-square" />
  <img src="https://img.shields.io/badge/Cassandra-blue?style=flat-square&logo=cassandra&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-red?style=flat-square&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/MongoDB-green?style=flat-square&logo=mongodb&logoColor=white" />
  <img src="https://img.shields.io/badge/Neo4j-blue?style=flat-square&logo=neo4j&logoColor=white" />
</p>

**NoSQL Vis**

NoSQL Vis is a lightweight, extensible desktop application for interactive exploration, querying, and visualization of NoSQL databases. It provides a unified graphical interface that abstracts database-specific query languages and heterogeneous data models, enabling users to inspect and analyze data efficiently across multiple NoSQL systems.

**Features**

Unified graphical interface for NoSQL database exploration

Support for multiple NoSQL data models:

    - Key–value databases

    - Wide-column databases

    - Document databases

    - Graph databases

Dynamic adaptation to database metadata

Interactive data browsing and filtering

Real-time data inspection

Export of query results for external analysis

Desktop-based application with a focus on usability and extensibility

**Supported Databases**

NoSQL Vis currently supports representative systems from each major NoSQL category:

    - Redis (Key–value)

    - Apache Cassandra (Wide-column)

    - MongoDB (Document)

    - Neo4j (Graph)

The architecture is designed to allow additional NoSQL backends to be integrated with minimal effort.

**Architecture Overview**

NoSQL Vis follows a modular architecture consisting of:

    - A backend abstraction layer that unifies database connections and queries

    - Database-specific adapters responsible for communication with each NoSQL system

    - A desktop graphical user interface for interaction and visualization

    - Utility modules for data processing and export

This design ensures backend independence and consistent user experience across different database types.

**Installation**

Requirements

    - Python 3.9 or newer
    - Docker

Clone the repository
git clone https://github.com/UserLadid/NoSQLVis.git
cd NoSQLVis

Install dependencies
pip install -r requirements.txt

## Docker Deployment

NoSQL Vis can be launched using Docker and Docker Compose, including all required NoSQL databases.

### Launch the application

From the project root, run:

```bash
docker-compose up --build



**Usage**

From the interface, users can:

    - Select a NoSQL backend

    - Configure connection parameters

    - Explore available keyspaces, databases, or collections

    - Browse, filter, and export data interactively


**License**

This project is released under the MIT License.
See the LICENSE file for details.

**Citation**

If you use NoSQL Vis in academic work, please cite the corresponding SoftwareX publication (to be added upon acceptance).


**Contact**

For questions, feedback, or collaboration inquiries, please contact the project maintainers via GitHub issues.
```
