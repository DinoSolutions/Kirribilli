# Project Codename "Kirribilli"

###Introduction

"Kirribilli" is a skeleton module to read numerical (and text in some cases) signals from raw MDF files, re-sample them to certain frequencies if required, and save them into a SQL database.

MDF is an efficient and high performance file format. Saving signals in database however brings potential benefits for web-based data management and visualization.

MDF v4.10 is tested. Theoretically the module should support v3.20 onwards.

###Prerequisites

New dependencies may be added.

##### - Python libraries needed for asammdf:

```
pip install asammdf
pip install PyQt5
pip install -I --no-deps https://github.com/pyqtgraph/pyqtgraph/archive/develop.zip
pip install psutil
```

*Skip PyQt5 and pyqtgraph if GUI is not used.*

##### - Database support:

PostgreSQL (V10 is tested)

[PostgreSQL Download](https://www.postgresql.org/download/ "Download") 

##### - PostgreSQL database adapter for Python:

```pip install psycopg2```

[psycopg2 Documentation](https://www.psycopg.org/docs/)

###Feedback

Feel free to create issues and/or PR to help improve.