# OSM-Historical-Importer
Import an OSM historical file into a PostgreSQL database.
Made with Python 3.

This scripts uses [pyosmium](https://github.com/osmcode/pyosmium) to parse an OSM historical file, and then imports the data into a PostgreSQL database.

## Usage
Create a database, and set the value ok `DB_NAME`, `DB_USER`, `DB_PWD`, `DB_HOST`and `DB_PORT`. Then simply run the script:
```
python osm-importer.py <osmfile>
```
