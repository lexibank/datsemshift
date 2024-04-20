# Examples for querying the database

To run the example showing how to search for language-specific word families, first create the SQLite database:

```
$ cldf createdb ../cldf/cldf-metadata.json dss.sqlite
```

Then run the shell script:

```
$ sh query.sh
```

To query for the individual directed relations in the data at the level of the concepts, type:

```
$ python shifts.py
```
