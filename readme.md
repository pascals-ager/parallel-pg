Challenge Task 1:
----------------

A load utility is provided to load the data into postgres. It expects basic configuration to be present in "/configuration/config.py/PostgresConfig" object.

Assumptions made:
1. The schema of the data is not known. Hence we cannot flatten the json and insert into pre-defined columns inorder to take advantage of columnar storage.
2. It is observed that track_events.json is an incorrect json array, i.e it had a prefix '[', but not the corresponding suffix ']', Hence it is assumed that the files need not be formatted.
3. One assumption is that the individual jsons are seperated by a ',\n'
4. The solution has to be generic enough to work for 1000's of flat json files. (Thereby ruling out COPY/LOAD utilities)
5. The solution should provide command line arguments to configure source files and destination tables.
6. The files can be arbitrarily large/small, hence the solution must provide a way to configure batch size of inserts because inserting per line is expensive on the cores whereas extremely large batches may not fit in memory.  

###Basic usage help
```buildoutcfg
python load.py -h

usage: load.py [-h] {load,seed} ...

Bulk Load flat files into Postgres

positional arguments:
  {load,seed}  sub command help
    load       load flat file
    seed       seed the database with the table

optional arguments:
  -h, --help   show this help message and exit

```
```buildoutcfg
python load.py seed -h

usage: load.py seed [-h] --dst_table DST_TABLE

optional arguments:
  -h, --help            show this help message and exit
  --dst_table DST_TABLE
                        Name of destination table with JSONB data column

```

```buildoutcfg
python load.py load -h

usage: load.py load [-h] --src_file SRC_FILE --dst_table DST_TABLE
                    [--chunk CHUNK]

optional arguments:
  -h, --help            show this help message and exit
  --src_file SRC_FILE   Location of the sourcefile
  --dst_table DST_TABLE
                        Name of the destination table
  --chunk CHUNK         chunks to load per insert

```

###How to run:
```buildoutcfg
sudo apt-get install postgresql-client
docker pull postgres
mkdir -p $HOME/docker/volumes/postgres
docker run --rm   --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 127.0.0.1:5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data  postgres

## Now we have a postgres docker runnin with a dummy password docker. Note that this password goes into /configuration/config.py

# Go to root of the repository:
python load.py seed --dst_table track_events_json
python load.py seed --dst_table weather_json

python load.py load --src_file ~/track_events.json --dst_table track_events_json --chunk 10000
python load.py load --src_file ~/weather.json --dst_table weather_json --chunk 5000

# logs can be found at /logs/tierloader.log

```