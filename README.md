# Musterstadt Gießt – Tree Data

Please review the content of the `README.md` and adjust it to the project.

## Prerequisites
 * [Python 3.11](https://www.python.org/downloads/)

## Install & Development

### Windows
#### QGIS
 * https://qgis.org/en/site/forusers/download.html
 * this will install also `OSGeo4W Shell` where gdal is available (check `gdalinfo --version`)
 
### Local PostgreSQL
 * Download latest (15.3) and install: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads 
 * Download latest PostGIS and install in before installed Postgres folder: https://download.osgeo.org/postgis/windows/pg15/ 
 * Connect to database and execute:
```
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
```
 
#### Miniconda
 * https://docs.conda.io/en/latest/miniconda.html#windows-installers
 * Open `Anaconda prompt (miniconda3)` from Quick start

### Linux
#### Miniconda

```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
rm ./Miniconda3-latest-Linux-x86_64.sh
```

### All
#### Miniconda
 * change to folder containing this locally checked out Git repo

```
conda update conda
conda create -n treedata
conda activate treedata
conda install pip
pip install -r requirements.txt
```

* install GDAL
```
conda install -c conda-forge gdal
```

* support for gdalwarp
```
conda install -c conda-forge krb5
pip install gssapi
gdalwarp -v
```

### PyCharm
 * Download Community Edition: https://www.jetbrains.com/pycharm/
 * https://www.jetbrains.com/help/pycharm/conda-support-creating-conda-virtual-environment.html#create-a-conda-environment
   * select existing environment `treedata`
 * open new terminal to see activated conda environment `treedata`

### Next steps
 * copy `resources/sample.env` to `resources/.env`
   * set your (locally) PostgreSQL connection data

## Demo
 * Download city shape WFS file to geojson: `python ./treedata/main.py city_shape`
   * complete: `python ./treedata/main.py city_shape --wfs-url <WFS-URL> --source-encoding iso-8859-1 --xml-file-name wfs --geojson-file-name city-shape --skip-download-wfs-xml --skip-convert-to-geojson`
 * Download trees WFS file to geojson: `python ./treedata/main.py trees`
   * complete: `python ./treedata/main.py trees --wfs-url <WFS-URL> --source-encoding iso-8859-1 --xml-file-name wfs --geojson-file-name trees --skip-download-wfs-xml --skip-convert-to-geojson`
 * Process geojson: `python ./treedata/main.py trees_process
   * complete: `python ./treedata/main.py trees_process --city-shape-geojson-file-name city_shape --trees-geojson-file-name trees --geojson-file-name trees-transformed --database-table-name trees_tmp --skip-transform --skip-store-as-geojson --skip-upload-to-db`
   * store as file only: `python ./treedata/main.py trees_process --city-shape-geojson-file-name city_shape --skip-upload-to-db --trees-geojson-file-name s_wfs_baumbestand_2023-07-15`
   * store in db only: `python ./treedata/main.py trees_process --skip-transform --skip-store-as-geojson --trees-geojson-file-name trees_transformed --database-table-name trees_tmp`
