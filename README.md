# 2019-nCoV data utility

Command line utility handling 2019-nCoV data conversion from [Isaac Lin](https://github.com/BlankerL)'s time series flow to individual csv records of [globalcitizen](https://github.com/globalcitizen/2019-wuhan-coronavirus-data/) whose format better suit needs of visualization as well as deduplication maintenance of globalcitizen's csv records. 

## Usage

```
nutil <command> [<args>]
```
where <command> is one of available commands.

## Available commands

`icsv2gcsv`, `ijson2gcsv` and `gdedupe` commands are integrated.

### icsv2gcsv

Converts DXYArea CSV data from Isaac Lin to CSV records for globalcitizen. Deduplication is baked in.

```
nutil icsv2gcsv <-o <output-directory>> <input-csv>
  -o string
    	csv records output directory
```

e.g. `nutil icsv2gcsv -o dxy data/DXYarea.csv` (read `data/DXYarea.csv` & pour csv records to `dxy` folder)

### ijson2gcsv 

Convert DXYArea JSON data from Isaac Lin to CSV records for globalcitizen. Deduplication is baked in.

```
nutil ijson2gcsv <-o <output-directory>> <input-json>
  -o string
    	csv records output directory
```

e.g. `nutil ijson2gcsv -o dxy data/DXYarea.json` (read `data/DXYarea.json` & pour csv records to `dxy` folder)

### gdedupe

Purge duplicate CSV records for globalcitizen. All record-related files including `csv` itself, `json`, `svg` are removed whenever a duplication is detected.

```
nutil gdedupe [-n <max-count>] [-omit-comment] <input-directory>
  -n int
    	maximum csv records to read & process
    	default: no record limit (default -1)
  -omit-comment
    	omit comment of records when comparing to determine duplication
```

e.g. `nutil gdedupe dxy` (scan entire `dxy` folder) 

`nutil gdedupe -n 2 dxy` (scan only last two records in `dxy` folder, helpful for incremental dedupe)

## Related projects

[globalcitizen/2019-wuhan-coronavirus-data](https://github.com/globalcitizen/2019-wuhan-coronavirus-data/)(data repo, data fetcher & visualizer)<br/>

[BlankerL/DXY-2019-nCoV-Crawler](https://github.com/BlankerL/DXY-2019-nCoV-Crawler) (data fetcher)<br/>
[BlankerL/DXY-2019-nCoV-Data](https://github.com/BlankerL/DXY-2019-nCoV-Data) (data static repo)<br/>
[lab.isaaclin.cn/nCoV/](https://lab.isaaclin.cn/nCoV/) (data dynamic provider API)