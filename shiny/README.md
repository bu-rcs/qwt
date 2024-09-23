# accounting

Dir:     accounting
Auth:    cjahnke
Date:    2016-12-01
Updated: 2023-05-30 major restruct

## Overview
This directory contains the accounting data for the SCC and the scripts that 
parse the orignal source (`/usr/local/sge/{}/common/accounting`) of data.
There are two datasets -- "scc" and "ood" --  one for each cell that runs
jobs and each data source has three data formats (original, csv, feather). The
`getdata.sh` script runs daily as a cronjob on sccsvc to reduce and translate
the real accounting files into the data that RCS uses for various analysis and
reporting.

## Datasets
We store data for the two SCC cells seperately and maintain three data formats
for each. The data formats include the original (symlink), a reduced csv 
format, and a reduced feather format.

1. "scc"    Contains all jobs run on the SCC cell. This data is large and
.           split by year. Prior years are staic, the current year is 
.           updated daily.
2. "ood"    Contains all jobs run on the SCC OnDemand cell. This data is
.           smaller than the "scc" dataset and a single file contains 
.           multiple years worth of data. Also updated daily.

## Directory Structure
```
accounting/
|-- README.md
|-- getdata.R
`-- data
    |-- ood
    |   `-- accounting.csv
    |   `-- accounting.feather
    |   `-- accounting -> /usr/local/sge/sge_root/ood/common/accounting
    `-- scc
        |-- {%Y}.csv
        |-- {%Y}.feather
        `-- {%Y} -> /usr/local/sge/common/accounting.{%Y}
```


## Notes
 1. The `./data/scc/accounting.{YYYY}` file for current year is linked manually
    to the current sge accounting file each year after Mike rotates the file.
    This usually happens in February. 
   

## Updates, To Do, and Questions for Katia

1. I've replaced the awk files with csv and the rds files with feather. I have
.  also moved most of the data manipulation out of preprocessing and into the 
.  get_acct "helper function". The files should be better and faster for you.
.  TODO: CJ and Katia review the cols to ensure everyone has what they need.

2. The old "awk" file.
.  2.a We have a cronjob that copies the awk file to acct.bu.edu:/local/scfacct/Compute/Plots/accounting/
.      for buyin report charts. Do we still need this?
.  2.b If so, could the awk files be replaced with csv files? Seems silly to do both.

3. The `getdata_preprocess` script is written in R for historical reasons, but
.  it could (now) just as easily be done python. Would that be cleaner or faster?

