#!/bin/bash

# ~/sparkapps//sparkshell.bash

# This script should start ~/spark/bin/spark-shell

~/spark/bin/spark-shell --packages com.databricks:spark-csv_2.11:1.5.0 $@
# The above line is a demo of --packages
# Newer versions of spark already have com.databricks:spark-csv

exit
