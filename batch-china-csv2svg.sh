#!/bin/bash
for i in data/*.csv
	do ../../visualization/dxy-china-svg/dxy-china-csv2svg $i
done