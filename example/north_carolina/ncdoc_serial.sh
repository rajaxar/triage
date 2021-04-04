#!/bin/bash

# This script runs verything serially as king-friday doesnt have GNU parallel
set -e

sh ncdoc_des2csv.sh OFNT3AA1.zip 
sh ncdoc_des2csv.sh APPT7AA1.zip 
sh ncdoc_des2csv.sh APPT9BJ1.zip 
sh ncdoc_des2csv.sh INMT4AA1.zip 
sh ncdoc_des2csv.sh INMT4BB1.zip 
sh ncdoc_des2csv.sh INMT4CA1.zip 
sh ncdoc_des2csv.sh INMT9CF1.zip 
sh ncdoc_des2csv.sh OFNT1BA1.zip 
sh ncdoc_des2csv.sh OFNT3BB1.zip 
sh ncdoc_des2csv.sh OFNT3CE1.zip 
sh ncdoc_des2csv.sh OFNT3DE1.zip 
sh ncdoc_des2csv.sh OFNT9BE1.zip