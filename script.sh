#!/bin/sh 
while true  
do 
    date;
    cd csv_downloads/
    curl -O  \
        http://192.168.2.10/FileBrowser/Download?Path=%2FDataLogs%2Ffile_2.csv \
        -H "Accept-Encoding: gzip, deflate" \
        -H "Referer: http://192.168.2.10/Portal/Portal.mwsl?PriNav=DataLogs&Path=file_2.csv" \
        -H "Accept-Language: en-US,en;q=0.9,fr;q=0.8" \
        -H "Host: 192.168.2.10"

    cd -;
    now=$(date) 
    node ./index.js > ./logs/${now}_log.txt   
    sleep 3
done     