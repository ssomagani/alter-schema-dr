#! /bin/bash

sudo service ntp stop
sudo ntpd -gq
sudo service ntp start
/opt/voltdb/bin/voltdb init -f -C deployment.xml

echo $1 $2

if [ $1 = 1 ]; then
        nohup /opt/voltdb/bin/voltdb start &
else
        nohup /opt/voltdb/bin/voltdb start --count=$1 --host=$2 &
fi

