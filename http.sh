#!/bin/bash
CWD=`echo $(dirname $(readlink -f $0))`
METHOD=${METHOD:-GET}

SERVICE_URI=${SERVICE_URI:-http://localhost:8080/api/v1/ika}

DATA_JSON=${1:-test/rpc3/REQ_latest.json}

COUNT=${COUNT:-0}
SLEEP=${SLEEP:-0}

function request() {
   curl -S -s -D /dev/stderr -X ${METHOD} \
     --data @"$DATA_JSON" \
     -H 'Content-Type: application/json' \
     -H "Authorization: Bearer $ACCESS_TOKEN" \
     $SERVICE_URI/
}

for c in `seq 0 $COUNT`; do
   request
   sleep $SLEEP

done
