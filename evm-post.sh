#!/bin/bash
CWD=`echo $(dirname $(readlink -f $0))`

DATA_JSON=${1:-test/rpc3/evm/REQ_latest.json}

$CWD/rpc3-post.sh $DATA_JSON