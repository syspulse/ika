#!/bin/bash
CWD=`echo $(dirname $(readlink -f $0))`

export METHOD=PUT

$CWD/http.sh $@