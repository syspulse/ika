#!/bin/bash
CWD=`echo $(dirname $(readlink -f $0))`

export METHOD=POST

$CWD/http.sh $@