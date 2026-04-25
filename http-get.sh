#!/bin/bash
CWD=`echo $(dirname $(readlink -f $0))`

export METHOD=GET

$CWD/http.sh $@