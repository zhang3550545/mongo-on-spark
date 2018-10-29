#!/usr/bin/env bash

test1="test1"
test2="test2"
test3="test3"


if [ -n "${1}" -a -n "${2}" -a -n "${3}" ];then
    test1=$1
    test2=$2
    test3=$3
  else
    echo "需要传入3个参数"
    exit 1
fi

echo ${test1}
echo ${test2}
echo ${test3}