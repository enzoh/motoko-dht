#!/bin/bash

DFX=$(which dfx)
# DFX='./../sdk/target/x86_64-unknown-linux-musl/debug/dfx'

N=5

declare -a NODE

for i in $(seq 1 $N)
do
    ${DFX} build
    ${DFX} canister install dht
    NODE[$i]=$(${DFX} canister id dht | awk -F ':' '{print $2}')
done

for i in $(seq 1 $N)
do
    j=$(
    if [ $i -eq $N ]
    then echo 1
    else echo $(expr $i + 1)
    fi
    )
    eval ${DFX}' canister call ic:'${NODE[$i]}' configure '\''("'${NODE[$j]}'")'\'''
done

for i in $(seq 0 255)
do
    i=$(expr $i % $N + 1)
    KEY=$(openssl rand -hex 32)
    VALUE=$(openssl rand -hex 1000)
    eval ${DFX}' canister call ic:'${NODE[$i]}' putInHex '\''("'${KEY^^}'","'${VALUE^^}'")'\'''
done
