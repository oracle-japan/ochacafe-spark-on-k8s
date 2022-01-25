#!/bin/bash

APP_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)

BEFORE="name\: op-num-airports-by-state-py"
AFTER="name\: op-num-airports-by-state-py-${APP_ID,,}"
sed -e "s/$BEFORE/$AFTER/" $(dirname $0)/num_airports_by_state_py.yaml | kubectl apply -f -



