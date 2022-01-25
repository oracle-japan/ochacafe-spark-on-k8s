#!/bin/bash

APP_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)

BEFORE="name\: op-avg-arrival-delay-by-airline"
AFTER="name\: op-avg-arrival-delay-by-airline-${APP_ID,,}"
sed -e "s/$BEFORE/$AFTER/" $(dirname $0)/avg_arrival_delay_by_airline.yaml | kubectl apply -f -
