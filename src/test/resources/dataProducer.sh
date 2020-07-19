#!/bin/bash

display_usage() {
  echo "Usage: $0 <KAFKA_BROKER> <KAFKA_TOPIC>"
}

if [ "$#" -ne 2 ]; then
  display_usage
  exit 1
fi

KAFKA_BROKER=$1
KAFKA_TOPIC=$2

TEMP=(96 97 98)
USERS=("user1" "user2" "user3")

while sleep 1; do
  tmp=${TEMP[$RANDOM % ${#TEMP[@]}]}
  usr=${USERS[$RANDOM % ${#USERS[@]}]}
  epochSeconds=$(date '+%s')
  echo "{\"ts\":$epochSeconds,\"usr\":\"${usr}\",\"tmp\":$tmp}" | kafka-console-producer.sh --broker-list ${KAFKA_BROKER} --topic ${KAFKA_TOPIC}
  echo "{\"ts\":$epochSeconds,\"usr\":\"${usr}\",\"tmp\":$tmp}" | kafka-console-producer.sh --broker-list ${KAFKA_BROKER} --topic ${KAFKA_TOPIC}
done