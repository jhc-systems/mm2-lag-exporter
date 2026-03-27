#!/bin/bash
curl -s http://localhost:8088/lag  | jq '[.connector | .[] | .topics // {} | .[] | .partitions | .[] | select(.lag > 0) | {topic: .topicName, partition: .partition, lag: .lag}]'
