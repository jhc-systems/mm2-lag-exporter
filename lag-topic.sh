#!/bin/bash
curl -s http://localhost:8088/lag |jq -r '[.connector | .[] | .topics // {} | .[] | .partitions | .[] | select(.lag > 0)] | group_by(.topicName) | .[] | "\(.[0].topicName) \(map(.lag) | add)"'
