package com.lowes.mm2lagexporter.model;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class TopicInfo {
    private String topicName;
    private Map<Integer, PartitionOffsetInfo> partitions = new ConcurrentHashMap<>();
}
