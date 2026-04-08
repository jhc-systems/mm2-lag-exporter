package com.lowes.mm2lagexporter.service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.lowes.mm2lagexporter.config.ConnectorConfig;
import com.lowes.mm2lagexporter.model.ConnectorInfo;
import com.lowes.mm2lagexporter.model.ConsumerStatus;
import com.lowes.mm2lagexporter.model.MM2LagInfo;
import com.lowes.mm2lagexporter.model.PartitionOffsetInfo;
import com.lowes.mm2lagexporter.model.TopicInfo;
import com.lowes.mm2lagexporter.utils.Constants;
import com.lowes.mm2lagexporter.utils.Status;

import lombok.extern.slf4j.Slf4j;

/**
 * Creates a new Consumer instance to read the logend offset of the topics in the source cluster.
 */
@Slf4j
@Service
public class SourceConsumerService {
    private final MM2LagInfo mM2LagInfo;
    private final ConnectorConfig connectorConfig;
    private final ConsumerStatus sourceConsumerStatus;
    private final Properties sourceConsumerProperties;
    private ConnectorInfo connectorInfo;
    private KafkaConsumer<String, String> sourceConsumer;

    public SourceConsumerService(MM2LagInfo mM2LagInfo,
                                 ConnectorConfig connectorConfig,
                                 ConsumerStatus sourceConsumerStatus,
                                 @Qualifier("sourceConsumerProperties") Properties sourceConsumerProperties,
                                 @Qualifier("sourceConsumer") KafkaConsumer<String, String> sourceConsumer) {
        this.mM2LagInfo = mM2LagInfo;
        this.connectorConfig = connectorConfig;
        this.sourceConsumerStatus = sourceConsumerStatus;
        this.sourceConsumerProperties = sourceConsumerProperties;
        this.sourceConsumer = sourceConsumer;
    }

    @EventListener(ApplicationReadyEvent.class)
    @Async("sourceTaskExecutor")
    public void runSourceConsumer() {
        log.info("mm2-lag-exporter::Starting New Source Kafka Consumer with config: brokers={}, groupId={}",
                 sourceConsumerProperties.getProperty("bootstrap.servers"),
                 sourceConsumerProperties.getProperty("group.id"));
        Map<String, ConnectorInfo> connectorMap;
        List<String> topicsList;
        sourceConsumerStatus.setStatus(Status.RUNNING);
        sourceConsumerStatus.setTrace("");
        mM2LagInfo.setSourceClusterBrokerUrl(connectorConfig.getSourceBrokerUrl());
        mM2LagInfo.setSourceClusterAlias(connectorConfig.getSourceClusterAlias());
        try {
            while (true) {
                for (Map.Entry<String, List<String>> connectors : connectorConfig.getConnectors().entrySet()) {
                    String connector = connectors.getKey();
                    topicsList = connectors.getValue();
                    Preconditions.checkState(!Strings.isNullOrEmpty(topicsList.toString()), "Topic Name is Empty or Null");

                    if (mM2LagInfo.getConnector() == null) {
                        connectorMap = createConnectorMapFromSource(connector);
                        mM2LagInfo.setConnector(connectorMap);
                        connectorInfo = mM2LagInfo.getConnector().get(connector);
                    } else if (mM2LagInfo.getConnector().containsKey(connector)) {
                        connectorInfo = mM2LagInfo.getConnector().get(connector);
                    } else if (!mM2LagInfo.getConnector().containsKey(connector)) {
                        connectorInfo = new ConnectorInfo(connector, null);
                        mM2LagInfo.getConnector().put(connector, connectorInfo);
                    }
                    Iterator<String> topicIterator = topicsList.iterator();
                    while (topicIterator.hasNext()) {
                        String topicName = topicIterator.next();
                        processTopicDetail(topicName, topicIterator);
                    }
                }
            }
        } catch (Exception ex) {
            log.error("mm2-lag-exporter::Error in source consumer instance", ex);
            sourceConsumerStatus.setStatus(Status.FAILED);
            sourceConsumerStatus.setTrace(ExceptionUtils.getStackTrace(ex));
        } finally {
            log.info("mm2-lag-exporter::Closing source cluster consumer");
            sourceConsumer.close();
        }
    }

    private void processTopicDetail(String topicName, Iterator<String> topicIterator) {

        try {
            //calculate log end offset of topic only if it's exists in the source cluster.
            List<PartitionInfo> topicPartitionInfo = sourceConsumer.partitionsFor(topicName);
            if (topicPartitionInfo != null) {

                // Getting partition information for each topic in the white listed topics.
                Collection<TopicPartition> topicPartitions =
                        topicPartitionInfo.stream()
                                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                                .collect(Collectors.toList());

                log.debug("mm2-lag-exporter::Found {} partitions for topic {}: {}",
                         topicPartitions.size(), topicName, topicPartitions);

                // Fetching Log end offsets and beginning offsets for each Partitions in the topic.
                Map<TopicPartition, Long> endOffsets = sourceConsumer.endOffsets(topicPartitions);
                Map<TopicPartition, Long> beginningOffsets = sourceConsumer.beginningOffsets(topicPartitions);

                if (endOffsets == null || endOffsets.isEmpty()) {
                    log.warn("mm2-lag-exporter::No end offsets returned for topic {}", topicName);
                    return;
                }

                if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                    log.warn("mm2-lag-exporter::No beginning offsets returned for topic {}", topicName);
                    return;
                }

                log.debug("mm2-lag-exporter::Fetched offsets for topic {}: endOffsets={}, beginningOffsets={}",
                         topicName, endOffsets, beginningOffsets);

                endOffsets.forEach((topic, endOffset) -> {
                            String topicValue = topic.topic();
                            int partitionValue = topic.partition();
                            long endOffsetValue = endOffset;
                            long beginningOffsetValue = beginningOffsets.get(topic);

                            log.debug("mm2-lag-exporter::Processing topic={}, partition={}, endOffset={}, beginningOffset={}",
                                     topicValue, partitionValue, endOffsetValue, beginningOffsetValue);

                            if (connectorInfo.getTopics() == null) {
                                // If Map is null
                                connectorInfo.setTopics(createTopicInfoMap(topicValue, partitionValue, endOffsetValue, beginningOffsetValue));
                            } else if (connectorInfo.getTopics().containsKey(topicValue)) {

                                // If the topic is available in the Map
                                if (connectorInfo.getTopics().get(topicValue).getPartitions().containsKey(partitionValue)) {
                                    var partition = connectorInfo.getTopics().get(topicValue).getPartitions().get(partitionValue);
                                    partition.setLogEndOffset(endOffsetValue);
                                    partition.setLogEndOffsetUpdatedAt(Timestamp.valueOf(LocalDateTime.now()));
                                    partition.setLogStartOffset(beginningOffsetValue);
                                    partition.setLogStartOffsetUpdatedAt(Timestamp.valueOf(LocalDateTime.now()));
                                    log.debug("mm2-lag-exporter::Updated existing partition - topic={}, partition={}, endOffset={}, startOffset={}",
                                             topicValue, partitionValue, endOffsetValue, beginningOffsetValue);
                                } else {

                                    //if new Partition is added to the topics
                                    PartitionOffsetInfo partitionOffsetInfo = createPartitionOffsetInfo(topicValue, partitionValue, endOffsetValue, beginningOffsetValue);
                                    connectorInfo.getTopics().get(topicValue).getPartitions().put(partitionValue, partitionOffsetInfo);
                                    log.debug("mm2-lag-exporter::Added new partition - topic={}, partition={}, endOffset={}, startOffset={}",
                                             topicValue, partitionValue, endOffsetValue, beginningOffsetValue);
                                }
                            } else {

                                // If no topic is available in the Map
                                connectorInfo.getTopics().put(topicValue, new TopicInfo(topicValue, createPartitionOffsetInfoMap(topicValue, partitionValue, endOffsetValue, beginningOffsetValue)));
                                log.debug("mm2-lag-exporter::Added new topic - topic={}, partition={}, endOffset={}, startOffset={}",
                                         topicValue, partitionValue, endOffsetValue, beginningOffsetValue);
                            }
                        }
                );
            } else {
                // Topic doesn't exists in source kafka cluster.Removing from the list.
                log.warn("mm2-lag-exporter::Topic {} doesn't exist in source cluster, removing from list", topicName);
                topicIterator.remove();
            }
        } catch (Exception ex) {
            log.error("mm2-lag-exporter::Error processing topic {}: {}", topicName, ex.getMessage(), ex);
            // Don't remove from iterator on error, just log and continue
        }
    }

    private Map<String, ConnectorInfo> createConnectorMapFromSource(String connectorName) {
        Map<String, ConnectorInfo> connectorMapSource = new HashMap<>();
        connectorMapSource.put(connectorName, new ConnectorInfo(connectorName, null));
        return connectorMapSource;
    }

    private Map<String, TopicInfo> createTopicInfoMap(String topicName, int partition, long endOffset, long beginningOffset) {
        Map<String, TopicInfo> topicMap = new HashMap<>();
        Map<Integer, PartitionOffsetInfo> partitionInfo = createPartitionOffsetInfoMap(topicName, partition, endOffset, beginningOffset);
        topicMap.put(topicName, new TopicInfo(topicName, partitionInfo));
        return topicMap;
    }

    private Map<Integer, PartitionOffsetInfo> createPartitionOffsetInfoMap(String topicName, int partition, long endOffset, long beginningOffset) {
        Map<Integer, PartitionOffsetInfo> partitionInfo = new HashMap<>();
        PartitionOffsetInfo partitionOffsetInfo = createPartitionOffsetInfo(topicName, partition, endOffset, beginningOffset);
        partitionInfo.put(partition, partitionOffsetInfo);
        return partitionInfo;
    }

    private PartitionOffsetInfo createPartitionOffsetInfo(String topicName, int partition, long endOffset, long beginningOffset) {
        PartitionOffsetInfo partitionOffsetInfo = new PartitionOffsetInfo();
        partitionOffsetInfo.setTopicName(topicName);
        partitionOffsetInfo.setPartition(partition);
        partitionOffsetInfo.setLogEndOffset(endOffset);
        partitionOffsetInfo.setLogEndOffsetUpdatedAt(Timestamp.valueOf(LocalDateTime.now()));
        partitionOffsetInfo.setLogStartOffset(beginningOffset);
        partitionOffsetInfo.setLogStartOffsetUpdatedAt(Timestamp.valueOf(LocalDateTime.now()));
        return partitionOffsetInfo;
    }

    /**
     * @return source consumer status whether it is in running or failed state.Also provides Error
     * details if it's in Failed state.
     */
    public ConsumerStatus getSourceConsumerStatus() {
        return sourceConsumerStatus;
    }

    /**
     * To manually restart the source consumer instance.
     */
    @Async("sourceTaskExecutor")
    public void restartSourceConsumer() {
        if (sourceConsumerStatus.getStatus() == Status.FAILED) {
            log.info("mm2-lag-exporter::Restarting Source consumer");
            sourceConsumer = new KafkaConsumer<>(sourceConsumerProperties);
            runSourceConsumer();
        }
    }

    /**
     * In Specific intervals scheduler will be called and will restart the failed internal source consumer.
     * This configuration is defaulted to false.This config can be modified to enable auto restart incase
     * any internal consumer failure.
     */
    @Async("sourceTaskExecutor")
    @Scheduled(fixedRateString = Constants.CONSUMER_RESTART_FREQUENCY, initialDelayString = Constants.SCHEDULER_INITIAL_DELAY)
    public void schedulerConsumerRestartOnFailure() {
        if (sourceConsumerStatus.getStatus() == Status.FAILED && connectorConfig.isAutoRestartConsumerEnabled()) {
            log.info("mm2-lag-exporter::Restarting Failed Source consumer from scheduler");
            restartSourceConsumer();
        }
    }
}
