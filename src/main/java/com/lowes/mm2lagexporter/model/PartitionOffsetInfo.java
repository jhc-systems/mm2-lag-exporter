package com.lowes.mm2lagexporter.model;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
@Getter
@Setter
public class PartitionOffsetInfo {
    private String topicName;
    private int partition;
    private long logEndOffset;
    private Timestamp logEndOffsetUpdatedAt;
    private long logStartOffset;
    private Timestamp logStartOffsetUpdatedAt;
    private long mmOffset;
    private Timestamp mmOffsetUpdatedAt;
    private long lag;

    // Constructor for backward compatibility with existing code
    public PartitionOffsetInfo(String topicName, int partition, long logEndOffset,
                              Timestamp logEndOffsetUpdatedAt, long mmOffset,
                              Timestamp mmOffsetUpdatedAt, long lag) {
        this.topicName = topicName;
        this.partition = partition;
        this.logEndOffset = logEndOffset;
        this.logEndOffsetUpdatedAt = logEndOffsetUpdatedAt;
        this.mmOffset = mmOffset;
        this.mmOffsetUpdatedAt = mmOffsetUpdatedAt;
        this.lag = lag;
        this.logStartOffset = 0; // default value
        this.logStartOffsetUpdatedAt = null; // default value
    }

    public void setLogEndOffset(long logEndOffset) {
        this.logEndOffset = logEndOffset;
        calculateLag();
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
        this.logStartOffsetUpdatedAt = Timestamp.valueOf(java.time.LocalDateTime.now());
        calculateLag();
    }

    public void setMmOffset(long mmOffset) {
        this.mmOffset = mmOffset + 1;
        calculateLag();
    }

    private void calculateLag() {
        // Handle the case where mmOffset is 0 (mirror maker hasn't read any messages)
        // If start offset equals end offset, the topic is caught up (lag = 0)
        if (this.mmOffset == 0 && this.logStartOffset == this.logEndOffset) {
            this.lag = 0;
        } else if (this.logEndOffset - this.mmOffset >= 0) {
            this.lag = this.logEndOffset - this.mmOffset;
        }
    }
}
