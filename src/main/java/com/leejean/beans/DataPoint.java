// file: entity/DataPoint.java

package com.leejean.beans;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Data point entity class.
 */
public class DataPoint implements Serializable,Comparable<DataPoint> {
    private static final long serialVersionUID = 1L;

    private String id;
    private long timestamp;
    private double[] features;
    private int label;
    private Map<String, Object> metadata;
    private long originalSequence;       // 原始数据顺序号，用于下游按原始顺序排序 / original data sequence number
    private double anomalyScore = 0.0;
    private boolean isAnomaly = false;



    public int targetPartition;
    private int flag;

    // Tracking information
    public int sourceTaskId;
    public String sourceHost;
    public long partitionStartTime;
    public long partitionEndTime;
    public int[] lshSignature;

    public DataPoint() {
        this.metadata = new HashMap<>();
    }

    public DataPoint(String id, long timestamp, double[] features, int label) {
        this.id = id;
        this.timestamp = timestamp;
        this.features = features;
        this.label = label;
        this.metadata = new HashMap<>();
    }


    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public double[] getFeatures() { return features; }
    public void setFeatures(double[] features) { this.features = features; }
    public int getLabel() { return label; }
    public void setLabel(int label) { this.label = label; }
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    public double getAnomalyScore() { return anomalyScore; }
    public void setAnomalyScore(double anomalyScore) { this.anomalyScore = anomalyScore; }
    public boolean isAnomaly() { return isAnomaly; }
    public void setPredictionResult(boolean predictionResult) { this.isAnomaly = predictionResult; }
    public int getTargetPartition() { return targetPartition; }
    public void setTargetPartition(int targetPartition) { this.targetPartition = targetPartition; }
    public long getOriginalSequence() { return originalSequence; }
    public void setOriginalSequence(long originalSequence) { this.originalSequence = originalSequence; }
    public int getFlag() { return flag; }
    public void setFlag(int flag) { this.flag = flag; }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public void setAnomaly(boolean anomaly) {
        isAnomaly = anomaly;
    }

    public int getSourceTaskId() {
        return sourceTaskId;
    }

    public void setSourceTaskId(int sourceTaskId) {
        this.sourceTaskId = sourceTaskId;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public long getPartitionStartTime() {
        return partitionStartTime;
    }

    public void setPartitionStartTime(long partitionStartTime) {
        this.partitionStartTime = partitionStartTime;
    }

    public long getPartitionEndTime() {
        return partitionEndTime;
    }

    public void setPartitionEndTime(long partitionEndTime) {
        this.partitionEndTime = partitionEndTime;
    }

    public int[] getLshSignature() {
        return lshSignature;
    }

    public void setLshSignature(int[] lshSignature) {
        this.lshSignature = lshSignature;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        return String.format("DataPoint{id='%s', time=%s, label=%s,features=%s}",
                id,
                sdf.format(new Date(timestamp)),
                label,
                Arrays.toString(features)
        );
    }



    public int dimensions() {
        return features.length;
    }

    @Override
    public int compareTo(DataPoint other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

}