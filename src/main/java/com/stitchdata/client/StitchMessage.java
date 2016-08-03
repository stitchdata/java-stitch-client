package com.stitchdata.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stitchdata.client.StitchClient.Action;

public class StitchMessage {

    private String action;
    private String tableName;
    private long tableVersion;
    private List<String> keyNames;
    private long sequence;
    private Map data;

    public StitchMessage withAction(String action) {
        this.action = action;
        return this;
    }

    public StitchMessage withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public StitchMessage withTableVersion(long tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    public StitchMessage withKeyNames(List<String> keyNames) {
        this.keyNames = keyNames;
        return this;
    }

    public StitchMessage withKeyNames(String... keyNames) {
        return withKeyNames(Arrays.asList(keyNames));
    }

    public StitchMessage withKeyName(String keyName) {
        return withKeyNames(keyName);
    }

    public StitchMessage withSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public StitchMessage withData(Map data) {
        this.data = data;
        return this;
    }

    public byte[] toBytes() {
        return null;
    }

    public String getAction() {
        return action;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getTableVersion() {
        return tableVersion;
    }

    public List<String> getKeyNames() {
        return keyNames;
    }

    public Long getSequence() {
        return sequence;
    }

    public Map getData() {
        return data;
    }

}
