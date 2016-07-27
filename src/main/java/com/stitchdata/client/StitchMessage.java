package com.stitchdata.client;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class StitchMessage {

    public enum Action { UPSERT, SWITCH_VIEW };

    private Action action;

    private Long clientId;
    private String token;
    private String namespace;
    private String tableName;
    private Long tableVersion;
    private List<String> keyNames;

    private Long sequence;
    private Map data;

    public StitchMessage() {

    }

    private StitchMessage(Action action, String namespace, String tableName, Long tableVersion, List<String> keyNames) {
        this.action = action;
        this.namespace = namespace;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
        this.keyNames = keyNames;
    }

    public StitchMessage withAction(Action action) {
        this.action = action;
        return this;
    }

    private StitchMessage withActionUpsert() {
        return withAction(Action.UPSERT);
    }

    private StitchMessage withActionSwitchView() {
        return withAction(Action.SWITCH_VIEW);
    }

    public StitchMessage withNamespace(String namespace) {
        this.namespace = namespace;
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

    public StitchMessage withKeyNames(Collection<String> keyNames) {
        this.keyNames = new ArrayList(keyNames);
        return this;
    }

    public StitchMessage withKeyNames(String... keyNames) {
        return withKeyNames(Arrays.asList(keyNames));
    }

    public StitchMessage withSequence(Long sequence) {
        this.sequence = sequence;
        return this;
    }

    public StitchMessage withData(Map data) {
        this.data = data;
        return this;
    }

    private void setRequiredField(Map map, String field, Object value) {
        if (value == null) {
            throw new RuntimeException(field + " must not be null");
        }
        map.put(field, value);
    }

    public Map toMap() {
        HashMap map = new HashMap();

        setRequiredField(map, "namespace", namespace);
        setRequiredField(map, "table_name", tableName);
        map.put("table_version", tableVersion);

        switch (action) {
        case UPSERT:
            map.put("action", "upsert");
            setRequiredField(map, "sequence", sequence);
            setRequiredField(map, "data", data);
            break;
        case SWITCH_VIEW:
            map.put("action", "switch_view");
            break;
        }
        return map;
    }

    public StitchMessage clone() {
        return new StitchMessage(action, namespace, tableName, tableVersion, keyNames);
    }

}
