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

    private enum Action { UPSERT, SWITCH_VIEW };

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

    private StitchMessage(Action action, Long clientId, String token, String namespace, String tableName, Long tableVersion, List<String> keyNames, Long sequence, Map data) {
        this.action = action;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
        this.keyNames = keyNames;
        this.sequence = sequence;
        this.data = data;
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

    public StitchMessage withClientId(long clientId) {
        this.clientId = clientId;
        return this;
    }

    public StitchMessage withToken(String token) {
        this.token = token;
        return this;
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
        this.data = (Map)copyData(data);
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

        setRequiredField(map, "action", action);
        setRequiredField(map, "client_id", clientId);
        setRequiredField(map, "token", token);
        setRequiredField(map, "namespace", namespace);
        setRequiredField(map, "table_name", tableName);
        map.put("table_version", tableVersion);

        switch (action) {
        case UPSERT:
            setRequiredField(map, "sequence", sequence);
            setRequiredField(map, "data", data);
            break;
        case SWITCH_VIEW:
            break;
        }
        return map;
    }

    private Object copyData(Object data) {

        if (data == null) {
            return data;
        }

        if (data instanceof Map) {
            HashMap result = new HashMap();
            for (Map.Entry e : (Set<Map.Entry>)((Map)data).entrySet()) {
                if (! (e.getKey() instanceof String) ) {
                    throw new IllegalArgumentException(
                        "Map keys must be string, got " + k +
                        " which is of type " + k.getClass());
                }
                result.put(e.getKey(), copyData(e.getValue()));
            }
            return result;
        }

        if (data instanceof List) {
            ArrayList result = new ArrayList();
            for (Object item : (List)data) {
                result.add(copyData(item));
            }
            return result;
        }

        else if (data instanceof String ||
                 data instanceof Boolean ||
                 data instanceof Number ||
                 data instanceof Date) {
            return data;
        }

        throw new IllegalArgumentException(
            "Don't know how to convert value '" + data + "', " +
            "which is of type " + data.getClass() + ", " +
            "to a Stitch datatype.");

    }

    public StitchMessage clone() {
        return new StitchMessage(action, clientId, token, namespace, tableName, tableVersion, keyNames, sequence, data);
    }

}
