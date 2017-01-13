package com.stitchdata.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Use this class to build messages to send to Stitch.
 *
 * <pre>
 *
 * {@code
 * StitchMessage message = StitchMessage.newUpsert()
 *   .withTableName("orders")
 *   .withKeyNames("customer_id", order_id")
 *   .withSequence(System.currentTimeMillis())
 *   .withData(data);
 * }
 * </pre>
 */
public class StitchMessage {

    public static enum Action { UPSERT, SWITCH_VIEW };

    private Action action;
    private String tableName;
    private long tableVersion;
    private List<String> keyNames;
    private long sequence;
    private Map data;

    public StitchMessage() {

    }

    /**
     * Return a new message with the action set to "upsert".
     *
     * @return new StitchMessage
     */
    public static StitchMessage newUpsert() {
        return new StitchMessage().withAction(Action.UPSERT);
    }

    /**
     * Set the action and return this message.
     *
     * @param action the action
     * @return this object
     */
    public StitchMessage withAction(Action action) {
        this.action = action;
        return this;
    }

    /**
     * Set the name of the table. You must set the table name, either
     * per-message with this function, or by setting it on the client
     * with {@link StitchClientBuilder#withTableName(String)}.
     *
     * @param tableName the name of the table
     * @return this object
     */
    public StitchMessage withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * EXPERIMENTAL Set the version of the table, for use with "full
     * table" replication. This allows you to associate a table
     * version number with each message, and later atomically switch
     * versions of the table using a message with action {@link
     * Action#SWITCH_VIEW}. This is an experimental
     * feature and the behavior may change without notice.
     *
     * @param tableVersion the version number of the table
     * @return this object
     */
    public StitchMessage withTableVersion(long tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    /**
     * Set the names of the key fields for this record. You must
     * specify key names, either per-message with this function, or by
     * setting it on the client with {@link
     * StitchClientBuilder#withKeyNames(List)}.
     *
     * @param keyNames list of key fieldnames, which must exist in the data map
     * @return this object
     */
    public StitchMessage withKeyNames(List<String> keyNames) {
        this.keyNames = keyNames;
        return this;
    }

    /**
     * Set the names of the key fields for this record. You must
     * specify key names, either per-message with this function, or by
     * setting it on the client with {@link
     * StitchClientBuilder#withKeyNames(List)}.
     *
     * @param keyNames key fieldnames, which must exist in the data map
     * @return this object
     */
    public StitchMessage withKeyNames(String... keyNames) {
        return withKeyNames(Arrays.asList(keyNames));
    }

    /**
     * Set the name of the key field for this record, for use when
     * there is only one key field. You must specify key names, either
     * per-message with this function, or by setting it on the client
     * with {@link StitchClientBuilder#withKeyNames(List)}.
     *
     * @param keyName the field name, which must exist in the data map
     * @return this object
     */
    public StitchMessage withKeyName(String keyName) {
        return withKeyNames(keyName);
    }

    /**
     * Set the sequence number for the message. The sequence number is
     * used to determine the ordering in which upserts for the same
     * key are applied. When stitch loads a record into the warehouse,
     * it will check to see if there is already a record with the same
     * values for the key fields. If there is already a record, Stitch
     * will only update it if the sequence number on the incoming
     * record is greater than the sequenc enumber of the existing
     * record.
     *
     * For example, suppose we send in the following messages:
     *
     * <pre>
     * {@code
     * HashMap data = new HashMap();
     * data.put("order_id", 123);
     * data.put("status", "pending");
     *
     * StitchMessage a = StitchMessage.newUpsert()
     *   .withTableName("orders")
     *   .withKeyNames("order_id")
     *   .withData(data)
     *   .withSequence(1);
     *
     * StitchMessage b = StitchMessage.newUpsert()
     *   .withTableName("orders")
     *   .withKeyNames("order_id")
     *   .withData(data)
     *   .withSequence(2);
     *}
     * </pre>
     *
     * Regardless of the order in which the records are processed by
     * the loader, the end result will be that "status" for order
     * number 123 will be "completed".
     *
     * @param sequence sequence number
     * @return this object
     */
    public StitchMessage withSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    /**
     * Set the data map. This will not modify the contents of the map,
     * though it will retain a pointer to it. The caller should not
     * modify the map until after the message is pushed via {@link
     * StitchClient#push(StitchMessage)}. After that point, it is safe to
     * modify and reuse the map.
     *
     * @param data body of the message
     * @return this object
     */
    public StitchMessage withData(Map data) {
        this.data = data;
        return this;
    }

    public Action getAction() {
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
