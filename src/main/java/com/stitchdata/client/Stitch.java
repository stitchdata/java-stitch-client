package com.stitchdata.client;

public interface Stitch {
    public static final String PUSH_URL =  "https://pipeline-gateway.rjmetrics.com/push";

    public static class Action {
        public static final String UPSERT = "upsert";
        public static final String SWITCH_VIEW = "switch_view";
    }

    public static class Field {
        public static final String CLIENT_ID = "client_id";
        public static final String NAMESPACE = "namespace";
        public static final String ACTION = "action";
        public static final String TABLE_NAME = "table_name";
        public static final String TABLE_VERSION = "table_version";
        public static final String KEY_NAMES = "key_names";
        public static final String SEQUENCE = "sequence";
        public static final String DATA = "data";
    }

    public static final int DEFAULT_MAX_FLUSH_INTERVAL_MILLIS = 10000;
    public static final int DEFAULT_MAX_FLUSH_BYTES = 4194304;
    public static final int DEFAULT_MAX_FLUSH_RECORDS = 20000;
    public static final int HTTP_CONNECT_TIMEOUT = 1000 * 60 * 2;
}
