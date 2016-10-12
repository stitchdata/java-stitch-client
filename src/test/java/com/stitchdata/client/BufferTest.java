package com.stitchdata.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.ByteArrayOutputStream;
import com.cognitect.transit.Writer;
import com.cognitect.transit.WriteHandler;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;
import static org.junit.Assert.*;

public class BufferTest  {

    static final Map tinyRecord = new HashMap();
    static final Map bigRecord = new HashMap();
    static final Map hugeRecord = new HashMap();

    static final String tinyResult = "[\"^ \",\"a\",\"b\"]";

    private Buffer buffer = null;

    @BeforeClass
    public static void initTestRecords() {
        tinyRecord.put("a", "b");

        char bigRecordChars[] = new char[1500000];
        Arrays.fill(bigRecordChars, 'b');
        bigRecord.put("a", new String(bigRecordChars));

        char hugeRecordChars[] = new char[5000000];
        Arrays.fill(hugeRecordChars, 'b');
        hugeRecord.put("a", new String(hugeRecordChars));
    }

    @Before
    public void initBuffer() {
        buffer = new Buffer();
    }

    public void putMessage(Map record) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(record);
        buffer.put(new Buffer.Entry(baos.toByteArray(), null));
    }

    public String takeBatchBody(int batchSizeBytes, int batchDelayMillis)
        throws UnsupportedEncodingException {
        List<Buffer.Entry> entries = buffer.take(batchSizeBytes, batchDelayMillis);
        return entries == null ? null : StitchClient.serializeEntries(entries);
    }

    @Test
    public void testSingleRecordAvailableImmediately() throws IOException {
        putMessage(tinyRecord);
        assertEquals("[" + tinyResult + "]", takeBatchBody(0, 0));
    }

    @Test
    public void testWithholdUntilBytesAvailable() throws IOException {
        putMessage(tinyRecord);
        assertNull(takeBatchBody(36, Integer.MAX_VALUE));
        putMessage(tinyRecord);
        assertNull(takeBatchBody(36, Integer.MAX_VALUE));
        putMessage(tinyRecord);
        assertEquals(
            "[" + tinyResult + "," + tinyResult + "," + tinyResult + "]",
            takeBatchBody(36, Integer.MAX_VALUE));
    }

    @Test
    public void testBufferEmptyAfterBatch() throws IOException {
        putMessage(tinyRecord);
        putMessage(tinyRecord);
        putMessage(tinyRecord);
        assertNotNull(takeBatchBody(36, Integer.MAX_VALUE));
        assertNull(takeBatchBody(36, Integer.MAX_VALUE));
    }

    @Test
    public void testDoesNotExceedMaxBatchSize() throws IOException {
        putMessage(bigRecord);
        putMessage(bigRecord);
        putMessage(bigRecord);

        String batch1 = takeBatchBody(0, 0);
        String batch2 = takeBatchBody(0, 0);
        String batch3 = takeBatchBody(0, 0);

        System.out.println("Batch1 size is " + batch1.length());

        assertTrue(batch1.length() < Buffer.MAX_BATCH_SIZE_BYTES);
        assertTrue(batch2.length() < batch1.length());
        assertNull(batch3);
    }

    @Test(expected=IllegalArgumentException.class)
    public void assertCantPutRecordLargerThanMaxMessageSize() {
        putMessage(hugeRecord);
    }

    @Test
    public void testTriggerBatchAt10kMessages() throws IOException {
        for (int i = 0; i < 9999; i++) {
            putMessage(tinyRecord);
        }
        assertNull(takeBatchBody(Buffer.MAX_BATCH_SIZE_BYTES, 60000));
        putMessage(tinyRecord);
        assertNotNull(takeBatchBody(Buffer.MAX_BATCH_SIZE_BYTES, 60000));
    }

}
