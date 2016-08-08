package com.stitchdata.client;

import java.io.IOException;
import java.util.Arrays;
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

    @Test
    public void testSingleRecordAvailableImmediately() throws IOException {
        buffer.putMessage(tinyRecord);
        assertEquals("[" + tinyResult + "]", buffer.takeBatch(0, 0));
    }

    @Test
    public void testWithholdUntilBytesAvailable() throws IOException {
        buffer.putMessage(tinyRecord);
        assertNull(buffer.takeBatch(36, Integer.MAX_VALUE));
        buffer.putMessage(tinyRecord);
        assertNull(buffer.takeBatch(36, Integer.MAX_VALUE));
        buffer.putMessage(tinyRecord);
        assertEquals(
            "[" + tinyResult + "," + tinyResult + "," + tinyResult + "]",
            buffer.takeBatch(36, Integer.MAX_VALUE));
    }

    @Test
    public void testBufferEmptyAfterBatch() throws IOException {
        buffer.putMessage(tinyRecord);
        buffer.putMessage(tinyRecord);
        buffer.putMessage(tinyRecord);
        assertNotNull(buffer.takeBatch(36, Integer.MAX_VALUE));
        assertNull(buffer.takeBatch(36, Integer.MAX_VALUE));
    }

    @Test
    public void testDoesNotExceedMaxBatchSize() throws IOException {
        buffer.putMessage(bigRecord);
        buffer.putMessage(bigRecord);
        buffer.putMessage(bigRecord);

        String batch1 = buffer.takeBatch(0, 0);
        String batch2 = buffer.takeBatch(0, 0);
        String batch3 = buffer.takeBatch(0, 0);

        System.out.println("Batch1 size is " + batch1.length());

        assertTrue(batch1.length() < Buffer.MAX_BATCH_SIZE_BYTES);
        assertTrue(batch2.length() < batch1.length());
        assertNull(batch3);
    }

    @Test(expected=IllegalArgumentException.class)
    public void assertCantPutRecordLargerThanMaxMessageSize() {
        buffer.putMessage(hugeRecord);
    }

    @Test
    public void testTriggerBatchAt10kMessages() throws IOException {
        for (int i = 0; i < 9999; i++) {
            buffer.putMessage(tinyRecord);
        }
        assertNull(buffer.takeBatch(Buffer.MAX_BATCH_SIZE_BYTES, 60000));
        buffer.putMessage(tinyRecord);
        assertNotNull(buffer.takeBatch(Buffer.MAX_BATCH_SIZE_BYTES, 60000));
    }

}
