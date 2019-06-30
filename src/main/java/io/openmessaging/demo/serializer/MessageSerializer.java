package io.openmessaging.demo.serializer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Message Serializer
 *
 * Created by yfu on 5/21/17.
 */
public final class MessageSerializer {

    private final static byte NUL = (byte)0;

    private final static int START_K = 16;
    
    private final static byte KEY_HEADER_KEY = (byte)0xff;
    private final static byte KEY_PRO_OFFSET = (byte)0xfe;
    private final static String HEADER_KEY = "MessageId";
    private final static String PRO_OFFSET = "PRO_OFFSET";
    private final static String PRODUCER = "PRODUCER";
    
    // ATTENTION! Because of this, all reading functions should only be called in single thread
    private final byte[] buf = new byte[101 * 1024];

    public int getSize(Message message) {
        byte[] body = ((BytesMessage) message).getBody();
        return body.length + 1 + getKeyValueSize(message.headers()) + getKeyValueSize(message.properties());
    }

    private int getKeyValueSize(KeyValue keyValue) {
        int size = 1;
        if (keyValue == null) return size;
        Set<String> keySet = keyValue.keySet();
        for (String key : keySet) {
            String value = keyValue.getString(key);
            if (HEADER_KEY.equals(key)) {
                size += 2 + value.length();
            } else if (PRO_OFFSET.equals(key) && value.startsWith(PRODUCER)) {
                size += 2 + value.length() - PRODUCER.length();
            } else {
                // This is not correct when string contains non-ascii char
                // Correct but slower: key.getBytes().length
                size += 1 + key.length();
                size += 1 + value.length();
            }
        }
        return size;
    }

    public void write(ByteBuffer buffer, Message message) throws BufferOverflowException {
        // TODO: only support BytesMessage currently
        byte[] body = ((BytesMessage) message).getBody();
        buffer.put(body).put(NUL);  // C-style

        write(buffer, message.headers());
        write(buffer, message.properties());
    }

    private void write(ByteBuffer buffer, KeyValue keyValue) throws BufferOverflowException {
        if (keyValue == null) {
            buffer.put((byte)0);
            return;
        }
        Set<String> keySet = keyValue.keySet();
        int size = keySet.size();
        if (size > 127) {
            throw new RuntimeException("keyValue has too many entries (at most 127 entries), size=" + size);
        }
        buffer.put((byte)size);
        for (String key : keySet) {
            String value = keyValue.getString(key);
            if (HEADER_KEY.equals(key)) {
                buffer.put(KEY_HEADER_KEY);
                writeValue(buffer, value);
            } else if (PRO_OFFSET.equals(key) && value.startsWith(PRODUCER)) {
                buffer.put(KEY_PRO_OFFSET);
                writeValue(buffer, value.substring(PRODUCER.length()));
            } else {
                writeKey(buffer, key);
                writeValue(buffer, value);
            }
        }
    }

    // write short string (length <= 127)
    private void writeKey(ByteBuffer buffer, String string) {
        if (string.length() > 127) {
            throw new RuntimeException("key is too long (at most 127 bytes), length=" + string.length());
        }
        byte[] valueBytes = string.getBytes();
        buffer.put((byte)valueBytes.length);
        buffer.put(valueBytes);
    }

    // write short string (length <= 32767)
    private void writeValue(ByteBuffer buffer, String string) {
        if (string.length() > 32767) {
            throw new RuntimeException("value is too long (at most 32767 bytes), length=" + string.length());
        }
        byte[] valueBytes = string.getBytes();
        buffer.put(valueBytes).put(NUL); // C-style
    }
    
    public Message read(ByteBuffer buffer) throws BufferUnderflowException {
        if (!buffer.hasRemaining()) return null;
        
        byte[] body = readBody(buffer);
        if (body == null) return null;
        BytesMessage message = new DefaultBytesMessage(body);

        int numHeaders = buffer.get(); // one byte
        for (int i = 0; i < numHeaders; i++) {
            String key = readKey(buffer);
            String value = readValue(buffer);
            message.putHeaders(key, value);
        }

        int numProperties = buffer.get(); // one byte
        for (int i = 0; i < numProperties; i++) {
            byte length = buffer.get(); // one byte
            String key;
            if (length == KEY_HEADER_KEY) {
                key = HEADER_KEY;
            } else if (length == KEY_PRO_OFFSET) {
                key = PRO_OFFSET;
            } else {
                buffer.get(buf, 0, length);
                key = new String(buf, 0, length);
            }
            
            String value = readValue(buffer);
            if (length == KEY_PRO_OFFSET) {
                value = PRODUCER + value;
            }
            message.putProperties(key, value);
        }
        return message;
    }

    private String readKey(ByteBuffer buffer) throws BufferUnderflowException {
        byte length = buffer.get(); // one byte
        if (length == KEY_HEADER_KEY) {
            return HEADER_KEY;
        } else if (length == KEY_PRO_OFFSET) {
            return PRO_OFFSET;
        }
        buffer.get(buf, 0, length);
        return new String(buf, 0, length);
    }
    
    private String readValue(ByteBuffer buffer) throws BufferOverflowException {
        int position = readCStyleString(buffer);
        return new String(buf, 0, position);
    }
    
    private byte[] readBody(ByteBuffer buffer) {
        int position = readCStyleString(buffer);
        if (position == 0) return null;
        return Arrays.copyOf(buf, position);
    }
    
    private int readCStyleString(ByteBuffer buffer) {
        final int bufferPosition = buffer.position();
        int limit = 0;
        int position = 0;
        for (int k = START_K; position == limit; k <<= 1) {
            try {
                buffer.mark();
                buffer.get(buf, limit, k);
            } catch (BufferUnderflowException ex) {
                buffer.reset();
                k = buffer.remaining();
                buffer.get(buf, limit, k);
            }
            limit += k;
            while (buf[position] != NUL &&  position < limit) {
                position++;
            }
        }
        buffer.position(bufferPosition + position + 1);
        return position;
    }
}
