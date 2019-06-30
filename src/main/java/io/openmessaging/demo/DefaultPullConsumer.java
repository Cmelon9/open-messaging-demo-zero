package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.consumer.BufferService;
import io.openmessaging.demo.consumer.MessageReader;
import io.openmessaging.demo.serializer.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class DefaultPullConsumer implements PullConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPullConsumer.class);
    
    private KeyValue properties;
    private BufferService bufferService;
    private ArrayList<MessageReader> readers = new ArrayList<>();
    private int pollIndex = 0;
    private int count = 0;

    private MessageSerializer deserializer = new MessageSerializer(); // thread local
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.bufferService = BufferService.getInstance(properties.getString("STORE_PATH"));
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public synchronized Message poll() {
        MessageReader reader = readers.get(pollIndex);
        Message message = reader.readMessage();
        while (message == null) {
            readers.remove(pollIndex);
            if (readers.isEmpty()) return null;
            pollIndex = pollIndex % readers.size();
            reader = readers.get(pollIndex);
            message = reader.readMessage();
        }
        if ((++count & 0x3f) == 0) { // change buffer every 64 messages
            pollIndex = (pollIndex + 1) % readers.size();
            
            if (Constants.ENABLE_MESSAGE_SAMPLING) {
                // FOR DEBUG: Sample and print message every 2^17 (~131K) messages
                if ((count & 0x1ffff) == 0) {
                    logger.info("Sampled message: {}", message.toString());
                }
            }
        }
        return message;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        readers.add(new MessageReader(queueName, true, bufferService, deserializer));
        for (String topic: topics) {
            readers.add(new MessageReader(topic, false, bufferService, deserializer));
        }
    }
}
