package io.openmessaging.demo.producer;

import io.openmessaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Managers of all buckets
 * 
 * Created by yfu on 5/27/17.
 */
public class BucketManager {
    private static final Logger logger = LoggerFactory.getLogger(BucketManager.class);

    private volatile static BucketManager instance;

    private final BufferService bufferService;
    
    private final HashMap<String, BucketWriter> bucketMap = new HashMap<>();
    
    public BucketManager(String storePath) {
        bufferService = new BufferService(storePath);
        
        // In case the storePath does not exist
        Paths.get(storePath).toFile().mkdirs();
    }
    
    public static BucketManager getInstance(String storePath) {
        if (instance == null) {
            synchronized (BucketManager.class) {
                if (instance == null) {
                    instance = new BucketManager(storePath);
                }
            }
        }
        return instance;
    }
    
    public void putMessage(String bucket, Message message, ByteBuffer localBuffer) {
        BucketWriter store = bucketMap.get(bucket);
        if (store == null) {
            synchronized (bucketMap) {
                if (!bucketMap.containsKey(bucket)) {
                    bucketMap.put(bucket, new BucketWriter(bucket, bufferService));
                }
            }
            store = bucketMap.get(bucket);
        }

        store.putMessage(message, localBuffer);
    }
}
