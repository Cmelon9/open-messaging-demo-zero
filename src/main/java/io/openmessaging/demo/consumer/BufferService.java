package io.openmessaging.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 * Buffer Service
 * <p>
 * Created by yfu on 5/27/17.
 */
public class BufferService {
    private static Logger logger = LoggerFactory.getLogger(BufferService.class);

    private volatile static BufferService instance;

    private final String storePath;
    private final HashMap<String, FileChannel> fileChannels = new HashMap<>(100);

    public BufferService(String storePath) {
        this.storePath = storePath;
        
        // In case the storePath does not exist
        Paths.get(storePath).toFile().mkdirs();
    }

    public static BufferService getInstance(String storePath) {
        if (instance == null) {
            synchronized (BufferService.class) {
                if (instance == null) {
                    instance = new BufferService(storePath);
                }
            }
        }
        return instance;
    }
    
    public ByteBuffer getBuffer(String bucket) {

        FileChannel channel = fileChannels.get(bucket);
        if (channel == null) {
            synchronized (fileChannels) {
                if ((channel = fileChannels.get(bucket)) == null) {
                    try {
                        channel = FileChannel.open(Paths.get(storePath, bucket), StandardOpenOption.READ);
                    } catch (IOException ex) {
                        throw new RuntimeException("File channel open file failed", ex);
                    }
                    fileChannels.put(bucket, channel);
                }
            }
        }

        MappedByteBuffer buffer;
        try {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        } catch (IOException ex) {
            throw new RuntimeException("File channel open file failed", ex);
        }
        
        logger.info("Allocated MappedByteBuffer (read)  bucket={} size={}", bucket, buffer.limit());
        return buffer;
    }
}
