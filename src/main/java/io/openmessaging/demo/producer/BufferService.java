package io.openmessaging.demo.producer;

import io.openmessaging.demo.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 * Pool of memory-mapped buffer
 * <p>
 * Created by yfu on 5/27/17.
 */
public class BufferService {
    private static Logger logger = LoggerFactory.getLogger(BufferService.class);

    private final String storePath;
    private final HashMap<String, MappedFile> fileChannels = new HashMap<>(100);

    // Must call this before starting to put data
    public BufferService(String storePath) {
        this.storePath = storePath;
    }

    public ByteBuffer getBuffer(String bucket, int position) {

        MappedFile mappedFile = fileChannels.get(bucket);
        if (mappedFile == null) {
            synchronized (fileChannels) {
                if ((mappedFile = fileChannels.get(bucket)) == null) {
                    Path path = Paths.get(storePath, bucket);
                    try {
                        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                        mappedFile = new MappedFile(channel);
                    } catch (IOException ex) {
                        throw new RuntimeException("File channel open file failed", ex);
                    }
                    fileChannels.put(bucket, mappedFile);
                }
            }
        }

        // Allocating next buffer (size == BUFFER_SIZE)
        final int droppedPages = position >> 12;
        final int newPosition = position & 0xfff;
        mappedFile.currentMapStart += droppedPages << 12;
        mappedFile.currentMapEnd += Constants.BUFFER_SIZE;

        MappedByteBuffer byteBuffer;
        try {
            byteBuffer = mappedFile.channel.map(FileChannel.MapMode.READ_WRITE,
                    mappedFile.currentMapStart, mappedFile.currentMapEnd - mappedFile.currentMapStart);
            byteBuffer.position(newPosition);
        } catch (IOException ex) {
            throw new RuntimeException("File channel open file failed", ex);
        }

        logger.info("Allocated MappedByteBuffer (write)  bucket={} file_offset={} map_start={} map_end={}",
                bucket, position, mappedFile.currentMapStart, mappedFile.currentMapEnd);
        return byteBuffer;
    }

    private static class MappedFile {
        final FileChannel channel;

        int currentMapStart = 0;
        int currentMapEnd = 0;

        MappedFile(FileChannel channel) {
            this.channel = channel;
        }
    }
}
