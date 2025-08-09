package cc.mq.broker.meta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class IndexedQueue {

    public static final int CQ_STORE_UNIT_SIZE = 20;

    private static final Integer FILE_SIZE = 6 * 1024 * 1024;

    private static final Integer MAX_POSITION = (FILE_SIZE / 20) * 20;

    private String topic;

    private Integer queueId;

    private List<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    private ByteBuffer byteBufferIndex;

    /**
     * 这个是当前写入位置，值为 0,1,2,3,4,5. 用户消费者消费时使用
     */
    private AtomicLong tareIndex = new AtomicLong();


    public IndexedQueue(final String topic, final Integer queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public Boolean write(IndexedMeta indexed) throws IOException {

        //这里需要判断写入的是否超出，需要滚动生成新文件
        MappedFile mappedFile = getLastMappedFile(indexed.getLength());

        byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(indexed.getOffset());
        this.byteBufferIndex.putInt(indexed.getLength());
        this.byteBufferIndex.putLong(indexed.getTag());

        return mappedFile.appendMessage(byteBufferIndex.array());
    }

    public IndexedMeta get(final Integer consumeOffset) {

        if (consumeOffset > tareIndex.get()) {
            return null;
        }

        int mode = (consumeOffset * CQ_STORE_UNIT_SIZE) % MAX_POSITION;

        Long fileOffset = (long) mode * MAX_POSITION;

        MappedFile mappedFile = MappedFile.findByFileOffset(mappedFiles, fileOffset);

        Integer msgOffset = consumeOffset * CQ_STORE_UNIT_SIZE % MAX_POSITION;

        byte[] content = mappedFile.getMessage(msgOffset, CQ_STORE_UNIT_SIZE);

        ByteBuffer buffer = ByteBuffer.wrap(content);
        long offset = buffer.getLong(); // 读取前 8 字节
        int size = buffer.getInt();  // 读取中间 4 字节
        long tag = buffer.getLong(); // 读取最后 8 字节

        IndexedMeta indexedMeta = new IndexedMeta();
        indexedMeta.setLength(size);
        indexedMeta.setOffset(offset);
        indexedMeta.setTopic(topic);
        indexedMeta.setQueueId(queueId);
        indexedMeta.setTag(tag);

        return indexedMeta;
    }

    public MappedFile getLastMappedFile(Integer msgSize) throws IOException {
        if (mappedFiles.isEmpty()) {
            // "topic/queueId/00000000000000000000";

            MappedFile mappedFile = MappedFile.createNew(converterPath(topic, queueId), 0L, FILE_SIZE);
            mappedFiles.add(mappedFile);
        }
        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);
        if (mappedFile.isFull(msgSize)) {
            mappedFile = MappedFile.createNew(converterPath(topic, queueId), mappedFile.getNextFileOffset(), FILE_SIZE);
            mappedFiles.add(mappedFile);
            return mappedFile;
        }
        return mappedFile;
    }

    public void close() {
        mappedFiles.forEach(MappedFile::close);
    }

    public Long getTareIndex() {
        return tareIndex.get();
    }

    public void calCurOffset() {
        if (mappedFiles.isEmpty()) {
            tareIndex.set(0L);
            return;
        }
        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);
        tareIndex.set(mappedFile.getFileOffset() + mappedFile.getLastWritePos() / 20);
    }

    public String converterPath(String topic, Integer queueId) {
        return String.format("%s/%s/", topic, queueId);
    }
}
