package cc.mq.broker.meta;

import jakarta.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
@Slf4j
public class CommitedLog {

    private static final String END_FILE_MAGIC_CODE = "ds23dsfs";

    private static final Integer END_FILE_MAGIC_CODE_LENGTH = 8;

    private static final Integer FILE_SIZE = 10 * 1024 * 1024;

    private static final String PATH = "/store/commitlog/";

    private List<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    @Resource
    private IndexedStore indexedStore;

    public byte[] get(final String topic, final Integer queueId, final Integer consumeOffset) throws IOException {

        IndexedMeta indexedMeta = indexedStore.get(topic, queueId, consumeOffset);
        if (indexedMeta == null) {
            return null;
        }

        long offset = indexedMeta.getOffset();

        long mode = offset % FILE_SIZE;

        long fileOffset = mode * FILE_SIZE;

        MappedFile mappedFile = MappedFile.findByFileOffset(mappedFiles, fileOffset);

        if (mappedFile == null) {
            log.error("topic {},  queueId {} not found", topic, queueId);
            return null;
        }

        long pos = offset - fileOffset;

        return mappedFile.getMessage((int) pos, indexedMeta.getLength());
    }

    public Boolean write(String topic, Integer queueId, byte[] bytes) throws IOException {

        MappedFile mappedFile = getLastMappedFile(bytes.length);

        Integer lastWritePos = mappedFile.getLastWritePosition().get();

        boolean success = mappedFile.appendMessage(bytes);

        if (!success) {
            return false;
        }

        long offset = mappedFile.getFileOffset() + lastWritePos;

        //写入索引
        IndexedMeta indexedLog = new IndexedMeta();
        indexedLog.setLength(bytes.length);
        indexedLog.setOffset(offset);
        indexedLog.setTopic(topic);
        indexedLog.setQueueId(queueId);

        return indexedStore.write(indexedLog);
    }

    public MappedFile getLastMappedFile(Integer msgSize) throws IOException {
        if (mappedFiles.isEmpty()) {

            MappedFile mappedFile = MappedFile.createNew(PATH, 0L, FILE_SIZE);
            mappedFiles.add(mappedFile);

            return mappedFile;
        }

        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);

        // 预留 8 个字节，用于存储文件结束魔数
        if (mappedFile.isFull(msgSize + END_FILE_MAGIC_CODE_LENGTH)) {
            mappedFile.appendMessageWhenFull(END_FILE_MAGIC_CODE.getBytes(StandardCharsets.UTF_8));
            mappedFile = MappedFile.createNew(PATH, mappedFile.getNextFileOffset(), FILE_SIZE);
            mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public void close() {
        mappedFiles.forEach(MappedFile::close);
    }
}
