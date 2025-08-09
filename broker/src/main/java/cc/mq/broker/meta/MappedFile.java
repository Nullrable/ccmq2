package cc.mq.broker.meta;

import cc.mq.broker.util.UtilAll;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author nhsoft.lsd
 */
@Setter
@Getter
@Slf4j
public class MappedFile {

    private MappedByteBuffer buffer;

    private FileChannel fileChannel;

    private RandomAccessFile file;

    private Long fileOffset;

    private Integer fileSize;

    private AtomicInteger lastWritePosition;

    public MappedFile(final MappedByteBuffer buffer, final FileChannel fileChannel, final RandomAccessFile file, final Long fileOffset, final Integer fileSize) {
        this.buffer = buffer;
        this.fileChannel = fileChannel;
        this.file = file;
        this.fileOffset = fileOffset;
        this.fileSize = fileSize;
        this.lastWritePosition = new AtomicInteger(0);
    }

    public boolean isFull(Integer writeFileSize) {
        int curPos = buffer.position();
        return curPos + writeFileSize >= fileSize;
    }

    protected static MappedFile createNew(final String path, final Long fileOffset, final Integer fileSize) throws IOException {

        String pathNew = path + UtilAll.offset2FileName(fileOffset);

        //获取目录下文件的最大值
        RandomAccessFile file = new RandomAccessFile(pathNew, "rw");
        FileChannel fileChannel = file.getChannel();

        //申请1MB 的内存空间跟文件映射
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

        return new MappedFile(buffer, fileChannel, file, fileOffset, fileSize);
    }

    public void appendMessageWhenFull(byte[] data) {
        int currentPos = buffer.position();

        int remaining = buffer.limit() - currentPos;

        this.buffer.putInt(remaining);

        this.buffer.put(data);
    }

    public boolean appendMessage(byte[] data) {
        if ((lastWritePosition.get() + data.length) <= fileSize) {
            try {
                this.buffer.limit(fileSize);
                this.buffer.position(lastWritePosition.get());
                this.buffer.put(ByteBuffer.wrap(data, 0, data.length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            lastWritePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    public Long getNextFileOffset() {
        return fileOffset + fileSize;
    }

    public void close() {

        try {
            file.close();
        } catch (IOException e) {
            log.warn("Error occurred when close file.");
        }

        try {
            fileChannel.close();
        } catch (IOException e) {
            log.warn("Error occurred when close file channel.");
        }
    }

    public byte[] getMessage(final Integer pos, final Integer size) {

        byte[] strBytes = new byte[size];

        ByteBuffer byteBuffer = this.buffer.slice();
        byteBuffer.position(pos);
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);

        byteBuffer.get(strBytes);

        return strBytes;
    }

    public Integer getLastWritePos() {
        return lastWritePosition.get();
    }

    public static MappedFile findByFileOffset(List<MappedFile> mappedFiles, final Long fileOffset) {
        return mappedFiles.stream().filter(e -> e.getFileOffset().equals(fileOffset)).findFirst().orElse(null);
    }
}
