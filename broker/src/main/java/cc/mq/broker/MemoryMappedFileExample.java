package cc.mq.broker;

/**
 * @author nhsoft.lsd
 */

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;

public class MemoryMappedFileExample {

    private static final String MAGIC_CODE = "da2fdsfa";

    private static FileChannel fileChannel;
    private static RandomAccessFile file;

    public static void main(String[] args) throws Exception {
        read();
    }

    private static void read() throws IOException {
        // 将文件的前1024字节映射到内存中
        file = new RandomAccessFile("example.txt", "r");
        fileChannel = file.getChannel();


        long fileSize = (long) 1024 * 1024; //1MB 1,048,576

        //申请1MB 的内存空间跟文件映射
        MappedByteBuffer buffer = fileChannel.map(MapMode.READ_ONLY, 0, fileSize);

        buffer.position(1);

        // 读取一个字符串（先读取长度，再读取内容）
        byte[] strBytes = new byte[10];
        buffer.get(strBytes);          // 读取字符串内容
        String content = new String(strBytes, StandardCharsets.UTF_8);
        System.out.println(content);
    }

    private static void write(byte[] bytes, int lastPosition, int size) throws IOException {

        // 打开文件

        file = new RandomAccessFile("example.txt", "rw");
        fileChannel = file.getChannel();

        long fileSize = 1L * 1024 * 1024; //1MB

        //申请1MB 的内存空间跟文件映射
        MappedByteBuffer buffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

        // 设置文件大小（可选，但推荐）
        file.setLength(fileSize);

        String content = "Hello World Next Day ";

        int lastPos = 0;
        for (int i = 0; i < 10; i++) {
            String newContent = MAGIC_CODE + content + i;
            byte[] newBytes = newContent.getBytes(StandardCharsets.UTF_8);

            buffer.position(lastPos);
            buffer.put(newBytes);

            buffer.force();
            lastPos = buffer.position();
        }

        // 关闭资源
        fileChannel.close();
        file.close();
    }
}
