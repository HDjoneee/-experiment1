import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 实验一第二部分：使用 HDFS Java API 完成常用文件操作。
 *
 * 运行格式：
 *   hadoop jar build/experiment1.jar HdfsOperations <子命令> [参数]
 */
public class HdfsOperations {

    private static final SimpleDateFormat TIME_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            String command = args[0];
            switch (command) {
                case "upload":
                    requireArgumentCount(args, 4);
                    upload(fileSystem, args[1], new Path(args[2]), args[3]);
                    break;
                case "download":
                    requireArgumentCount(args, 3);
                    download(fileSystem, new Path(args[1]), args[2]);
                    break;
                case "cat":
                    requireArgumentCount(args, 2);
                    printFile(fileSystem, new Path(args[1]), configuration);
                    break;
                case "stat":
                    requireArgumentCount(args, 2);
                    printStatus(fileSystem, new Path(args[1]));
                    break;
                case "list-recursive":
                    requireArgumentCount(args, 2);
                    listRecursive(fileSystem, new Path(args[1]));
                    break;
                case "create-file":
                    requireArgumentCount(args, 2);
                    createFile(fileSystem, new Path(args[1]));
                    break;
                case "delete-file":
                    requireArgumentCount(args, 2);
                    deleteFile(fileSystem, new Path(args[1]));
                    break;
                case "create-dir":
                    requireArgumentCount(args, 2);
                    createDirectory(fileSystem, new Path(args[1]));
                    break;
                case "delete-dir":
                    requireArgumentCount(args, 2);
                    deleteEmptyDirectory(fileSystem, new Path(args[1]));
                    break;
                case "append":
                    requireArgumentCount(args, 4);
                    appendText(fileSystem, new Path(args[1]), args[2], args[3]);
                    break;
                case "move":
                    requireArgumentCount(args, 3);
                    move(fileSystem, new Path(args[1]), new Path(args[2]));
                    break;
                default:
                    System.err.println("未知子命令：" + command);
                    printUsage();
                    System.exit(1);
            }
        }
    }

    private static void upload(FileSystem fs, String localName, Path destination,
                               String mode) throws IOException {
        File localFile = new File(localName);
        if (!localFile.isFile()) {
            throw new IOException("本地文件不存在：" + localFile.getAbsolutePath());
        }

        createParentDirectory(fs, destination);
        Path localPath = new Path(localFile.getAbsolutePath());

        if (!fs.exists(destination)) {
            fs.copyFromLocalFile(false, false, localPath, destination);
            System.out.println("上传完成：" + destination);
            return;
        }

        if ("overwrite".equalsIgnoreCase(mode)) {
            fs.copyFromLocalFile(false, true, localPath, destination);
            System.out.println("已覆盖：" + destination);
        } else if ("append".equalsIgnoreCase(mode)) {
            try (FileInputStream input = new FileInputStream(localFile);
                 FSDataOutputStream output = fs.append(destination)) {
                IOUtils.copyBytes(input, output, 4096, false);
                output.hflush();
            }
            System.out.println("已追加到文件末尾：" + destination);
        } else {
            throw new IllegalArgumentException("上传模式必须是 overwrite 或 append");
        }
    }

    private static void download(FileSystem fs, Path source, String localName)
            throws IOException {
        ensureFile(fs, source);

        File requested = new File(localName);
        File target = chooseAvailableLocalFile(requested);
        File parent = target.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("无法创建本地目录：" + parent);
        }

        fs.copyToLocalFile(false, source, new Path(target.getAbsolutePath()), true);
        System.out.println("下载完成：" + target.getAbsolutePath());
    }

    private static File chooseAvailableLocalFile(File requested) {
        if (!requested.exists()) {
            return requested;
        }

        String name = requested.getName();
        int dot = name.lastIndexOf('.');
        String base = dot > 0 ? name.substring(0, dot) : name;
        String extension = dot > 0 ? name.substring(dot) : "";
        File parent = requested.getAbsoluteFile().getParentFile();

        int number = 1;
        File candidate;
        do {
            candidate = new File(parent, base + "-" + number + extension);
            number++;
        } while (candidate.exists());
        return candidate;
    }

    private static void printFile(FileSystem fs, Path path, Configuration conf)
            throws IOException {
        ensureFile(fs, path);
        try (FSDataInputStream input = fs.open(path)) {
            IOUtils.copyBytes(input, System.out, conf, false);
        }
        System.out.println();
    }

    private static void printStatus(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new IOException("路径不存在：" + path);
        }
        printOneStatus(fs.getFileStatus(path));
    }

    private static void listRecursive(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new IOException("路径不存在：" + path);
        }
        FileStatus status = fs.getFileStatus(path);
        printOneStatus(status);
        if (status.isDirectory()) {
            for (FileStatus child : fs.listStatus(path)) {
                listRecursive(fs, child.getPath());
            }
        }
    }

    private static void printOneStatus(FileStatus status) {
        String kind = status.isDirectory() ? "目录" : "文件";
        System.out.printf(
                "%s\t权限=%s\t大小=%d\t修改时间=%s\t访问时间=%s\t路径=%s%n",
                kind,
                status.getPermission(),
                status.getLen(),
                TIME_FORMAT.format(new Date(status.getModificationTime())),
                TIME_FORMAT.format(new Date(status.getAccessTime())),
                status.getPath());
    }

    private static void createFile(FileSystem fs, Path path) throws IOException {
        if (fs.exists(path)) {
            System.out.println("文件已经存在：" + path);
            return;
        }
        createParentDirectory(fs, path);
        try (FSDataOutputStream ignored = fs.create(path, false)) {
            // 创建空文件。
        }
        System.out.println("文件创建完成：" + path);
    }

    private static void deleteFile(FileSystem fs, Path path) throws IOException {
        ensureFile(fs, path);
        if (!fs.delete(path, false)) {
            throw new IOException("文件删除失败：" + path);
        }
        System.out.println("文件删除完成：" + path);
    }

    private static void createDirectory(FileSystem fs, Path path) throws IOException {
        if (fs.exists(path)) {
            if (!fs.getFileStatus(path).isDirectory()) {
                throw new IOException("同名路径是文件，不能创建目录：" + path);
            }
            System.out.println("目录已经存在：" + path);
            return;
        }
        if (!fs.mkdirs(path)) {
            throw new IOException("目录创建失败：" + path);
        }
        System.out.println("目录创建完成：" + path);
    }

    private static void deleteEmptyDirectory(FileSystem fs, Path path)
            throws IOException {
        if (!fs.exists(path)) {
            throw new IOException("目录不存在：" + path);
        }
        if (!fs.getFileStatus(path).isDirectory()) {
            throw new IOException("指定路径不是目录：" + path);
        }
        if (fs.listStatus(path).length != 0) {
            System.out.println("目录不为空，按照实验要求不删除：" + path);
            return;
        }
        if (!fs.delete(path, false)) {
            throw new IOException("目录删除失败：" + path);
        }
        System.out.println("空目录删除完成：" + path);
    }

    private static void appendText(FileSystem fs, Path path, String position,
                                   String text) throws IOException {
        byte[] content = (text + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        createParentDirectory(fs, path);

        if (!fs.exists(path)) {
            try (FSDataOutputStream output = fs.create(path, false)) {
                output.write(content);
            }
            System.out.println("文件原先不存在，已创建并写入内容：" + path);
            return;
        }
        ensureFile(fs, path);

        if ("tail".equalsIgnoreCase(position)) {
            try (FSDataOutputStream output = fs.append(path)) {
                output.write(content);
                output.hflush();
            }
            System.out.println("内容已追加到文件末尾：" + path);
        } else if ("head".equalsIgnoreCase(position)) {
            prependText(fs, path, content);
            System.out.println("内容已追加到文件开头：" + path);
        } else {
            throw new IllegalArgumentException("追加位置必须是 head 或 tail");
        }
    }

    private static void prependText(FileSystem fs, Path original, byte[] prefix)
            throws IOException {
        long marker = System.currentTimeMillis();
        Path temporary = new Path(original.toString() + ".tmp-" + marker);
        Path backup = new Path(original.toString() + ".bak-" + marker);
        FileStatus oldStatus = fs.getFileStatus(original);

        try {
            try (FSDataOutputStream output = fs.create(temporary, false);
                 FSDataInputStream input = fs.open(original)) {
                output.write(prefix);
                IOUtils.copyBytes(input, output, 4096, false);
            }
            fs.setPermission(temporary, oldStatus.getPermission());
            fs.setReplication(temporary, oldStatus.getReplication());

            if (!fs.rename(original, backup)) {
                throw new IOException("无法为原文件建立临时备份：" + original);
            }
            if (!fs.rename(temporary, original)) {
                fs.rename(backup, original);
                throw new IOException("无法用新文件替换原文件：" + original);
            }
            fs.delete(backup, false);
        } finally {
            if (fs.exists(temporary)) {
                fs.delete(temporary, false);
            }
        }
    }

    private static void move(FileSystem fs, Path source, Path destination)
            throws IOException {
        if (!fs.exists(source)) {
            throw new IOException("源路径不存在：" + source);
        }
        if (fs.exists(destination)) {
            throw new IOException("目标路径已经存在：" + destination);
        }
        createParentDirectory(fs, destination);
        if (!fs.rename(source, destination)) {
            throw new IOException("移动失败：" + source + " -> " + destination);
        }
        System.out.println("移动完成：" + source + " -> " + destination);
    }

    private static void createParentDirectory(FileSystem fs, Path path)
            throws IOException {
        Path parent = path.getParent();
        if (parent != null && !fs.exists(parent) && !fs.mkdirs(parent)) {
            throw new IOException("无法创建父目录：" + parent);
        }
    }

    private static void ensureFile(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new IOException("文件不存在：" + path);
        }
        if (!fs.getFileStatus(path).isFile()) {
            throw new IOException("指定路径不是文件：" + path);
        }
    }

    private static void requireArgumentCount(String[] args, int count) {
        if (args.length != count) {
            printUsage();
            throw new IllegalArgumentException(
                    "参数数量错误：需要 " + count + " 个参数，实际得到 " + args.length + " 个");
        }
    }

    private static void printUsage() {
        System.err.println("用法：");
        System.err.println("  upload <本地文件> <HDFS文件> <overwrite|append>");
        System.err.println("  download <HDFS文件> <本地文件>");
        System.err.println("  cat <HDFS文件>");
        System.err.println("  stat <HDFS路径>");
        System.err.println("  list-recursive <HDFS路径>");
        System.err.println("  create-file <HDFS文件>");
        System.err.println("  delete-file <HDFS文件>");
        System.err.println("  create-dir <HDFS目录>");
        System.err.println("  delete-dir <HDFS目录>");
        System.err.println("  append <HDFS文件> <head|tail> <文本内容>");
        System.err.println("  move <HDFS源路径> <HDFS目标路径>");
    }
}

