# 实验一 HDFS Java API 与 MapReduce 程序

本目录包含实验一中需要编程的两部分：

- 第二部分：`HdfsOperations.java`，通过 HDFS Java API 实现常用文件操作。
- 第三部分：3 个 MapReduce 程序，分别完成合并去重、整数排序和祖孙关系挖掘。

以下命令假设：

- 虚拟机用户是 `hadoop`。
- Hadoop 安装在 `/usr/local/hadoop`。
- HDFS 用户目录是 `/user/hadoop`。
- 项目被传到 `/home/hadoop/experiment1-hadoop-java`。

如果你的用户名或 Hadoop 安装目录不同，请替换相应路径。

## 一 将项目传入虚拟机

在 Windows PowerShell 中进入本项目的上一级目录，然后执行：

```powershell
cd "C:\Users\huashuo\Desktop\大数据管理与分析experiments"
scp -r ".\experiment1-hadoop-java" hadoop@虚拟机IP:/home/hadoop/
```

把命令中的 `虚拟机IP` 替换成实际地址，例如 `192.168.56.101`。如果使用 VMware 共享文件夹，也可以直接把整个 `experiment1-hadoop-java` 目录复制到 `/home/hadoop/`。

也可以只传已经生成的 ZIP 文件。在 Windows PowerShell 中执行：

```powershell
scp ".\experiment1-hadoop-java.zip" hadoop@虚拟机IP:/home/hadoop/
```

然后在虚拟机终端中解压：

```bash
cd /home/hadoop
```

```bash
unzip experiment1-hadoop-java.zip
```

## 二 登录虚拟机并检查环境

下面的命令都在虚拟机终端中执行。

进入项目目录：

```bash
cd /home/hadoop/experiment1-hadoop-java
```

设置 Hadoop 环境变量：

```bash
export HADOOP_HOME=/usr/local/hadoop
```

```bash
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
```

检查 Java、Java 编译器和 Hadoop：

```bash
java -version
```

```bash
javac -version
```

```bash
hadoop version
```

如果 `javac` 不存在，说明虚拟机只安装了 JRE，需要安装与当前环境匹配的 JDK 后再继续。

## 三 启动 Hadoop

启动 HDFS：

```bash
start-dfs.sh
```

启动 YARN：

```bash
start-yarn.sh
```

查看 Java 进程：

```bash
jps
```

伪分布式环境中通常应看到 `NameNode`、`DataNode`、`SecondaryNameNode`、`ResourceManager` 和 `NodeManager`。

创建 HDFS 用户目录：

```bash
hdfs dfs -mkdir -p /user/hadoop
```

检查 HDFS：

```bash
hdfs dfs -ls /user/hadoop
```

## 四 编译全部 Java 程序

确认当前目录：

```bash
pwd
```

输出应为：

```text
/home/hadoop/experiment1-hadoop-java
```

创建编译目录：

```bash
mkdir -p build/classes
```

如果以前编译过，先删除旧的 `.class` 文件：

```bash
find build/classes -type f -name '*.class' -delete
```

编译：

```bash
javac -encoding UTF-8 -classpath "$(hadoop classpath)" -d build/classes src/*.java
```

打包：

```bash
jar -cvf build/experiment1.jar -C build/classes .
```

检查 JAR 包：

```bash
jar -tf build/experiment1.jar
```

如果能看到 `HdfsOperations.class`、`MergeDistinct.class`、`IntegerSort.class` 和 `GrandchildRelationship.class`，说明编译和打包成功。

## 五 第二部分 HDFS Java API 操作

设置本次实验的 HDFS 根目录：

```bash
BASE=/user/hadoop/exp1
```

创建实验目录：

```bash
hdfs dfs -mkdir -p "$BASE/hdfs-api"
```

### 1 上传文件并处理同名文件

首次上传：

```bash
hadoop jar build/experiment1.jar HdfsOperations upload data/hdfs-api/first.txt "$BASE/hdfs-api/demo.txt" overwrite
```

文件已经存在时，在末尾追加本地文件内容：

```bash
hadoop jar build/experiment1.jar HdfsOperations upload data/hdfs-api/append.txt "$BASE/hdfs-api/demo.txt" append
```

文件已经存在时，用本地文件覆盖：

```bash
hadoop jar build/experiment1.jar HdfsOperations upload data/hdfs-api/first.txt "$BASE/hdfs-api/demo.txt" overwrite
```

### 2 下载文件并自动处理本地重名

创建下载目录：

```bash
mkdir -p downloads
```

第一次下载：

```bash
hadoop jar build/experiment1.jar HdfsOperations download "$BASE/hdfs-api/demo.txt" downloads/demo.txt
```

再次执行同一条命令：

```bash
hadoop jar build/experiment1.jar HdfsOperations download "$BASE/hdfs-api/demo.txt" downloads/demo.txt
```

第二次不会覆盖原文件，而是生成 `demo-1.txt`；继续运行会生成 `demo-2.txt` 等。

查看下载结果：

```bash
ls -l downloads
```

### 3 把 HDFS 文件内容输出到终端

```bash
hadoop jar build/experiment1.jar HdfsOperations cat "$BASE/hdfs-api/demo.txt"
```

### 4 显示指定路径的信息

```bash
hadoop jar build/experiment1.jar HdfsOperations stat "$BASE/hdfs-api/demo.txt"
```

程序会显示类型、权限、大小、修改时间、访问时间和路径。Hadoop 常用版本的 `FileStatus` 不提供独立的“创建时间”，所以这里使用修改时间和访问时间。

### 5 递归显示目录内容

```bash
hadoop jar build/experiment1.jar HdfsOperations list-recursive "$BASE"
```

### 6 创建和删除文件

创建文件；父目录不存在时会自动创建：

```bash
hadoop jar build/experiment1.jar HdfsOperations create-file "$BASE/files/empty.txt"
```

查看：

```bash
hadoop jar build/experiment1.jar HdfsOperations stat "$BASE/files/empty.txt"
```

删除文件：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-file "$BASE/files/empty.txt"
```

### 7 创建和删除目录

创建一个空目录；父目录不存在时会自动创建：

```bash
hadoop jar build/experiment1.jar HdfsOperations create-dir "$BASE/directories/empty-dir"
```

删除空目录：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-dir "$BASE/directories/empty-dir"
```

创建非空目录：

```bash
hadoop jar build/experiment1.jar HdfsOperations create-dir "$BASE/directories/not-empty"
```

```bash
hadoop jar build/experiment1.jar HdfsOperations create-file "$BASE/directories/not-empty/data.txt"
```

尝试删除非空目录：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-dir "$BASE/directories/not-empty"
```

程序应提示“目录不为空，不删除”。先删除其中的文件：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-file "$BASE/directories/not-empty/data.txt"
```

再删除空目录：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-dir "$BASE/directories/not-empty"
```

### 8 向文件开头或末尾追加文本

向末尾追加：

```bash
hadoop jar build/experiment1.jar HdfsOperations append "$BASE/hdfs-api/demo.txt" tail "TAIL added by Java API"
```

向开头追加：

```bash
hadoop jar build/experiment1.jar HdfsOperations append "$BASE/hdfs-api/demo.txt" head "HEAD added by Java API"
```

查看结果：

```bash
hadoop jar build/experiment1.jar HdfsOperations cat "$BASE/hdfs-api/demo.txt"
```

### 9 删除指定文件

先创建一个测试文件：

```bash
hadoop jar build/experiment1.jar HdfsOperations create-file "$BASE/hdfs-api/delete-me.txt"
```

再删除：

```bash
hadoop jar build/experiment1.jar HdfsOperations delete-file "$BASE/hdfs-api/delete-me.txt"
```

### 10 移动文件

移动并重命名：

```bash
hadoop jar build/experiment1.jar HdfsOperations move "$BASE/hdfs-api/demo.txt" "$BASE/moved/demo-moved.txt"
```

查看移动后的文件：

```bash
hadoop jar build/experiment1.jar HdfsOperations stat "$BASE/moved/demo-moved.txt"
```

如果要继续后面的 Shell 对照实验，可以把文件移回去：

```bash
hadoop jar build/experiment1.jar HdfsOperations move "$BASE/moved/demo-moved.txt" "$BASE/hdfs-api/demo.txt"
```

## 六 第二部分对应的 HDFS Shell 命令

文档要求使用 Shell 命令完成相同任务。以下命令使用单独的 `$BASE/shell` 目录，避免影响 Java API 的测试结果。

创建目录：

```bash
hdfs dfs -mkdir -p "$BASE/shell"
```

### 1 上传 覆盖和末尾追加

上传或覆盖：

```bash
hdfs dfs -put -f data/hdfs-api/first.txt "$BASE/shell/demo.txt"
```

末尾追加：

```bash
hdfs dfs -appendToFile data/hdfs-api/append.txt "$BASE/shell/demo.txt"
```

### 2 下载并避免覆盖本地同名文件

设置下载文件名：

```bash
target=downloads/demo-shell.txt
```

如果同名文件已存在，则添加时间戳：

```bash
if [ -e "$target" ]; then target="downloads/demo-shell-$(date +%Y%m%d%H%M%S).txt"; fi
```

下载：

```bash
hdfs dfs -get "$BASE/shell/demo.txt" "$target"
```

### 3 输出文件内容

```bash
hdfs dfs -cat "$BASE/shell/demo.txt"
```

### 4 显示文件信息

```bash
hdfs dfs -ls "$BASE/shell/demo.txt"
```

### 5 递归显示目录信息

```bash
hdfs dfs -ls -R "$BASE/shell"
```

### 6 创建和删除文件

```bash
hdfs dfs -mkdir -p "$BASE/shell/files"
```

```bash
hdfs dfs -touchz "$BASE/shell/files/empty.txt"
```

```bash
hdfs dfs -rm "$BASE/shell/files/empty.txt"
```

### 7 创建目录和只删除空目录

```bash
hdfs dfs -mkdir -p "$BASE/shell/directories/empty-dir"
```

```bash
hdfs dfs -rmdir "$BASE/shell/directories/empty-dir"
```

`hdfs dfs -rmdir` 遇到非空目录会拒绝删除，符合实验要求。

### 8 向末尾或开头追加

末尾追加：

```bash
printf 'TAIL added by shell\n' > /tmp/exp1-tail.txt
```

```bash
hdfs dfs -appendToFile /tmp/exp1-tail.txt "$BASE/shell/demo.txt"
```

HDFS Shell 没有直接向文件开头追加的命令，需要下载、拼接后覆盖：

```bash
rm -f /tmp/exp1-old.txt /tmp/exp1-head.txt /tmp/exp1-new.txt
```

```bash
printf 'HEAD added by shell\n' > /tmp/exp1-head.txt
```

```bash
hdfs dfs -get "$BASE/shell/demo.txt" /tmp/exp1-old.txt
```

```bash
cat /tmp/exp1-head.txt /tmp/exp1-old.txt > /tmp/exp1-new.txt
```

```bash
hdfs dfs -put -f /tmp/exp1-new.txt "$BASE/shell/demo.txt"
```

### 9 删除文件

```bash
hdfs dfs -touchz "$BASE/shell/delete-me.txt"
```

```bash
hdfs dfs -rm "$BASE/shell/delete-me.txt"
```

### 10 移动文件

```bash
hdfs dfs -mkdir -p "$BASE/shell/moved"
```

```bash
hdfs dfs -mv "$BASE/shell/demo.txt" "$BASE/shell/moved/demo-moved.txt"
```

```bash
hdfs dfs -ls "$BASE/shell/moved"
```

## 七 第三部分 MapReduce 程序

### 任务一 文件合并与去重

清理本任务以前的输入和输出。以下命令只删除本实验目录：

```bash
hdfs dfs -rm -r -f "$BASE/mapreduce/merge/input" "$BASE/mapreduce/merge/output"
```

创建输入目录：

```bash
hdfs dfs -mkdir -p "$BASE/mapreduce/merge/input"
```

上传两个输入文件：

```bash
hdfs dfs -put data/merge/inputA.txt data/merge/inputB.txt "$BASE/mapreduce/merge/input/"
```

运行程序：

```bash
hadoop jar build/experiment1.jar MergeDistinct "$BASE/mapreduce/merge/input" "$BASE/mapreduce/merge/output"
```

查看结果：

```bash
hdfs dfs -cat "$BASE/mapreduce/merge/output/part-r-00000"
```

### 任务二 整数全局排序

清理本任务以前的输入和输出：

```bash
hdfs dfs -rm -r -f "$BASE/mapreduce/sort/input" "$BASE/mapreduce/sort/output"
```

创建输入目录：

```bash
hdfs dfs -mkdir -p "$BASE/mapreduce/sort/input"
```

上传三个输入文件：

```bash
hdfs dfs -put data/sort/input1.txt data/sort/input2.txt data/sort/input3.txt "$BASE/mapreduce/sort/input/"
```

运行程序：

```bash
hadoop jar build/experiment1.jar IntegerSort "$BASE/mapreduce/sort/input" "$BASE/mapreduce/sort/output"
```

查看结果：

```bash
hdfs dfs -cat "$BASE/mapreduce/sort/output/part-r-00000"
```

预期结果为：

```text
1  1
2  4
3  5
4  12
5  16
6  25
7  33
8  37
9  39
10 40
11 45
```

实际文件中的两列通常由制表符分隔，视觉上可能与上面的空格数量不同。

### 任务三 挖掘祖孙关系

清理本任务以前的输入和输出：

```bash
hdfs dfs -rm -r -f "$BASE/mapreduce/family/input" "$BASE/mapreduce/family/output"
```

创建输入目录：

```bash
hdfs dfs -mkdir -p "$BASE/mapreduce/family/input"
```

上传输入文件：

```bash
hdfs dfs -put data/family/child-parent.txt "$BASE/mapreduce/family/input/"
```

运行程序：

```bash
hadoop jar build/experiment1.jar GrandchildRelationship "$BASE/mapreduce/family/input" "$BASE/mapreduce/family/output"
```

查看结果：

```bash
hdfs dfs -cat "$BASE/mapreduce/family/output/part-r-00000"
```

输出第一行为 `grandchild grandparent`，后面是计算得到的祖孙关系。

## 八 查看全部实验文件

```bash
hdfs dfs -ls -R "$BASE"
```

需要把某个结果下载到虚拟机本地时，例如下载整数排序结果：

```bash
hdfs dfs -get "$BASE/mapreduce/sort/output/part-r-00000" downloads/integer-sort-result.txt
```

## 九 上传代码到 GitHub

以下命令只上传源码、示例数据和说明文档，`.gitignore` 会排除编译产物和下载结果。

进入项目目录：

```bash
cd /home/hadoop/experiment1-hadoop-java
```

初始化仓库：

```bash
git init
```

如果 Git 提示没有设置用户信息，执行：

```bash
git config user.name "你的GitHub用户名"
```

```bash
git config user.email "你的邮箱"
```

添加文件：

```bash
git add .
```

检查将要提交的文件：

```bash
git status
```

提交：

```bash
git commit -m "Complete experiment 1 HDFS and MapReduce programs"
```

把默认分支改成 `main`：

```bash
git branch -M main
```

在 GitHub 网站上新建一个公开且为空的仓库，然后执行下面命令。请替换用户名和仓库名：

```bash
git remote add origin https://github.com/你的GitHub用户名/你的仓库名.git
```

推送：

```bash
git push -u origin main
```

最后把 GitHub 仓库页面链接放入实验报告。

## 十 常见错误

### 输出目录已经存在

MapReduce 不允许输出到已经存在的目录。如果看到 `FileAlreadyExistsException`，只删除对应任务的输出目录，然后重新运行。例如：

```bash
hdfs dfs -rm -r -f "$BASE/mapreduce/sort/output"
```

### 找不到 Hadoop 类

重新使用下面命令编译，不能只执行普通的 `javac src/*.java`：

```bash
javac -encoding UTF-8 -classpath "$(hadoop classpath)" -d build/classes src/*.java
```

### HDFS 连接失败

先执行：

```bash
jps
```

如果没有 `NameNode` 和 `DataNode`，执行：

```bash
start-dfs.sh
```

### HDFS 权限不足

确认当前用户：

```bash
whoami
```

本说明默认使用 `hadoop` 用户，并把实验数据放在 `/user/hadoop/exp1` 下。
