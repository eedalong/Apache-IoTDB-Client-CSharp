# C# 原生接口

## 依赖

- .NET SDK >= 5.0 或 .NET Framework 4.x
- ApacheThrift >= 0.14.1
- NLog >= 4.7.9

## 安装

您可以使用 NuGet Package Manager, .NET CLI等工具来安装，以 .NET CLI为例

如果您使用的是.NET 5.0 或者更高版本的SDK，输入如下命令即可安装最新的NuGet包

```
dotnet add package Apache.IoTDB
```

为了适配 .NET Framework 4.x，我们单独构建了一个NuGet包，如果您使用的是.NET Framework 4.x，输入如下命令即可安装最新的包

```bash
dotnet add package Apache.IoTDB.framework
```

如果您想安装更早版本的客户端，只需要指定版本即可

```bash
# 安装0.12.1.2版本的客户端
dotnet add package Apache.IoTDB --version 0.12.1.2
```

## 基本接口说明

Session接口在语义上和其他语言客户端相同

```c#
// 参数定义
string host = "localhost";
int port = 6667;
int pool_size = 2;

// 初始化session
var session_pool = new SessionPool(host, port, pool_size);

// 开启session
await session_pool.Open(false);

// 创建时间序列
await session_pool.CreateTimeSeries("root.test_group.test_device.ts1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
await session_pool.CreateTimeSeries("root.test_group.test_device.ts2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
await session_pool.CreateTimeSeries("root.test_group.test_device.ts3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);

// 插入record
var measures = new List<string>{"ts1", "ts2", "ts3"};
var values = new List<object> { "test_text", true, (int)123 };
var timestamp = 1;
var rowRecord = new RowRecord(timestamp, values, measures);
await session_pool.InsertRecordAsync("root.test_group.test_device", rowRecord);

// 插入Tablet
var timestamp_lst = new List<long>{ timestamp + 1 };
var value_lst = new List<object> {new() {"iotdb", true, (int) 12}};
var tablet = new Tablet("root.test_group.test_device", measures, value_lst, timestamp_ls);
await session_pool.InsertTabletAsync(tablet);

// 关闭Session
await session_pool.Close();
```

详细接口信息可以参考[接口文档](https://github.com/eedalong/Apache-IoTDB-Client-CSharp/blob/main/docs/API.md)

用法可以参考[用户示例](https://github.com/eedalong/Apache-IoTDB-Client-CSharp/tree/main/samples/Apache.IoTDB.Samples)

## 连接池

为了实现并发客户端请求，我们提供了针对原生接口的连接池(`SessionPool`)，由于`SessionPool`本身为`Session`的超集，当`SessionPool`的`pool_size`参数设置为1时，退化为原来的`Session`

我们使用`ConcurrentQueue`数据结构封装了一个客户端队列，以维护与服务端的多个连接，当调用`Open()`接口时，会在该队列中创建指定个数的客户端，同时通过`System.Threading.Monitor`类实现对队列的同步访问。

当请求发生时，会尝试从连接池中寻找一个空闲的客户端连接，如果没有空闲连接，那么程序将需要等待直到有空闲连接

当一个连接被用完后，他会自动返回池中等待下次被使用

在使用连接池后，客户端的并发性能提升明显，[这篇文档](https://github.com/eedalong/Apache-IoTDB-Client-CSharp/blob/main/docs/session_pool_zh.md#%E5%BB%BA%E7%AB%8Bclient%E8%BF%9E%E6%8E%A5)展示了使用线程池比起单线程所带来的性能提升

## ByteBuffer

在传入RPC接口参数时，需要对Record和Tablet两种数据结构进行序列化，我们主要通过封装的ByteBuffer类实现

在封装字节序列的基础上，我们进行了内存预申请与内存倍增的优化，减少了序列化过程中内存的申请和释放，在一个拥有20000行的Tablet上进行序列化测试时，速度比起原生的数组动态增长具有**35倍的性能加速**

详见以下两篇文档

[ByteBuffer详细介绍](https://github.com/eedalong/Apache-IoTDB-Client-CSharp/blob/main/docs/bytebuffer_zh.md)

[ByteBuffer性能测试文档](https://bnw3yl170k.feishu.cn/docs/doccnxiHV7avYiFBkuEljCLIYO4#mMS5HE)

## 异常重连

当服务端发生异常或者宕机重启时，客户端中原来通过`Open()`产生的的session会失效，抛出`TException`异常

为了避免这一情况的发生，我们对大部分的接口进行了增强，一旦出现连接问题，就会尝试重新调用`Open()`接口并创建新的Session，并尝试重新发送对应的请求

