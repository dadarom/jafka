
## 通信协议及相关类
### Message

Message类是具体的消息数据，在传递的过程中，其字节序列的组成及含义如下:

|字节大小(Byte)|含义|
|:-------:|:-------:|
|4|length,该条消息总长度|
|1|version(magic byte),消息版本，适应后面更改通信协议，目前只有一种协议|
|1|attribute,目前主要用于指明压缩算法：0--NoCompression 1--GzipCompression|
|4|crc32，消息完整性校验|
|x|实际数据，x=length-1-1-4|
{: class="table table-striped table-bordered"}

### MessageSet

1. 多个Message的集合，它有两个子类：ByteBufferMessageSet和FileMessageSet，前者供producer和consumer使用，后者供broker使用.

2. MessageSet封装产生的ByteBuffer就是多个Message首尾相连构造而成。

3. writeTo的实现：

#### ByteBufferMessageSet

主要被Producer和Consumer使用，前者将要传送的Message封装成ByteBufferMessageSet,然后传送到broker；后者将fetch到的Message封装成ByteBufferMessageSet，然后遍历消费。

~~~java
@Override
public long writeTo(GatheringByteChannel channel, long offset, long maxSize) throws IOException {
    buffer.mark();
    int written = channel.write(buffer);
    buffer.reset();
    return written;
}
~~~

#### FileMessageSet

主要由broker使用

~~~java
@Override
public long writeTo(GatheringByteChannel destChannel, long writeOffset, long maxSize) throws IOException {
    // file zero copy
    return channel.transferTo(offset + writeOffset, Math.min(maxSize, getSizeInBytes()), destChannel);
}
~~~

#### Message相关
Client用： `ProducerData` `ProducerPoolData`
内部用： `Message` `Encoder: 将client的数据类型转换为内部类型Message`

数据转换过程：
1. Sync发送数据前已经作完数据的转换
2. Async将数据一条一条塞入队列（由对列的Consumer来作数据的处理？）

`ProducerData --> ProducerPoolData --> Message --> ByteBufferMessageSet --> ProducerRequest`

在`ByteBufferMessageSet`内部将 Messages --> ByteBuffer --> ByteBufferMessageSet

#### IteratorTemplate
SyncProducer.send 在check max size of each message时候，结合IteratorTemplate的用法需回顾（怎样形成 MessageAndOffset）

#### Send Receive

##### Producer

Request --> ProducerRequest
Socket --> (ReadChannel/WriteChannel)BlockingChannel.send(Request) --> BoundedByteBufferSend.writeCompletely(WriteChannel) --> writeTo(WriteChannel)

__ProducerRequest --> BoundedByteBufferSend过程：__

BoundedByteBufferSend: 将Request内数据包装：加入2字节RequestKey.
按以下格式，将Request内Buffer数据写入BoundedByteBufferSend's Buffer

|字节大小(Byte)|含义|
|:-------:|:-------:|
|2|RequestKey, RequestKeys.PRODUCE| 
|request size|request.bytes| 

`request.bytes`
|字节大小(Byte)|含义|
|:-------:|:-------:|
|2| topic size | 
|topic | bytes |
|4| partition | 
ByteBufferMessageSet --> Bytes
|4| Bytes size | 
| | Bytes content|


# 总体
## ConfigSend
####Producer 
_对于实时性要求较高的信息，采取同步发送的方法好，而对于像日志这种数据，可以采取异步发送的形式，减小对当前程序的压力。_

1. SyncProducer(借助ByteBuffer)通过BoundedByteBufferSend写入Socket WriteChannel: Broker端应该还有定时flush(where?)

2. AsyncProducer
将每条数据包装成QueueItem并塞入队列，并启动ProducerSendThread(每个Producer仅一个SendThread)，定时(或者batchSize满时)将pool到的数据发往Broker.

* eventHandler: 处理从队列内取出的数据.__DefaultEventHandler__
将QueueItem包装成ProducerRequest，利用SyncProducer send MultiProducerRequest.
* callbackHandler: 处理时的回调
* syncProducer

由于queue是有大小限制的（防止数据过多，占用大量内存），所以添加的时候有一定的策略，该策略可以通过queue.enqueueTimeout.ms来配置，即enqueueTimeoutMs。策略如下：

1. 等于0---调用offer方法，无论是否成功，直接返回，意味着如果queue满了，消息会被舍弃，并返回false。
2. 小于0---调用put方法，阻塞直到可以成功加入queue
3. 大于0---调用offer(e,time,unit)方法，等待一段时间，超时的话返回false

__`写入socket后 怎么处理写入文件的并发写问题呢？？？`__
