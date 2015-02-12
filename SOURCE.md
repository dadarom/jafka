
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







