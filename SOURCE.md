
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
|2| topic size | 
|topic | bytes |
|4| partition | 
ByteBufferMessageSet --> Bytes
|4| Bytes size | 
| | Bytes content|


# 总体
三条主线：
* Server
* Producer 
* Consumer

## Server

### server startup

* `logManager.load`

* `socketServer.startup`
1. __RequestHandlers:__ handle request from client
2. __SocketServer:__ 

   `Processor`: Thread that processes all requests from a single connection.There are N of these running in parallel each of which has its own selectors.它先于Accptor线程启动，一直等待至`Acceptor`放入client socketChannel（有client socket接入）,
      
   `Acceptor`: Thread that accepts and configures new connections. There is only need for one of these.即ServerSocket，等待Socket的连接。分配ClientSocketChannel和对应的Processor,这样可以在processors内均匀分配待处理的clientSocket.
   
   __processor处理client socket过程：__
   >1. 在该processor的selector上注册该channel的read事件，之后processor等待读写请求并做出响应的操作。
   >2. 按读出的RequestType找RequestHandler处理，有response再注册Write Event
.__(但为什么频繁的close socket & remove selector key???)__

   >>A: __ProducerHandler__ --> ByteBufferMessageSet
   
   每个broker内startup时的单实例requestHandlerFactory有多个processors共享，应此多producer同时写同topic的数据时，_(即使有brokers间的分摊写入的patitions,但同一BrokerPartion仍有并发写的可能)_虽然不同的process处理client socket，但各自processor会用同一个requestHandler取处理log.append.__所以完全依赖log.append内部的锁来控制并发写问题，这个应该比较影响性能吧!!!__
   
   >>B: __FetchHandler__ --> FileMessageSet
   
   根据offset找到对应的LogSegment -> fileMessageSet.read(offset,size);`log.read`反馈的是FileMessageSet,在包装成`MessageSetSend`__(A zero-copy message response that writes the bytes needed directly from the file wholly in kernel space)__,其在response consumer时的writeTo(socketChannel)就是用的`FileMessageSet内的 fileChannel.transferTo(socketChannel)`
   
   _command window模式下的consume topic的刷数据是怎样实现的？？ MultiFetchRequest ？_
   
* `httpServer.start`

* `logManager.startup` 

* `serverInfo.started`

### `ServerRegister`
处理Broker和ZK之间的交互，主要写入以下信息：
~~~
/topics/[topic]/[node_id-partition_num]
/brokers/[0...N] --> host:port
~~~

### LogManager主要方法过程
_初始化消息数据管理类LogManager，并将所有的消息数据按照一定格式读入内存（非数据内容本身）_

* __构造函数:__ 

* __load:__ 	
1. 加载Data相关信息

2. 启动delete task of old logs： LogSegmentFilter --> 按时间 / 按总大小

3. ServerRegister.startup: 

1) zkClient->subscribeStateChanges-->KeeperState change;new session
2) TopicRegisterTask.start(守护线程): 阻塞式监听topic task（_CREATE/DELETE/ENLARGE/SHUTDOWN_），并处理每个Task

* __startup__
1. Register this broker in ZK
2. flushAllLogs 
定时(强制/按一定interval)flush all messages to disk： broker内的各topic数据文件 --> log.flush --> segments.getLastView().getMessageSet().flush();
--> FileMessageSet.flush(): 将此segment.fileChannel内的数据force to disk --> channel.force(true); __Forces any updates to this channel's file to be written to the storage device that contains it.__

__segment.fileChannel内的数据写入方__

`FileMessageSet.append + FileMessageSet.flush`
_将producer传递过来的messages添加到当前messageset对象(channel)中，虽然调用了writeTo方法，但是由于操作系统缓冲的存在，数据可能还没有真正写入磁盘，而flush方法的作用便是强制写磁盘。这两个方法便完成了消息数据持久化到磁盘的操作。_

~~~
FileMessageSet.append(MessageSet)
>>Log.append(ByteBufferMessageSet)
>>>>HttpRequestHandler.handleProducerRequest(ProducerRequest)
>>>>ProducerHandler.handleProducerRequest(ProducerRequest)
~~~

#### Log.append
* 将messages写入LogSegment的file channel

* 每隔flushInterval条数据，触发`segments.getLastView().getMessageSet().flush();`

* 每批次的messages写入都会rollingStategy.check(lastSegment)来判断是否需轮转文件(可以根据文件大小highWaterMark/时间)

__Log内lock对象的使用__

* Log.append会竞争lock:`写入file channel->maybeFlush->maybeRoll`用到了锁的继承._lock的存在，保证了其内部对文件操作的有序性;同时在多Producer写同一topic_partition时不会有并发写问题_

* log.validateSegments： 实例化Log加载data文件时的验证Segments,不会有(也不允许)data文件有变化

* log.close:  segments.close --> fileChannels.close

* log.markDeletedWhile: 定时delete log task会调用，此时不允许data文件(segments)有变化



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
* socket readstream listener --> zero copy到文件内： 进程内数据PageCache直接到Disk？

__多consumer读不同数据时的性能问题 利用linux的预读算法 / RandomAccess ？__



## 关于MetaQ:
* 优先级： MetaQ支持在内存中排序，但是放弃了磁盘系统的排序。
* 重复消费(Exactly And Only Once)： 保证送达，不保证不重复，而在业务中判断重复，消息消费具有幂等性。

## NIO

1. [Java NIO与IO](http://ifeve.com/java-nio-vs-io/)

* Java NIO和IO之间第一个最大的区别是，IO是面向流的，NIO是面向缓冲区的。 Java IO面向流意味着每次从流中读一个或多个字节，直至读取所有字节，它们没有被缓存在任何地方。此外，它不能前后移动流中的数据。如果需要前后移动从流中读取的数据，需要先将它缓存到一个缓冲区。 Java NIO的缓冲导向方法略有不同。数据读取到一个它稍后处理的缓冲区，需要时可在缓冲区中前后移动。这就增加了处理过程中的灵活性。

* Java IO的各种流是阻塞的。这意味着，当一个线程调用read() 或 write()时，该线程被阻塞，直到有一些数据被读取，或数据完全写入。该线程在此期间不能再干任何事情了。 Java NIO的非阻塞模式，使一个线程从某通道发送请求读取数据，但是它仅能得到目前可用的数据，如果目前没有数据可用时，就什么都不会获取。而不是保持线程阻塞，所以直至数据变的可以读取之前，该线程可以继续做其他的事情。 非阻塞写也是如此。一个线程请求写入一些数据到某通道，但不需要等待它完全写入，这个线程同时可以去做别的事情。 线程通常将非阻塞IO的空闲时间用于在其它通道上执行IO操作，所以一个单独的线程现在可以管理多个输入和输出通道（channel）。

* Java NIO的选择器允许一个单独的线程来监视多个输入通道，你可以注册多个通道使用一个选择器，然后使用一个单独的线程来“选择”通道：这些通道里已经有可以处理的输入，或者选择已准备写入的通道。这种选择机制，使得一个单独的线程很容易来管理多个通道。

2. [Selector](http://ifeve.com/selectors/)

* 与Selector一起使用时，Channel必须处于非阻塞模式下。 这意味着不能将FileChannel与Selector一起使用，因为FileChannel不能切换到非阻塞模式。而套接字通道都可以.

* 通道触发了一个事件意思是该事件已经就绪。所以，某个channel成功连接到另一个服务器称为“连接就绪”。一个server socket channel准备好接收新进入的连接称为“接收就绪”。一个有数据可读的通道可以说是“读就绪”。等待写数据的通道可以说是“写就绪”。

* __wakeUp()__
某个线程调用select()方法后阻塞了，即使没有通道已经就绪，也有办法让其从select()方法返回。只要让其它线程在第一个线程调用select()方法的那个对象上调用Selector.wakeup()方法即可。阻塞在select()方法上的线程会立马返回。* 