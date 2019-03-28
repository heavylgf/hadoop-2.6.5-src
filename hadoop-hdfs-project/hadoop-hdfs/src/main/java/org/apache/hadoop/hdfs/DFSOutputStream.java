/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;


/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.  
 *  
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer
    implements Syncable, CanSetDropBehind {
  private final long dfsclientSlowLogThresholdMs;
  /**
   * Number of times to retry creating a file when there are transient 
   * errors (typically related to encryption zones and KeyProvider operations).
   */
  @VisibleForTesting
  static final int CREATE_RETRY_COUNT = 10;
  @VisibleForTesting
  static CryptoProtocolVersion[] SUPPORTED_CRYPTO_VERSIONS =
      CryptoProtocolVersion.supported();

  private final DFSClient dfsClient;
  private final ByteArrayManager byteArrayManager;
  private Socket s;
  // closed is accessed by different threads under different locks.
  private volatile boolean closed = false;

  private String src;
  private final long fileId;
  private final long blockSize;
  /** Only for DataTransferProtocol.writeBlock(..) */
  private final DataChecksum checksum4WriteBlock;
  private final int bytesPerChecksum; 

  // both dataQueue and ackQueue are protected by dataQueue lock
  // 如果用LinkedList的话，相当于就是队列
  private final LinkedList<Packet> dataQueue = new LinkedList<Packet>();
  private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
  private Packet currentPacket = null;
  private DataStreamer streamer;
  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private int packetSize = 0; // write packet size, not including the header.
  private int chunksPerPacket = 0;
  private final AtomicReference<IOException> lastException = new AtomicReference<IOException>();
  private long artificialSlowdown = 0;
  private long lastFlushOffset = 0; // offset when flush was invoked
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private volatile boolean appendChunk = false;   // appending to existing partial block
  private long initialFileSize = 0; // at time of file open
  private final Progressable progress;
  private final short blockReplication; // replication factor of file
  private boolean shouldSyncBlock = false; // force blocks to disk upon close
  private final AtomicReference<CachingStrategy> cachingStrategy;
  private boolean failPacket = false;
  private FileEncryptionInfo fileEncryptionInfo;
  private static final BlockStoragePolicySuite blockStoragePolicySuite =
      BlockStoragePolicySuite.createDefaultSuite();

  /** Use {@link ByteArrayManager} to create buffer for non-heartbeat packets.*/
  private Packet createPacket(int packetSize, int chunksPerPkt, long offsetInBlock,
      long seqno) throws InterruptedIOException {
    final byte[] buf;
    final int bufferSize = PacketHeader.PKT_MAX_HEADER_LEN + packetSize;

    try {
      buf = byteArrayManager.newByteArray(bufferSize);
    } catch (InterruptedException ie) {
      final InterruptedIOException iioe = new InterruptedIOException(
          "seqno=" + seqno);
      iioe.initCause(ie);
      throw iioe;
    }

    return new Packet(buf, chunksPerPkt, offsetInBlock, seqno, getChecksumSize());
  }

  /**
   * For heartbeat packets, create buffer directly by new byte[]
   * since heartbeats should not be blocked.
   */
  private Packet createHeartbeatPacket() throws InterruptedIOException {
    final byte[] buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
    return new Packet(buf, 0, 0, Packet.HEART_BEAT_SEQNO, getChecksumSize());
  }

  private static class Packet {
    private static final long HEART_BEAT_SEQNO = -1L;
    long seqno; // sequencenumber of buffer in block
    final long offsetInBlock; // offset in block
    boolean syncBlock; // this packet forces the current block to disk
    int numChunks; // number of chunks currently in packet
    final int maxChunks; // max chunks in packet
    
    // packet里的数据，是包含了一个checksum对应一个chunk，依次写入
    // 全部是放在一个buf缓冲数组里的
    private byte[] buf;
    private boolean lastPacketInBlock; // is this the last packet in block?

    /**
     * buf is pointed into like follows:
     *  (C is checksum data, D is payload data)
     *
     * [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
     *           ^        ^               ^               ^
     *           |        checksumPos     dataStart       dataPos
     *           checksumStart
     * 
     * Right before sending, we move the checksum data to immediately precede
     * the actual data, and then insert the header into the buffer immediately
     * preceding the checksum data, so we make sure to keep enough space in
     * front of the checksum data to support the largest conceivable header. 
     */
    int checksumStart;
    int checksumPos;
    final int dataStart;
    int dataPos;

    /**
     * Create a new packet.
     * 
     * @param pktSize maximum size of the packet, 
     *                including checksum data and actual data.
     * @param chunksPerPkt maximum number of chunks per packet.
     * @param offsetInBlock offset in bytes into the HDFS block.
     */
    private Packet(byte[] buf, int chunksPerPkt, long offsetInBlock, long seqno,
        int checksumSize) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = seqno;

      this.buf = buf;

      checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
      checksumPos = checksumStart;
      dataStart = checksumStart + (chunksPerPkt * checksumSize);
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    void writeData(byte[] inarray, int off, int len) {
      if (dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void writeChecksum(byte[] inarray, int off, int len) {
      if (len == 0) {
        return;
      }
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }
    
    /**
     * Write the full packet, including the header, to the given output stream.
     */
    void writeTo(DataOutputStream stm) throws IOException {
      final int dataLen = dataPos - dataStart;
      final int checksumLen = checksumPos - checksumStart;
      final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;

      PacketHeader header = new PacketHeader(
        pktLen, offsetInBlock, seqno, lastPacketInBlock, dataLen, syncBlock);
      
      if (checksumPos != dataStart) {
        // Move the checksum to cover the gap. This can happen for the last
        // packet or during an hflush/hsync call.
        System.arraycopy(buf, checksumStart, buf, 
                         dataStart - checksumLen , checksumLen); 
        checksumPos = dataStart;
        checksumStart = checksumPos - checksumLen;
      }
      
      final int headerStart = checksumStart - header.getSerializedSize();
      assert checksumStart + 1 >= header.getSerializedSize();
      assert checksumPos == dataStart;
      assert headerStart >= 0;
      assert headerStart + header.getSerializedSize() == checksumStart;
      
      // Copy the header data into the buffer immediately preceding the checksum
      // data.
      System.arraycopy(header.getBytes(), 0, buf, headerStart,
          header.getSerializedSize());
      
      // corrupt the data for testing.
      if (DFSClientFaultInjector.get().corruptPacket()) {
        buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
      }

      // Write the now contiguous full packet to the output stream.
      stm.write(buf, headerStart, header.getSerializedSize() + checksumLen + dataLen);

      // undo corruption.
      if (DFSClientFaultInjector.get().uncorruptPacket()) {
        buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
      }
    }

    private void releaseBuffer(ByteArrayManager bam) {
      bam.release(buf);
      buf = null;
    }
    
    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }
    
    /**
     * Check if this packet is a heart beat packet
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }
    
    @Override
    public String toString() {
      return "packet seqno:" + this.seqno +
      " offsetInBlock:" + this.offsetInBlock + 
      " lastPacketInBlock:" + this.lastPacketInBlock +
      " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
    }
  }

  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  /**
   * 
   * DataStreamer是一个核心的线程，是专门负责将数据上传到datanode的核心线程
   * 他是负责通过一个数据管道（pipeline），将数据包（packet）发送给datanodes
   * 他会从namenode获取一个blockid以及这些block副本在哪些datanode上，这样他才可以知道往哪个datanode写数据
   * 他发送的每个packet都有一个序号
   * 如果一个block里面的所有的packet数据包都成功发送给了datanode，而且所有的datanode都收到了这个block
   * DataStreamer就会关闭当前的这个block，说明这个block已经上传成功了
   * 
   * @author zhonghuashishan
   *
   */
  class DataStreamer extends Daemon {
    
	private volatile boolean streamerClosed = false;
    private volatile ExtendedBlock block; // its length is number of bytes acked
    private Token<BlockTokenIdentifier> accessToken;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private ResponseProcessor response = null;
    private volatile DatanodeInfo[] nodes = null; // list of targets for current block
    private volatile StorageType[] storageTypes = null;
    private volatile String[] storageIDs = null;
   
    // 根据这个名字，你大概可以理解一下，可能是什么呢？
    // 某个datanode比如说故障了，无法接收你上传的那个block，此时就会将那个datanode
    // 放入一个excludedNodes里面去
    private final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes =
        CacheBuilder.newBuilder()
        .expireAfterWrite(
            dfsClient.getConf().excludedNodesCacheExpiry,
            TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              RemovalNotification<DatanodeInfo, DatanodeInfo> notification) {
            DFSClient.LOG.info("Removing node " +
                notification.getKey() + " from the excluded nodes list");
          }
        })
        .build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
    private String[] favoredNodes;
    volatile boolean hasError = false;
    volatile int errorIndex = -1;
    volatile int restartingNodeIndex = -1; // Restarting node index
    private long restartDeadline = 0; // Deadline of DN restart
    private BlockConstructionStage stage;  // block construction stage
    private long bytesSent = 0; // number of bytes that've been sent
    private final boolean isLazyPersistFile;

    /** Nodes have been used in the pipeline before and have failed. */
    private final List<DatanodeInfo> failed = new ArrayList<DatanodeInfo>();
    /** The times have retried to recover pipeline, for the same packet. */
    private volatile int pipelineRecoveryCount = 0;
    /** Has the current block been hflushed? */
    private boolean isHflushed = false;
    /** Append on an existing block? */
    private final boolean isAppend;

    private final Span traceSpan;

    /**
     * construction with tracing info
     */
    private DataStreamer(HdfsFileStatus stat, Span span) {
      isAppend = false;
      isLazyPersistFile = isLazyPersist(stat);
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
      traceSpan = span;
    }
    
    /**
     * Construct a data streamer for append
     * @param lastBlock last block of the file to be appended
     * @param stat status of the file to be appended
     * @param bytesPerChecksum number of bytes per checksum
     * @throws IOException if error occurs
     */
    private DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
        int bytesPerChecksum, Span span) throws IOException {
      isAppend = true;
      stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
      traceSpan = span;
      block = lastBlock.getBlock();
      bytesSent = block.getNumBytes();
      accessToken = lastBlock.getBlockToken();
      isLazyPersistFile = isLazyPersist(stat);
      long usedInLastBlock = stat.getLen() % blockSize;
      int freeInLastBlock = (int)(blockSize - usedInLastBlock);

      // calculate the amount of free space in the pre-existing 
      // last crc chunk
      int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
      int freeInCksum = bytesPerChecksum - usedInCksum;

      // if there is space in the last block, then we have to 
      // append to that block
      if (freeInLastBlock == blockSize) {
        throw new IOException("The last block for file " + 
            src + " is full.");
      }

      if (usedInCksum > 0 && freeInCksum > 0) {
        // if there is space in the last partial chunk, then 
        // setup in such a way that the next packet will have only 
        // one chunk that fills up the partial chunk.
        //
        computePacketChunkSize(0, freeInCksum);
        setChecksumBufSize(freeInCksum);
        appendChunk = true;
      } else {
        // if the remaining space in the block is smaller than 
        // that expected size of of a packet, then create 
        // smaller size packet.
        //
        computePacketChunkSize(Math.min(dfsClient.getConf().writePacketSize, freeInLastBlock), 
            bytesPerChecksum);
      }

      // setup pipeline to append to the last block XXX retries??
      setPipeline(lastBlock);
      errorIndex = -1;   // no errors yet.
      if (nodes.length < 1) {
        throw new IOException("Unable to retrieve blocks locations " +
            " for last block " + block +
            "of file " + src);

      }
    }

    private void setPipeline(LocatedBlock lb) {
      setPipeline(lb.getLocations(), lb.getStorageTypes(), lb.getStorageIDs());
    }
    private void setPipeline(DatanodeInfo[] nodes, StorageType[] storageTypes,
        String[] storageIDs) {
      this.nodes = nodes;
      this.storageTypes = storageTypes;
      this.storageIDs = storageIDs;
    }

    private void setFavoredNodes(String[] favoredNodes) {
      this.favoredNodes = favoredNodes;
    }

    /**
     * Initialize for data streaming
     */
    private void initDataStreaming() {
      this.setName("DataStreamer for file " + src +
          " block " + block);
      response = new ResponseProcessor(nodes);
      response.start();
      stage = BlockConstructionStage.DATA_STREAMING;
    }
    
    private void endBlock() {
      if(DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Closing old block " + block);
      }
      this.setName("DataStreamer for file " + src);
      closeResponder();
      closeStream();
      setPipeline(null, null, null);
      // stage重新变为了PIPELINE_SETUP_CREATE，需要重新建立一个数据管道
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }
    
    /*
     * streamer thread is the only thread that opens streams to datanode, 
     * and closes them. Any error recovery is also done by this thread.
     * 
     * 这个DataStreamer的线程，是唯一的一个负责跟datanode交互上传数据的一个线程
     * 如果说这个线程被启动了之后，就会开始运行他的run方法
     * 
     */
    @Override
    public void run() {
      long lastPacket = Time.now();
      TraceScope traceScope = null;
      if (traceSpan != null) {
        traceScope = Trace.continueSpan(traceSpan);
      }
      while (!streamerClosed && dfsClient.clientRunning) {

        // if the Responder encountered an error, shutdown Responder
    	// 就是大概可以理解为，数据上传的过程中，如果出现了错误，此时会走这里
    	// 如果在发送packet给primary datanode的过程中失败了，报错了，记录几个标识位
    	// 进入下一轮循环到这里
        if (hasError && response != null) {
          try {
            response.close();
            // 在这里直接阻塞住，务必等待PacketResponser线程必须先结束，才能往下走
            // interrupt、sleep、close、volatile、join，在java并发编程的课程里都讲了
            response.join();
            response = null;
          } catch (InterruptedException  e) {
            DFSClient.LOG.warn("Caught exception ", e);
          }
        }

        Packet one;
        try {
          // process datanode IO errors if any
          // 这里大概也是说，如果数据上传的过程中，出现了错误，会从这里来进行错误的处理
          // 关于hdfs文件上传的错误处理的机制，也是很重要的
          // 之前我们就是有同学，出去面试大数据这块，人家就问底层的原理，就说hdfs上传文件出错是怎么解决的
          boolean doSleep = false;
          // hasError = true
          // errorIndex = 0，>= 0的条件
          if (hasError && (errorIndex >= 0 || restartingNodeIndex >= 0)) {
        	// 处理datanode的故障
            doSleep = processDatanodeError();
          }

          synchronized (dataQueue) {
            // wait for a packet to be sent.
            long now = Time.now();
            
            // 这个什么意思
            // 大概是发现如果说dataQueue这个队列里的数据是空的
            // 此时会进入一个等待，无限的while循环
            // 他在这里每次会等待个1秒钟，再来判断dataQueue中是否有数据
            
            // 再往下的代码，我们就可以先不要看了
            // 你只要知道一点，刚开始初始化一个DataStreamer启动这个线程之后
            // dataQueue这个队列一定是空的
            // 所以这个线程一定会在这里卡住，阻塞住，等待有人放数据到dataQueue队列里来
            // 如果有数据，那么就取出来数据，发送给datanode
            while ((!streamerClosed && !hasError && dfsClient.clientRunning 
                && dataQueue.size() == 0 && 
                (stage != BlockConstructionStage.DATA_STREAMING || 
                 stage == BlockConstructionStage.DATA_STREAMING && 
                 now - lastPacket < dfsClient.getConf().socketTimeout/2)) || doSleep ) {
              long timeout = dfsClient.getConf().socketTimeout/2 - (now-lastPacket);
              timeout = timeout <= 0 ? 1000 : timeout;
              timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                 timeout : 1000;
              try {
                dataQueue.wait(timeout);
              } catch (InterruptedException  e) {
                DFSClient.LOG.warn("Caught exception ", e);
              }
              doSleep = false;
              now = Time.now();
            }
            
            // 在这里跳出了这个while循环
            // 说明他就从dataQueue这个队列里，获取到了一个packet
            
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            
            // get packet to be sent.
            if (dataQueue.isEmpty()) {
              one = createHeartbeatPacket();
            } else {
            	// 入队的时候，addLast
            	// 出队的时候，getFirst，先进先出
            	// 看过我的JDK集合源码分析的课，LinkedList是一个双向链表，分析过他的源码
            	one = dataQueue.getFirst(); // regular data packet
            }
          }
          assert one != null;

          // get new block from namenode.
          // 刚开始的时候，一定是走第一个if else的代码分支的，此时这个block正处于under construction状态
          if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
            if(DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Allocating new block");
            }
            // nextBlockOutputStream()方法
            // 其实就是负责在跟namenode去申请一个新的block
            setPipeline(nextBlockOutputStream());
            initDataStreaming();
          } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
            if(DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Append to block " + block);
            }
            setupPipelineForAppendOrRecovery();
            initDataStreaming();
          }

          long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
          if (lastByteOffsetInBlock > blockSize) {
            throw new IOException("BlockSize " + blockSize +
                " is smaller than data size. " +
                " Offset of packet in block " + 
                lastByteOffsetInBlock +
                " Aborting file " + src);
          }

          if (one.lastPacketInBlock) {
            // wait for all data packets have been successfully acked
        	// 如果发现是block里的最后一个packet，那么就会在这里陷入一个无限的循环和等待，等待之前发送的
        	// block里面的packet全部被处理掉，都上传好
            synchronized (dataQueue) {
              // 本来DataStreamer卡在这个while循环这里
              // hasError = true，不是false，所以此时这个循环会退出
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                try {
                  // wait for acks to arrive from datanodes
                  dataQueue.wait(1000);
                } catch (InterruptedException  e) {
                  DFSClient.LOG.warn("Caught exception ", e);
                }
              }
            }
            // 如果发现hasError = true的话
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            // 退出了以后，就会修改这个stage
            // PIPELINE_CLOSE，关闭这个数据管道了
            stage = BlockConstructionStage.PIPELINE_CLOSE;
          }
          
          // send the packet
          synchronized (dataQueue) {
            // move packet from dataQueue to ackQueue
            if (!one.isHeartbeatPacket()) {
              dataQueue.removeFirst();
              ackQueue.addLast(one);
              dataQueue.notifyAll();
            }
          }

          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("DataStreamer block " + block +
                " sending packet " + one);
          }

          // write out data to remote datanode
          try {
        	// 直接将一个packet中的127个chunk所组成的数据，通过网络的输出流
        	// 写入到第一个datanode上去
        	// 将packet写入第一个datanode的代码是在这里，如果在这里报错了
        	// 说明这个数据传输到第一个datanode是失败了
            one.writeTo(blockStream);
            blockStream.flush();   
          } catch (IOException e) {
            // HDFS-3398 treat primary DN is down since client is unable to 
            // write to primary DN. If a failed or restarting node has already
            // been recorded by the responder, the following call will have no 
            // effect. Pipeline recovery can handle only one node error at a
            // time. If the primary node fails again during the recovery, it
            // will be taken out then.
        	  
        	// 如果无法将packet写入到第一个datanode，那么就将第一个datanode（primary datanode）认为是宕机
        	// 如果PacketResponder已经记录了某个失败的、或者是正在重启中的datanode，对他的调用是不受影响的
        	// 数据管道的恢复机制每次只能处理已给datanode的故障
        	// 如果在数据管道恢复的过程中，primary datanode再次失败，那么就会将这个datanode从list中摘除
            tryMarkPrimaryDatanodeFailed();
            throw e;
          }
          lastPacket = Time.now();
          
          // update bytesSent
          long tmpBytesSent = one.getLastByteOffsetBlock();
          if (bytesSent < tmpBytesSent) {
            bytesSent = tmpBytesSent;
          }

          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            // wait for the close packet has been acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError && 
                  ackQueue.size() != 0 && dfsClient.clientRunning) {
                dataQueue.wait(1000);// wait for acks to arrive from datanodes
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }

            endBlock();
          }
          if (progress != null) { progress.progress(); }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && dfsClient.clientRunning) {
            Thread.sleep(artificialSlowdown); 
          }
        } catch (Throwable e) {
          // Log warning if there was a real error.
          if (restartingNodeIndex == -1) {
            DFSClient.LOG.warn("DataStreamer Exception", e);
          }
          if (e instanceof IOException) {
            setLastException((IOException)e);
          } else {
            setLastException(new IOException("DataStreamer Exception: ",e));
          }
          // errorIndex标记的是数据管道里的哪个datanode故障了
          // 标记hasError是true
          hasError = true;
          if (errorIndex == -1 && restartingNodeIndex == -1) {
            // Not a datanode issue
            streamerClosed = true;
          }
        }
        
        // 处理完异常故障了以后，就会进入下一轮的while循环
        
      }
      if (traceScope != null) {
        traceScope.close();
      }
      closeInternal();
    }

    private void closeInternal() {
      closeResponder();       // close and join
      closeStream();
      streamerClosed = true;
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
    }

    /*
     * close both streamer and DFSOutputStream, should be called only 
     * by an external thread and only after all data to be sent has 
     * been flushed to datanode.
     * 
     * Interrupt this data streamer if force is true
     * 
     * @param force if this data stream is forced to be closed 
     */
    void close(boolean force) {
      streamerClosed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
      if (force) {
        this.interrupt();
      }
    }

    private void closeResponder() {
      if (response != null) {
        try {
          response.close();
          response.join();
        } catch (InterruptedException  e) {
          DFSClient.LOG.warn("Caught exception ", e);
        } finally {
          response = null;
        }
      }
    }

    private void closeStream() {
      if (blockStream != null) {
        try {
          blockStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockStream = null;
        }
      }
      if (blockReplyStream != null) {
        try {
          blockReplyStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockReplyStream = null;
        }
      }
      if (null != s) {
        try {
          s.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          s = null;
        }
      }
    }

    // The following synchronized methods are used whenever 
    // errorIndex or restartingNodeIndex is set. This is because
    // check & set needs to be atomic. Simply reading variables
    // does not require a synchronization. When responder is
    // not running (e.g. during pipeline recovery), there is no
    // need to use these methods.

    /** Set the error node index. Called by responder */
    synchronized void setErrorIndex(int idx) {
      errorIndex = idx;
    }

    /** Set the restarting node index. Called by responder */
    synchronized void setRestartingNodeIndex(int idx) {
      restartingNodeIndex = idx;
      // If the data streamer has already set the primary node
      // bad, clear it. It is likely that the write failed due to
      // the DN shutdown. Even if it was a real failure, the pipeline
      // recovery will take care of it.
      errorIndex = -1;      
    }

    /**
     * This method is used when no explicit error report was received,
     * but something failed. When the primary node is a suspect or
     * unsure about the cause, the primary node is marked as failed.
     */
    synchronized void tryMarkPrimaryDatanodeFailed() {
      // There should be no existing error and no ongoing restart.
      if ((errorIndex == -1) && (restartingNodeIndex == -1)) {
        // 主要就是做一个标记：errorIndex = 0，就意味着说第一个datanode是故障了
    	errorIndex = 0;
      }
    }

    /**
     * Examine whether it is worth waiting for a node to restart.
     * @param index the node index
     */
    boolean shouldWaitForRestart(int index) {
      // Only one node in the pipeline.
      if (nodes.length == 1) {
        return true;
      }

      // Is it a local node?
      InetAddress addr = null;
      try {
        addr = InetAddress.getByName(nodes[index].getIpAddr());
      } catch (java.net.UnknownHostException e) {
        // we are passing an ip address. this should not happen.
        assert false;
      }

      if (addr != null && NetUtils.isLocalAddress(addr)) {
        return true;
      }
      return false;
    }

    //
    // Processes responses from the datanodes.  A packet is removed
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Daemon {

      private volatile boolean responderClosed = false;
      private DatanodeInfo[] targets = null;
      private boolean isLastPacketInBlock = false;

      ResponseProcessor (DatanodeInfo[] targets) {
        this.targets = targets;
      }

      @Override
      public void run() {

        setName("ResponseProcessor for block " + block);
        PipelineAck ack = new PipelineAck();

        while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
          // process responses from datanodes.
          try {
            // read an ack from the pipeline
            long begin = Time.monotonicNow();
            ack.readFields(blockReplyStream);
            long duration = Time.monotonicNow() - begin;
            if (duration > dfsclientSlowLogThresholdMs
                && ack.getSeqno() != Packet.HEART_BEAT_SEQNO) {
              DFSClient.LOG
                  .warn("Slow ReadProcessor read fields took " + duration
                      + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms); ack: "
                      + ack + ", targets: " + Arrays.asList(targets));
            } else if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("DFSClient " + ack);
            }

            // 是否还有印象？每个packet其实都是有一个自增的seq序号
            long seqno = ack.getSeqno();
            // processes response status from datanodes.
            for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
              // 在这里每次循环通过ack.getReply(i)，就可以获取到每个datanode给返回的一个结果
              final Status reply = ack.getReply(i);
              // Restart will not be treated differently unless it is
              // the local node or the only one in the pipeline.
              if (PipelineAck.isRestartOOBStatus(reply) &&
                  shouldWaitForRestart(i)) {
                restartDeadline = dfsClient.getConf().datanodeRestartTimeout +
                    Time.now();
                setRestartingNodeIndex(i);
                String message = "A datanode is restarting: " + targets[i];
                DFSClient.LOG.info(message);
               throw new IOException(message);
              }
              // node error
              // 我们在这里可以来看一下，他一定会感知到node error了
              if (reply != SUCCESS) {
                setErrorIndex(i); // first bad datanode
                throw new IOException("Bad response " + reply +
                    " for block " + block +
                    " from datanode " + 
                    targets[i]);
              }
            }
            
            assert seqno != PipelineAck.UNKOWN_SEQNO : 
              "Ack for unknown seqno should be a failed ack: " + ack;
            if (seqno == Packet.HEART_BEAT_SEQNO) {  // a heartbeat ack
              continue;
            }

            // a success ack for a data packet
            Packet one;
            synchronized (dataQueue) {
              one = ackQueue.getFirst();
            }
            if (one.seqno != seqno) {
              throw new IOException("ResponseProcessor: Expecting seqno " +
                                    " for block " + block +
                                    one.seqno + " but received " + seqno);
            }
            isLastPacketInBlock = one.lastPacketInBlock;

            // Fail the packet write for testing in order to force a
            // pipeline recovery.
            if (DFSClientFaultInjector.get().failPacket() &&
                isLastPacketInBlock) {
              failPacket = true;
              throw new IOException(
                    "Failing the last packet for testing.");
            }
              
            // update bytesAcked
            block.setNumBytes(one.getLastByteOffsetBlock());

            synchronized (dataQueue) {
              lastAckedSeqno = seqno;
              pipelineRecoveryCount = 0;
              ackQueue.removeFirst();
              dataQueue.notifyAll();

              one.releaseBuffer(byteArrayManager);
            }
          } catch (Exception e) {
            if (!responderClosed) {
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              hasError = true;
              // If no explicit error report was received, mark the primary
              // node as failed.
              tryMarkPrimaryDatanodeFailed();
              synchronized (dataQueue) {
                dataQueue.notifyAll();
              }
              if (restartingNodeIndex == -1) {
                DFSClient.LOG.warn("DFSOutputStream ResponseProcessor exception "
                     + " for block " + block, e);
              }
              responderClosed = true;
            }
          }
        }
      }
      
      // 关闭一个核心的工作线程
      // 这是非常经典的一个做法，我在java并发课程里全部都讲解了里面的原理
      // 包括都演示了我们是如何通过一个手写微服务注册中心的项目全程演示了一模一样的关闭线程的机制
      void close() {
    	// volatile的变量  
        responderClosed = true;
        // 因为你的这个线程可能处于一个wait等待的状态，或者是sleep休眠的状态
        // 所以你要打断一下这个线程的等待、休眠的状态，让他尽快结束
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError() throws IOException {
      if (response != null) {
        DFSClient.LOG.info("Error Recovery for " + block +
        " waiting for responder to exit. ");
        return true;
      }
      
      // 直接将针对primary datanode的socket以及IO流的资源全部释放
      closeStream();

      // move packets from ack queue to front of the data queue
      synchronized (dataQueue) {
    	// 将ackQeueu中的packet都重新加回到dataQueue中去  
    	// 清空掉ackQueue
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      // If we had to recover the pipeline five times in a row for the
      // same packet, this client likely has corrupt data or corrupting
      // during transmission.
      if (restartingNodeIndex == -1 && ++pipelineRecoveryCount > 5) {
        DFSClient.LOG.warn("Error recovering pipeline for writing " +
            block + ". Already retried 5 times for the same packet.");
        lastException.set(new IOException("Failing write. Tried pipeline " +
            "recovery 5 times without success."));
        streamerClosed = true;
        return false;
      }

      // 为了数据恢复而建立数据管道
      boolean doSleep = setupPipelineForAppendOrRecovery();
      
      if (!streamerClosed && dfsClient.clientRunning) {
        if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

          // If we had an error while closing the pipeline, we go through a fast-path
          // where the BlockReceiver does not run. Instead, the DataNode just finalizes
          // the block immediately during the 'connect ack' process. So, we want to pull
          // the end-of-block packet from the dataQueue, since we don't actually have
          // a true pipeline to send it over.
          //
          // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
          // a client waiting on close() will be aware that the flush finished.
          synchronized (dataQueue) {
            Packet endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
            assert endOfBlockPacket.lastPacketInBlock;
            assert lastAckedSeqno == endOfBlockPacket.seqno - 1;
            lastAckedSeqno = endOfBlockPacket.seqno;
            pipelineRecoveryCount = 0;
            dataQueue.notifyAll();
          }
          endBlock();
        } else {
          // 之前是关闭了对应的PacketResponder
          // 使用了新的datanode pipeline，此时会重新分配一个新的PacketResponder线程
          initDataStreaming();
        }
      }
      
      return doSleep;
    }

    private void setHflush() {
      isHflushed = true;
    }

    private int findNewDatanode(final DatanodeInfo[] original
        ) throws IOException {
      if (nodes.length != original.length + 1) {
        throw new IOException(
            new StringBuilder()
            .append("Failed to replace a bad datanode on the existing pipeline ")
            .append("due to no more good datanodes being available to try. ")
            .append("(Nodes: current=").append(Arrays.asList(nodes))
            .append(", original=").append(Arrays.asList(original)).append("). ")
            .append("The current failed datanode replacement policy is ")
            .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
            .append("a client may configure this via '")
            .append(DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY)
            .append("' in its configuration.")
            .toString());
      }
      for(int i = 0; i < nodes.length; i++) {
        int j = 0;
        for(; j < original.length && !nodes[i].equals(original[j]); j++);
        if (j == original.length) {
          return i;
        }
      }
      throw new IOException("Failed: new datanode not found: nodes="
          + Arrays.asList(nodes) + ", original=" + Arrays.asList(original));
    }

    private void addDatanode2ExistingPipeline() throws IOException {
      if (DataTransferProtocol.LOG.isDebugEnabled()) {
        DataTransferProtocol.LOG.debug("lastAckedSeqno = " + lastAckedSeqno);
      }
      /*
       * Is data transfer necessary?  We have the following cases.
       * 
       * Case 1: Failure in Pipeline Setup
       * - Append
       *    + Transfer the stored replica, which may be a RBW or a finalized.
       * - Create
       *    + If no data, then no transfer is required.
       *    + If there are data written, transfer RBW. This case may happens 
       *      when there are streaming failure earlier in this pipeline.
       *
       * Case 2: Failure in Streaming
       * - Append/Create:
       *    + transfer RBW
       * 
       * Case 3: Failure in Close
       * - Append/Create:
       *    + no transfer, let NameNode replicates the block.
       */
      if (!isAppend && lastAckedSeqno < 0
          && stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
        //no data have been written
        return;
      } else if (stage == BlockConstructionStage.PIPELINE_CLOSE
          || stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        //pipeline is closing
        return;
      }

      //get a new datanode
      // 通过rpc的接口调用，找到了namenode获取了一个新的datanode
      // 大概在这里都可以进行这么一个推测
      // 在这里调用了namenode的方法之后，人家肯定是基于机架感知的特性，给datanode列表中加入一个datanode
      // 人家这里会传输过去failed列表，告诉namenode再次挑选datanode的时候，不要使用那个有故障的datanode了l
      final DatanodeInfo[] original = nodes;
      final LocatedBlock lb = dfsClient.namenode.getAdditionalDatanode(
          src, fileId, block, nodes, storageIDs,
          failed.toArray(new DatanodeInfo[failed.size()]),
          1, dfsClient.clientName);
      setPipeline(lb);

      //find the new datanode
      // 找到那个namenode给分配的新的datanode是哪个
      final int d = findNewDatanode(original);

      //transfer replica
      final DatanodeInfo src = d == 0? nodes[1]: nodes[d - 1];
      final DatanodeInfo[] targets = {nodes[d]};
      final StorageType[] targetStorageTypes = {storageTypes[d]};
      transfer(src, targets, targetStorageTypes, lb.getBlockToken());
    }

    private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
        final StorageType[] targetStorageTypes,
        final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //transfer replica to the new datanode
      // 如果一旦找namenode建立了一个新的datanode列表之后
      // 他就会在这里通过transfer方法，建立好跟primary datanode的socket、IO流资源
      // 发送一个block过去
      // 完成这个数据管道的这么一个建立
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock = createSocketForPipeline(src, 2, dfsClient);
        final long writeTimeout = dfsClient.getDatanodeWriteTimeout(2);
        
        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(sock);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(sock,
          unbufOut, unbufIn, dfsClient, blockToken, src);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        in = new DataInputStream(unbufIn);

        //send the TRANSFER_BLOCK request
        new Sender(out).transferBlock(block, blockToken, dfsClient.clientName,
            targets, targetStorageTypes);
        out.flush();

        //ack
        BlockOpResponseProto response =
          BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
        if (SUCCESS != response.getStatus()) {
          throw new IOException("Failed to add a datanode");
        }
      } finally {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }

    /**
     * Open a DataOutputStream to a DataNode pipeline so that 
     * it can be written to.
     * This happens when a file is appended or data streaming fails
     * It keeps on trying until a pipeline is setup
     */
    private boolean setupPipelineForAppendOrRecovery() throws IOException {
      // check number of datanodes
      if (nodes == null || nodes.length == 0) {
        String msg = "Could not get block locations. " + "Source file \""
            + src + "\" - Aborting...";
        DFSClient.LOG.warn(msg);
        setLastException(new IOException(msg));
        streamerClosed = true;
        return false;
      }
      
      boolean success = false;
      long newGS = 0L;
      while (!success && !streamerClosed && dfsClient.clientRunning) {
        // Sleep before reconnect if a dn is restarting.
        // This process will be repeated until the deadline or the datanode
        // starts back up.
        if (restartingNodeIndex >= 0) {
          // 4 seconds or the configured deadline period, whichever is shorter.
          // This is the retry interval and recovery will be retried in this
          // interval until timeout or success.
          long delay = Math.min(dfsClient.getConf().datanodeRestartTimeout,
              4000L);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            lastException.set(new IOException("Interrupted while waiting for " +
                "datanode to restart. " + nodes[restartingNodeIndex]));
            streamerClosed = true;
            return false;
          }
        }
        boolean isRecovery = hasError;
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex >= 0) {
          StringBuilder pipelineMsg = new StringBuilder();
          for (int j = 0; j < nodes.length; j++) {
            pipelineMsg.append(nodes[j]);
            if (j < nodes.length - 1) {
              pipelineMsg.append(", ");
            }
          }
          if (nodes.length <= 1) {
            lastException.set(new IOException("All datanodes " + pipelineMsg
                + " are bad. Aborting..."));
            streamerClosed = true;
            return false;
          }
          DFSClient.LOG.warn("Error Recovery for block " + block +
              " in pipeline " + pipelineMsg + 
              ": bad datanode " + nodes[errorIndex]);
          
          // 这里做了一个记录，标记errorIndex = 0，标记primary datanode是故障的
          failed.add(nodes[errorIndex]);

          // 这个是什么意思呢？他的这个意思，就是说，将故障的datanode之后的好的datanode
          // 放入一个新的数组中去
          // [datanode01, datanode02, datanode03]
          // [datanode02, datanode03]
          DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
          arraycopy(nodes, newnodes, errorIndex);

          final StorageType[] newStorageTypes = new StorageType[newnodes.length];
          arraycopy(storageTypes, newStorageTypes, errorIndex);

          final String[] newStorageIDs = new String[newnodes.length];
          arraycopy(storageIDs, newStorageIDs, errorIndex);
          
          // 目前的数据管道里，datanode就只有2个，[datanode02，datanode03]
          setPipeline(newnodes, newStorageTypes, newStorageIDs);

          // Just took care of a node error while waiting for a node restart
          if (restartingNodeIndex >= 0) {
            // If the error came from a node further away than the restarting
            // node, the restart must have been complete.
            if (errorIndex > restartingNodeIndex) {
              restartingNodeIndex = -1;
            } else if (errorIndex < restartingNodeIndex) {
              // the node index has shifted.
              restartingNodeIndex--;
            } else {
              // this shouldn't happen...
              assert false;
            }
          }

          if (restartingNodeIndex == -1) {
            hasError = false;
          }
          lastException.set(null);
          errorIndex = -1;
        }

        // 如果replace-datanode策略是允许的话
        // 他可以将之前故障的那个datanode给替换成一个新的良好的datanode
        // Check if replace-datanode policy is satisfied.
        if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(blockReplication,
            nodes, isAppend, isHflushed)) {
          try {
        	// 他可以在pipeline里再加入一个新的datanode，满足3个datanode的条件
        	// 他如果要执行这些代码，是要满足一些条件的，才能是找namenode来申请一个新的datanode加入管道中
        	// 重新建立管道的连接
            addDatanode2ExistingPipeline();
          } catch(IOException ioe) {
            if (!dfsClient.dtpReplaceDatanodeOnFailure.isBestEffort()) {
              throw ioe;
            }
            DFSClient.LOG.warn("Failed to replace datanode."
                + " Continue with the remaining datanodes since "
                + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_BEST_EFFORT_KEY
                + " is set to true.", ioe);
          }
        }

        // get a new generation stamp and an access token
        // 找namenode为数据管道更新一下block，大概的这个意思就是更新一下block的generation stamp，创建的时间戳
        LocatedBlock lb = dfsClient.namenode.updateBlockForPipeline(block, dfsClient.clientName);
        newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getBlockToken();
        
        // set up the pipeline again with the remaining nodes
        if (failPacket) { // for testing
          success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
          failPacket = false;
          try {
            // Give DNs time to send in bad reports. In real situations,
            // good reports should follow bad ones, if client committed
            // with those nodes.
            Thread.sleep(2000);
          } catch (InterruptedException ie) {}
        } else {
          
          success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
        }

        if (restartingNodeIndex >= 0) {
          assert hasError == true;
          // check errorIndex set above
          if (errorIndex == restartingNodeIndex) {
            // ignore, if came from the restarting node
            errorIndex = -1;
          }
          // still within the deadline
          if (Time.now() < restartDeadline) {
            continue; // with in the deadline
          }
          // expired. declare the restarting node dead
          restartDeadline = 0;
          int expiredNodeIndex = restartingNodeIndex;
          restartingNodeIndex = -1;
          DFSClient.LOG.warn("Datanode did not restart in time: " +
              nodes[expiredNodeIndex]);
          // Mark the restarting node as failed. If there is any other failed
          // node during the last pipeline construction attempt, it will not be
          // overwritten/dropped. In this case, the restarting node will get
          // excluded in the following attempt, if it still does not come up.
          if (errorIndex == -1) {
            errorIndex = expiredNodeIndex;
          }
          // From this point on, normal pipeline recovery applies.
        }
      } // while

      // 如果说对新的datanode列表，建立管道是成功了
      if (success) {
        // update pipeline at the namenode
        ExtendedBlock newBlock = new ExtendedBlock(
            block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(), newGS);
        dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock,
            nodes, storageIDs);
        // update client side generation stamp
        block = newBlock;
      }
      return false; // do not sleep, continue processing
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     * 
     * 这个方法，其实是负责打开针对一个datanode的输出流，后面可以通过这个输出流写数据到datanode上去
     * 这个操作一般会在一个文件被创建的时候，或者是一个新的block被分配出来的时候
     * 此时，比如说你要往一个datanode写block数据，你必须知道那个block对应的datanode是谁呢？
     * 你要知道datanode，又必须知道你要写的是哪个block啊？
     * 
     * 所以此时，在这个方法中，你需要找namenode申请一个新的block，人家会给你一个blockId
     * 包括这个block对应的各个datanode的节点，他的副本分布在哪些节点上
     * 
     */
    private LocatedBlock nextBlockOutputStream() throws IOException {
      LocatedBlock lb = null;
      DatanodeInfo[] nodes = null;
      StorageType[] storageTypes = null;
      int count = dfsClient.getConf().nBlockWriteRetry;
      boolean success = false;
      ExtendedBlock oldBlock = block;
      do {
        hasError = false;
        lastException.set(null);
        errorIndex = -1;
        success = false;

        long startTime = Time.now();
        DatanodeInfo[] excluded =
            excludedNodes.getAllPresent(excludedNodes.asMap().keySet())
            .keySet()
            .toArray(new DatanodeInfo[0]);
        block = oldBlock;
        
        // 我们在这里推测，估计很有可能找namenode申请一个新的block
        // 其实应该就在这里，对不对？
        // 通过这个方法，就可以在这里获取到一个block以及对应的datanode地址
        lb = locateFollowingBlock(startTime,
            excluded.length > 0 ? excluded : null);
        block = lb.getBlock();
        block.setNumBytes(0);
        bytesSent = 0;
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();
        storageTypes = lb.getStorageTypes();

        //
        // Connect to first DataNode in the list.
        // 直接对block的datnode列表中的第一个datanode，建立一个到他的连接以及输出流
        // 其实这个东西的背后，你在建立连接的时候，就直接把一个贯穿了3个dataonde的数据管道的东西
        // 给建立好了
        // 建立数据管道的过程是非常的复杂的
        success = createBlockOutputStream(nodes, storageTypes, 0L, false);

        if (!success) {
          DFSClient.LOG.info("Abandoning " + block);
          // 这里是他的一个故障处理的机制
          // 我们猜测，如果在这里尝试连接到这个block的第一个datanode失败了
          // 此处应该会找namenode，抛弃掉这个block
          // 下一轮循环，重试，找namenode重新再申请一个block以及其他的datanode的列表
          // 可以猜测，一定会上报这个故障的datanode给namenode
          // namenode在机架感知算法里，下次给新的block分配datanode时候，就会避免分配给有问题的datanode
          dfsClient.namenode.abandonBlock(block, fileId, src,
              dfsClient.clientName);
          block = null;
          DFSClient.LOG.info("Excluding datanode " + nodes[errorIndex]);
          // 那他下次再重新申请block的时候
          // 肯定会告诉namenode一个excludedNodes列表，说你给block绑定datanode的时候
          // 切记，兄弟，不要再搞那个有问题的datanode了
          excludedNodes.put(nodes[errorIndex], nodes[errorIndex]);
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return lb;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    // 其实就是跟datanode list中的第一个datanode
    // 通过建立socket连接
    // 发送请求过去，完成连接的创建
    // 这个方法就负责跟第一个datanode建立连接，然后由第一个datanode连接其他的datanode
    // 建立起来一个数据管道
    private boolean createBlockOutputStream(DatanodeInfo[] nodes,
        StorageType[] nodeStorageTypes, long newGS, boolean recoveryFlag) {
      if (nodes.length == 0) {
        DFSClient.LOG.info("nodes are empty for write pipeline of block "
            + block);
        return false;
      }
      Status pipelineStatus = SUCCESS;
      String firstBadLink = "";
      boolean checkRestart = false;
      if (DFSClient.LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          DFSClient.LOG.debug("pipeline = " + nodes[i]);
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks.set(true);

      int refetchEncryptionKey = 1;
      while (true) {
        boolean result = false;
        DataOutputStream out = null;
        try {
          assert null == s : "Previous socket unclosed";
          assert null == blockReplyStream : "Previous blockReplyStream unclosed";
          // 针对datanode list中的第一个node
          // 创建了一个针对第一个datanode的一个socket连接
          // Java基础，我们的java架构课里
          // 并发、IO+网络+Netty，那个里面就会讲解socket网络编程
          s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
          long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
          
          // 构建起针对那个datanode的socket
          // 获取输出流，可以用于向datanode通过socket链接写数据
          // 获取输入流，可以通过socket连接从datanode读取数据过来
          OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
          InputStream unbufIn = NetUtils.getInputStream(s);
          
          IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
          unbufOut = saslStreams.out;
          unbufIn = saslStreams.in;
          // 封装了一个DataOutputStream
          // 这个流的底层流是，BufferedOutputStream + socket的输出流
          // 分布式系统底层大量的东西，并发、集合、IO、网络、磁盘、CPU、内存
          out = new DataOutputStream(new BufferedOutputStream(unbufOut,
              HdfsConstants.SMALL_BUFFER_SIZE));
          // 用DataInputStream包裹了一下socket的输入流，可以读取到datanode返回的数据
          // 也就是说通过blockReplyStream就可以从你hdfs客户端直接对接的第一个datanode获取输入流
          // 可以通过这个流读取第一个datanode返回的响应消息
          blockReplyStream = new DataInputStream(unbufIn);
  
          //
          // Xmit header info to datanode
          //
  
          BlockConstructionStage bcs = recoveryFlag? stage.getRecoveryStage(): stage;

          // We cannot change the block length in 'block' as it counts the number
          // of bytes ack'ed.
          ExtendedBlock blockCopy = new ExtendedBlock(block);
          blockCopy.setNumBytes(blockSize);

          // send the request
          // 基于输出流，向datanode发送了一个请求来建立连接
          // 此时发送过去的block其实还是空的
          new Sender(out).writeBlock(blockCopy, nodeStorageTypes[0], accessToken,
              dfsClient.clientName, nodes, nodeStorageTypes, null, bcs, 
              nodes.length, block.getNumBytes(), bytesSent, newGS,
              checksum4WriteBlock, cachingStrategy.get(), isLazyPersistFile);
  
          // receive ack for connect
          // 等待获取datanode那边返回的建立好连接的ack消息
          BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
              PBHelper.vintPrefixed(blockReplyStream));
          pipelineStatus = resp.getStatus();
          firstBadLink = resp.getFirstBadLink();
          
          // Got an restart OOB ack.
          // If a node is already restarting, this status is not likely from
          // the same node. If it is from a different node, it is not
          // from the local datanode. Thus it is safe to treat this as a
          // regular node error.
          
          // 以后大家自己写代码的时候，对于这种代码，就一大堆的异常检查的代码
          // 完全可以抽到很多的小的私有方法里去
          // 让代码更加的清晰
          
          if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            restartingNodeIndex == -1) {
            checkRestart = true;
            throw new IOException("A datanode is restarting.");
          }
          if (pipelineStatus != SUCCESS) {
            if (pipelineStatus == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack with firstBadLink as "
                      + firstBadLink);
            } else {
              throw new IOException("Bad connect ack with firstBadLink as "
                  + firstBadLink);
            }
          }
          assert null == blockStream : "Previous blockStream unclosed";
          blockStream = out;
          result =  true; // success
          restartingNodeIndex = -1;
          hasError = false;
        } catch (IOException ie) {
          if (restartingNodeIndex == -1) {
            DFSClient.LOG.info("Exception in createBlockOutputStream", ie);
          }
          if (ie instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
            DFSClient.LOG.info("Will fetch a new encryption key and retry, " 
                + "encryption key was invalid when connecting to "
                + nodes[0] + " : " + ie);
            // The encryption key used is invalid.
            refetchEncryptionKey--;
            dfsClient.clearDataEncryptionKey();
            // Don't close the socket/exclude this node just yet. Try again with
            // a new encryption key.
            continue;
          }
  
          // find the datanode that matches
          if (firstBadLink.length() != 0) {
            for (int i = 0; i < nodes.length; i++) {
              // NB: Unconditionally using the xfer addr w/o hostname
              if (firstBadLink.equals(nodes[i].getXferAddr())) {
                errorIndex = i;
                break;
              }
            }
          } else {
            assert checkRestart == false;
            errorIndex = 0;
          }
          // Check whether there is a restart worth waiting for.
          if (checkRestart && shouldWaitForRestart(errorIndex)) {
            restartDeadline = dfsClient.getConf().datanodeRestartTimeout +
                Time.now();
            restartingNodeIndex = errorIndex;
            errorIndex = -1;
            DFSClient.LOG.info("Waiting for the datanode to be restarted: " +
                nodes[restartingNodeIndex]);
          }
          hasError = true;
          setLastException(ie);
          result =  false;  // error
        } finally {
          if (!result) {
            IOUtils.closeSocket(s);
            s = null;
            IOUtils.closeStream(out);
            out = null;
            IOUtils.closeStream(blockReplyStream);
            blockReplyStream = null;
          }
        }
        return result;
      }
    }

    private LocatedBlock locateFollowingBlock(long start,
        DatanodeInfo[] excludedNodes)  throws IOException {
      int retries = dfsClient.getConf().nBlockWriteLocateFollowingRetry;
      long sleeptime = 400;
      while (true) {
        long localstart = Time.now();
        while (true) {
          try {
        	// dfsClient的namenode rpc代理
        	// 调用rpc接口，addBlock()接口，申请新分配一个block
        	// 哪个客户端，要为哪个文件多申请一个block
            return dfsClient.namenode.addBlock(src, dfsClient.clientName,
                block, excludedNodes, fileId, favoredNodes);
          } catch (RemoteException e) {
            IOException ue = 
              e.unwrapRemoteException(FileNotFoundException.class,
                                      AccessControlException.class,
                                      NSQuotaExceededException.class,
                                      DSQuotaExceededException.class,
                                      UnresolvedPathException.class);
            if (ue != e) { 
              throw ue; // no need to retry these exceptions
            }
            
            
            if (NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {
              if (retries == 0) { 
                throw e;
              } else {
                --retries;
                DFSClient.LOG.info("Exception while adding a block", e);
                if (Time.now() - localstart > 5000) {
                  DFSClient.LOG.info("Waiting for replication for "
                      + (Time.now() - localstart) / 1000
                      + " seconds");
                }
                try {
                  DFSClient.LOG.warn("NotReplicatedYetException sleeping " + src
                      + " retries left " + retries);
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                  DFSClient.LOG.warn("Caught exception ", ie);
                }
              }
            } else {
              throw e;
            }

          }
        }
      } 
    }

    ExtendedBlock getBlock() {
      return block;
    }

    DatanodeInfo[] getNodes() {
      return nodes;
    }

    Token<BlockTokenIdentifier> getBlockToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      lastException.compareAndSet(null, e);
    }
  }

  /**
   * Create a socket for a write pipeline
   * @param first the first datanode 
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final String dnAddr = first.getXferAddr(
        client.getConf().connectToDnViaHostname);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), client.getConf().socketTimeout);
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if(DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }

  protected void checkClosed() throws IOException {
    if (closed) {
      IOException e = lastException.get();
      throw e != null ? e : new ClosedChannelException();
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  @VisibleForTesting
  public synchronized DatanodeInfo[] getPipeline() {
    if (streamer == null) {
      return null;
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return null;
    }
    DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
    for (int i = 0; i < currentNodes.length; i++) {
      value[i] = currentNodes[i];
    }
    return value;
  }

  /** 
   * @return the object for computing checksum.
   *         The type is NULL if checksum is not computed.
   */
  private static DataChecksum getChecksum4Compute(DataChecksum checksum,
      HdfsFileStatus stat) {
    if (isLazyPersist(stat) && stat.getReplication() == 1) {
      // do not compute checksum for writing to single replica to memory
      return DataChecksum.newDataChecksum(Type.NULL,
          checksum.getBytesPerChecksum());
    }
    return checksum;
  }
 
  private DFSOutputStream(DFSClient dfsClient, String src, Progressable progress,
      HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    super(getChecksum4Compute(checksum, stat));
    this.dfsClient = dfsClient;
    this.src = src;
    this.fileId = stat.getFileId();
    // blockSize就是你配置的 一个block是划分成多少MB一个块，默认反正就是128MB
    this.blockSize = stat.getBlockSize();
    // 你配置的是一个block有几个副本，3个副本
    this.blockReplication = stat.getReplication();
    this.fileEncryptionInfo = stat.getFileEncryptionInfo();
    this.progress = progress;
    this.cachingStrategy = new AtomicReference<CachingStrategy>(
        dfsClient.getDefaultWriteCachingStrategy());
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug(
          "Set non-null progress callback on DFSOutputStream " + src);
    }
    
    // 每个校验和块，有多少个字节
    this.bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum <= 0) {
      throw new HadoopIllegalArgumentException(
          "Invalid value: bytesPerChecksum = " + bytesPerChecksum + " <= 0");
    }
    if (blockSize % bytesPerChecksum != 0) {
      throw new HadoopIllegalArgumentException("Invalid values: "
          + DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (=" + bytesPerChecksum
          + ") must divide block size (=" + blockSize + ").");
    }
    this.checksum4WriteBlock = checksum;

    this.dfsclientSlowLogThresholdMs =
      dfsClient.getConf().dfsclientSlowIoWarningThresholdMs;
    this.byteArrayManager = dfsClient.getClientContext().getByteArrayManager();
  }

  /** Construct a new output stream for creating a file. */
  private DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
      EnumSet<CreateFlag> flag, Progressable progress,
      DataChecksum checksum, String[] favoredNodes) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

    // 一个package是64mb
    computePacketChunkSize(dfsClient.getConf().writePacketSize, bytesPerChecksum);

    Span traceSpan = null;
    if (Trace.isTracing()) {
      traceSpan = Trace.startSpan(this.getClass().getSimpleName()).detach();
    }
    // 最最核心的，在DFSOutputStream中，核心的组件是一个数据流组件
    // 也就是DataStreamer组件
    streamer = new DataStreamer(stat, traceSpan);
    if (favoredNodes != null && favoredNodes.length != 0) {
      streamer.setFavoredNodes(favoredNodes);
    }
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
      FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, Progressable progress, int buffersize,
      DataChecksum checksum, String[] favoredNodes) throws IOException {
    HdfsFileStatus stat = null;

    // Retry the create if we get a RetryStartFileException up to a maximum
    // number of times
    boolean shouldRetry = true;
    // 很明显，这个重要的操作，可以支持指定次数的重试，很明显这个东西是写死
    // 默认可以重试10次
    int retryCount = CREATE_RETRY_COUNT;
    
    // 进入了一个可以支持重试的这么while循环
    // 猜测：他会在里面干一件非常重要的事情，如果干成功了，就退出这个while循环
    // 如果干失败了，可以设置shouldRetry = true
    // 下一次循环再次来尝试一下，重试这个重要的操作，我们看到代码
    while (shouldRetry) {
      shouldRetry = false;
      try {
    	// 走了一个rpc接口调用
    	// 去调用了namenode的一个rpc接口，create()接口
    	// 我告诉create()接口是干嘛的，非常的简单，你自己想一下，如果你在一个文件系统里创建一个文件的话
    	// 是不是要去更新那个文件目录树，也就是更新文件系统的元数据
    	// 文件目录树里，人家namenode就需要在内存的文件目录树里加入一个文件，挂在某个目录下面
    	// 所以告诉大家一点，这里干的就是这个事儿
    	// 如果你要针对一个hdfs文件创建一个输出流，首先人家就会调用namenode rpc接口，尝试在
    	// 内存的文件目录树中加入这个文件，同时写会去写edits log
        stat = dfsClient.namenode.create(src, masked, dfsClient.clientName,
            new EnumSetWritable<CreateFlag>(flag), createParent, replication,
            blockSize, SUPPORTED_CRYPTO_VERSIONS);
        break;
      } catch (RemoteException re) {
        IOException e = re.unwrapRemoteException(
            AccessControlException.class,
            DSQuotaExceededException.class,
            FileAlreadyExistsException.class,
            FileNotFoundException.class,
            ParentNotDirectoryException.class,
            NSQuotaExceededException.class,
            RetryStartFileException.class,
            SafeModeException.class,
            UnresolvedPathException.class,
            SnapshotAccessControlException.class,
            UnknownCryptoProtocolVersionException.class);
        if (e instanceof RetryStartFileException) {
          if (retryCount > 0) {
            shouldRetry = true;
            retryCount--;
          } else {
            throw new IOException("Too many retries because of encryption" +
                " zone operations", e);
          }
        } else {
          throw e;
        }
      }
    }
    Preconditions.checkNotNull(stat, "HdfsFileStatus should not be null!");
    // 构造和初始化DFSOutputStream
    // 核心的输出流组件
    final DFSOutputStream out = new DFSOutputStream(dfsClient, src, stat,
        flag, progress, checksum, favoredNodes);
    // 很关键的这个行为，就是启动了这个DFSOutputStream
    out.start();
    return out;
  }

  /** Construct a new output stream for append. */
  private DFSOutputStream(DFSClient dfsClient, String src,
      Progressable progress, LocatedBlock lastBlock, HdfsFileStatus stat,
      DataChecksum checksum) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    initialFileSize = stat.getLen(); // length of file when opened

    Span traceSpan = null;
    if (Trace.isTracing()) {
      traceSpan = Trace.startSpan(this.getClass().getSimpleName()).detach();
    }

    // The last partial block of the file has to be filled.
    if (lastBlock != null) {
      // indicate that we are appending to an existing block
      bytesCurBlock = lastBlock.getBlockSize();
      streamer = new DataStreamer(lastBlock, stat, bytesPerChecksum, traceSpan);
    } else {
      computePacketChunkSize(dfsClient.getConf().writePacketSize, bytesPerChecksum);
      streamer = new DataStreamer(stat, traceSpan);
    }
    this.fileEncryptionInfo = stat.getFileEncryptionInfo();
  }

  static DFSOutputStream newStreamForAppend(DFSClient dfsClient, String src,
      int buffersize, Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    final DFSOutputStream out = new DFSOutputStream(dfsClient, src,
        progress, lastBlock, stat, checksum);
    out.start();
    return out;
  }
  
  private static boolean isLazyPersist(HdfsFileStatus stat) {
    final BlockStoragePolicy p = blockStoragePolicySuite.getPolicy(
        HdfsConstants.MEMORY_STORAGE_POLICY_NAME);
    return p != null && stat.getStoragePolicy() == p.getId();
  }

  private void computePacketChunkSize(int psize, int csize) {
    final int chunkSize = csize + getChecksumSize();
    // 一个是package里面可以包含几个chunk，默认是127个chunk
    // chunkSize = 516字节
    // psize = 65532字节
    chunksPerPacket = Math.max(psize/chunkSize, 1);
    // package的大小，chunk的大小
    // chunkSize就是512字节，chunksPerPacket每个packet有多少个chunk
    // 一个packet是65532个字节，大概是64mb
    // chunkSize = 516字节 * 127 = 64mb
    
    // csize = 512字节
    // checkSumSize = 4字节
    // chunkSize = 516字节
    // 每个chunk不是都带了一个checksum，所以说一个checksum + chunk自己的数据 = 完成的chunk
    // 所以一个完整的chunk的数据是：512字节的chunk数据 + 4字节的checksum数据 = 516字节
    
    // csize = 512字节
    // checkSumSize = 4字节
    // chunkSize = 516字节
    // psize = 64mb * 1024字节 = 65536字节
    // chunksPerPacket 约等于 127个（每个chunk包含了一个数据+校验和）
    
    // packetSize = 516 * 127 = 65532字节
    
    packetSize = chunkSize*chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
                ", chunkSize=" + chunkSize +
                ", chunksPerPacket=" + chunksPerPacket +
                ", packetSize=" + packetSize);
    }
  }

  private void queueCurrentPacket() {
    synchronized (dataQueue) {
      if (currentPacket == null) return;
      // 直接在队列尾部加入一个packet
      dataQueue.addLast(currentPacket);
      // 更新一下packet的序号，每个packet都有一个从0开始递增的一个序号
      lastQueuedSeqno = currentPacket.seqno;
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Queued packet " + currentPacket.seqno);
      }
      // 接下来再写数据，就重新开启一个新的packet了
      currentPacket = null;
      dataQueue.notifyAll();
    }
  }

  private void waitAndQueueCurrentPacket() throws IOException {
    synchronized (dataQueue) {
      try {
      // If queue is full, then wait till we have enough space
      // 如果队列是满的话，此时就进入阻塞方式，while循环等待
      while (!closed && dataQueue.size() + ackQueue.size()  > dfsClient.getConf().writeMaxPackets) {
        try {
          dataQueue.wait();
        } catch (InterruptedException e) {
          // If we get interrupted while waiting to queue data, we still need to get rid
          // of the current packet. This is because we have an invariant that if
          // currentPacket gets full, it will get queued before the next writeChunk.
          //
          // Rather than wait around for space in the queue, we should instead try to
          // return to the caller as soon as possible, even though we slightly overrun
          // the MAX_PACKETS length.
          Thread.currentThread().interrupt();
          break;
        }
      }
      checkClosed();
      queueCurrentPacket();
      } catch (ClosedChannelException e) {
      }
    }
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    dfsClient.checkOpen();
    checkClosed();

    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
                            " is larger than supported  bytesPerChecksum " +
                            bytesPerChecksum);
    }
    if (cklen != 0 && cklen != getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
                            getChecksumSize() + " but found to be " + cklen);
    }

    // 在这里，他会判断一下，当前如果packet是空的话，就会新建一个packet出来
    if (currentPacket == null) {
      currentPacket = createPacket(packetSize, chunksPerPacket, 
          bytesCurBlock, currentSeqno++);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
            currentPacket.seqno +
            ", src=" + src +
            ", packetSize=" + packetSize +
            ", chunksPerPacket=" + chunksPerPacket +
            ", bytesCurBlock=" + bytesCurBlock);
      }
    }

    // 人家往packet里写入一个checksum校验快的数据
    currentPacket.writeChecksum(checksum, ckoff, cklen);
    // 就是把chunk自身的数据，写入了packet中
    currentPacket.writeData(b, offset, len);
    // 更新你已经往这个packet里面写了几个chunk了
    currentPacket.numChunks++;
    // 当前这个packet所属的这个block，已经写了多少字节的数据量了
    bytesCurBlock += len;

    // If packet is full, enqueue it for transmission
    // 判断一下，如果当前这个packet里面的chunk的数量，已经达到了127个最大的chunk数量的大小之后
    // 或者是当前block的数据量大小，已经等于你默认配置的那个block数据量的大小
    // 都会进入这个if分支
    if (currentPacket.numChunks == currentPacket.maxChunks ||
        bytesCurBlock == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
            currentPacket.seqno +
            ", src=" + src +
            ", bytesCurBlock=" + bytesCurBlock +
            ", blockSize=" + blockSize +
            ", appendChunk=" + appendChunk);
      }
      // 他在这里，这个方法，就会将当前这个已经写满的packet，给放入到dataQueue队列中去
      waitAndQueueCurrentPacket();

      // If the reopened file did not end at chunk boundary and the above
      // write filled up its partial chunk. Tell the summer to generate full 
      // crc chunks from now on.
      if (appendChunk && bytesCurBlock%bytesPerChecksum == 0) {
        appendChunk = false;
        resetChecksumBufSize();
      }

      if (!appendChunk) {
        int psize = Math.min((int)(blockSize-bytesCurBlock), dfsClient.getConf().writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
      }
      //
      // if encountering a block boundary, send an empty packet to 
      // indicate the end of block and reset bytesCurBlock.
      //
      // 如何切割block
      // 如果当前写入一个block的数据量，已经跟预先设置好的block的大小一样了
      // 说明一个block也写满了
      // 如果一个block满了以后，比如说一个block是2个packet
      // 此时会写一个空的packet，代表说，这个block已经写满了，可以开启下一个block了
      if (bytesCurBlock == blockSize) {
        currentPacket = createPacket(0, 0, bytesCurBlock, currentSeqno++);
        currentPacket.lastPacketInBlock = true;
        currentPacket.syncBlock = shouldSyncBlock;
        waitAndQueueCurrentPacket();
        bytesCurBlock = 0;
        lastFlushOffset = 0;
      }
    }
  }

  @Override
  @Deprecated
  public void sync() throws IOException {
    hflush();
  }
  
  /**
   * Flushes out to all replicas of the block. The data is in the buffers
   * of the DNs but not necessarily in the DN's OS buffers.
   *
   * It is a synchronous operation. When it returns,
   * it guarantees that flushed data become visible to new readers. 
   * It is not guaranteed that data has been flushed to 
   * persistent store on the datanode. 
   * Block allocations are persisted on namenode.
   */
  @Override
  public void hflush() throws IOException {
    flushOrSync(false, EnumSet.noneOf(SyncFlag.class));
  }

  @Override
  public void hsync() throws IOException {
    hsync(EnumSet.noneOf(SyncFlag.class));
  }
  
  /**
   * The expected semantics is all data have flushed out to all replicas 
   * and all replicas have done posix fsync equivalent - ie the OS has 
   * flushed it to the disk device (but the disk may have it in its cache).
   * 
   * Note that only the current block is flushed to the disk device.
   * To guarantee durable sync across block boundaries the stream should
   * be created with {@link CreateFlag#SYNC_BLOCK}.
   * 
   * @param syncFlags
   *          Indicate the semantic of the sync. Currently used to specify
   *          whether or not to update the block length in NameNode.
   */
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    flushOrSync(true, syncFlags);
  }

  /**
   * Flush/Sync buffered data to DataNodes.
   * 
   * @param isSync
   *          Whether or not to require all replicas to flush data to the disk
   *          device
   * @param syncFlags
   *          Indicate extra detailed semantic of the flush/sync. Currently
   *          mainly used to specify whether or not to update the file length in
   *          the NameNode
   * @throws IOException
   */
  private void flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags)
      throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    try {
      long toWaitFor;
      long lastBlockLength = -1L;
      boolean updateLength = syncFlags.contains(SyncFlag.UPDATE_LENGTH);
      synchronized (this) {
        // flush checksum buffer, but keep checksum buffer intact
        int numKept = flushBuffer(true, true);
        // bytesCurBlock potentially incremented if there was buffered data

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug(
            "DFSClient flush() :" +
            " bytesCurBlock " + bytesCurBlock +
            " lastFlushOffset " + lastFlushOffset);
        }
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {
          assert bytesCurBlock > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;
          if (isSync && currentPacket == null) {
            // Nothing to send right now,
            // but sync was requested.
            // Send an empty packet
            currentPacket = createPacket(packetSize, chunksPerPacket,
                bytesCurBlock, currentSeqno++);
          }
        } else {
          if (isSync && bytesCurBlock > 0) {
            // Nothing to send right now,
            // and the block was partially written,
            // and sync was requested.
            // So send an empty sync packet.
            currentPacket = createPacket(packetSize, chunksPerPacket,
                bytesCurBlock, currentSeqno++);
          } else {
            // just discard the current packet since it is already been sent.
            currentPacket = null;
          }
        }
        if (currentPacket != null) {
          currentPacket.syncBlock = isSync;
          waitAndQueueCurrentPacket();          
        }
        // Restore state of stream. Record the last flush offset 
        // of the last full chunk that was flushed.
        //
        bytesCurBlock -= numKept;
        toWaitFor = lastQueuedSeqno;
      } // end synchronized

      waitForAckedSeqno(toWaitFor);

      // update the block length first time irrespective of flag
      if (updateLength || persistBlocks.get()) {
        synchronized (this) {
          if (streamer != null && streamer.block != null) {
            lastBlockLength = streamer.block.getNumBytes();
          }
        }
      }
      // If 1) any new blocks were allocated since the last flush, or 2) to
      // update length in NN is required, then persist block locations on
      // namenode.
      if (persistBlocks.getAndSet(false) || updateLength) {
        try {
          dfsClient.namenode.fsync(src, fileId,
              dfsClient.clientName, lastBlockLength);
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Unable to persist blocks in hflush for " + src, ioe);
          // If we got an error here, it might be because some other thread called
          // close before our hflush completed. In that case, we should throw an
          // exception that the stream is closed.
          checkClosed();
          // If we aren't closed but failed to sync, we should expose that to the
          // caller.
          throw ioe;
        }
      }

      synchronized(this) {
        if (streamer != null) {
          streamer.setHflush();
        }
      }
    } catch (InterruptedIOException interrupt) {
      // This kind of error doesn't mean that the stream itself is broken - just the
      // flushing thread got interrupted. So, we shouldn't close down the writer,
      // but instead just propagate the error
      throw interrupt;
    } catch (IOException e) {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!closed) {
          lastException.set(new IOException("IOException flush:" + e));
          closeThreads(true);
        }
      }
      throw e;
    }
  }

  /**
   * @deprecated use {@link HdfsDataOutputStream#getCurrentBlockReplication()}.
   */
  @Deprecated
  public synchronized int getNumCurrentReplicas() throws IOException {
    return getCurrentBlockReplication();
  }

  /**
   * Note that this is not a public API;
   * use {@link HdfsDataOutputStream#getCurrentBlockReplication()} instead.
   * 
   * @return the number of valid replicas of the current block
   */
  public synchronized int getCurrentBlockReplication() throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    if (streamer == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    return currentNodes.length;
  }
  
  /**
   * Waits till all existing data is flushed and confirmations 
   * received from datanodes. 
   */
  private void flushInternal() throws IOException {
    long toWaitFor;
    synchronized (this) {
      dfsClient.checkOpen();
      checkClosed();
      //
      // If there is data in the current buffer, send it across
      //
      queueCurrentPacket();
      toWaitFor = lastQueuedSeqno;
    }

    waitForAckedSeqno(toWaitFor);
  }

  private void waitForAckedSeqno(long seqno) throws IOException {
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Waiting for ack for: " + seqno);
    }
    long begin = Time.monotonicNow();
    try {
      synchronized (dataQueue) {
        while (!closed) {
          checkClosed();
          if (lastAckedSeqno >= seqno) {
            break;
          }
          try {
            dataQueue.wait(1000); // when we receive an ack, we notify on
                                  // dataQueue
          } catch (InterruptedException ie) {
            throw new InterruptedIOException(
                "Interrupted while waiting for data to be acknowledged by pipeline");
          }
        }
      }
      checkClosed();
    } catch (ClosedChannelException e) {
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > dfsclientSlowLogThresholdMs) {
      DFSClient.LOG.warn("Slow waitForAckedSeqno took " + duration
          + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms)");
    }
  }

  private synchronized void start() {
	// 核心的一点，就是启动了这个DataStreamer组件
    streamer.start();
  }
  
  /**
   * Aborts this output stream and releases any system 
   * resources associated with this stream.
   */
  void abort() throws IOException {
    synchronized (this) {
      if (closed) {
        return;
      }
      streamer.setLastException(new IOException("Lease timeout of "
              + (dfsClient.getHdfsTimeout() / 1000) + " seconds expired."));
      closeThreads(true);
    }
    dfsClient.endFileLease(fileId);
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  private void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      if (s != null) {
        s.close();
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer = null;
      s = null;
      closed = true;
    }
  }
  
  /**
   * Closes this output stream and releases any system 
   * resources associated with this stream.
   */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (closed) {
        IOException e = lastException.getAndSet(null);
        if (e == null)
          return;
        else
          throw e;
      }

      try {
        flushBuffer();       // flush from all upper layers

        if (currentPacket != null) {
          waitAndQueueCurrentPacket();
        }

        if (bytesCurBlock != 0) {
          // send an empty packet to mark the end of the block
          currentPacket = createPacket(0, 0, bytesCurBlock, currentSeqno++);
          currentPacket.lastPacketInBlock = true;
          currentPacket.syncBlock = shouldSyncBlock;
        }

        flushInternal();             // flush all data to Datanodes
        // get last block before destroying the streamer
        ExtendedBlock lastBlock = streamer.getBlock();
        closeThreads(false);
        completeFile(lastBlock);

      } catch (ClosedChannelException e) {
      } finally {
        closed = true;
      }
    }
    dfsClient.endFileLease(fileId);
  }

  // should be called holding (this) lock since setTestFilename() may 
  // be called during unit tests
  private void completeFile(ExtendedBlock last) throws IOException {
    long localstart = Time.now();
    long localTimeout = 400;
    boolean fileComplete = false;
    int retries = dfsClient.getConf().nBlockWriteLocateFollowingRetry;
    while (!fileComplete) {
      fileComplete =
          dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId);
      if (!fileComplete) {
        final int hdfsTimeout = dfsClient.getHdfsTimeout();
        if (!dfsClient.clientRunning ||
              (hdfsTimeout > 0 && localstart + hdfsTimeout < Time.now())) {
            String msg = "Unable to close file because dfsclient " +
                          " was unable to contact the HDFS servers." +
                          " clientRunning " + dfsClient.clientRunning +
                          " hdfsTimeout " + hdfsTimeout;
            DFSClient.LOG.info(msg);
            throw new IOException(msg);
        }
        try {
          if (retries == 0) {
            throw new IOException("Unable to close file because the last block"
                + " does not have enough number of replicas.");
          }
          retries--;
          Thread.sleep(localTimeout);
          localTimeout *= 2;
          if (Time.now() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.warn("Caught exception ", ie);
        }
      }
    }
  }

  @VisibleForTesting
  public void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  @VisibleForTesting
  public synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = (bytesPerChecksum + getChecksumSize()) * chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  public long getInitialLen() {
    return initialFileSize;
  }

  /**
   * @return the FileEncryptionInfo for this stream, or null if not encrypted.
   */
  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  synchronized Token<BlockTokenIdentifier> getBlockToken() {
    return streamer.getBlockToken();
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    CachingStrategy prevStrategy, nextStrategy;
    // CachingStrategy is immutable.  So build a new CachingStrategy with the
    // modifications we want, and compare-and-swap it in.
    do {
      prevStrategy = this.cachingStrategy.get();
      nextStrategy = new CachingStrategy.Builder(prevStrategy).
                        setDropBehind(dropBehind).build();
    } while (!this.cachingStrategy.compareAndSet(prevStrategy, nextStrategy));
  }

  @VisibleForTesting
  ExtendedBlock getBlock() {
    return streamer.getBlock();
  }

  @VisibleForTesting
  public long getFileId() {
    return fileId;
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }

  /**
   * @return The times have retried to recover pipeline, for the same packet.
   */
  @VisibleForTesting
  int getPipelineRecoveryCount() {
    return streamer.pipelineRecoveryCount;
  }
}
