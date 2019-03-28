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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddBlockOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllocateBlockIdOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CreateSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisallowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.LogSegmentOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetAclOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV1Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV2Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetStoragePolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateBlocksOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 * FSEditlog维护namespace（命名空间，元数据，文件目录树）的一系列的修改的
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog implements LogsPurgeable {

  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  /**
   * State machine for edit log.
   * 
   * In a non-HA setup:
   * 
   * The log starts in UNITIALIZED state upon construction. Once it's
   * initialized, it is usually in IN_SEGMENT state, indicating that edits may
   * be written. In the middle of a roll, or while saving the namespace, it
   * briefly enters the BETWEEN_LOG_SEGMENTS state, indicating that the previous
   * segment has been closed, but the new one has not yet been opened.
   * 
   * In an HA setup:
   * 
   * The log starts in UNINITIALIZED state upon construction. Once it's
   * initialized, it sits in the OPEN_FOR_READING state the entire time that the
   * NN is in standby. Upon the NN transition to active, the log will be CLOSED,
   * and then move to being BETWEEN_LOG_SEGMENTS, much as if the NN had just
   * started up, and then will move to IN_SEGMENT so it can begin writing to the
   * log. The log states will then revert to behaving as they do in a non-HA
   * setup.
   */
  private enum State {
    UNINITIALIZED,
    BETWEEN_LOG_SEGMENTS,
    IN_SEGMENT,
    OPEN_FOR_READING,
    CLOSED;
  }  
  private State state = State.UNINITIALIZED;
  
  //initialize
  private JournalSet journalSet = null;
  // EditLogOutputuStream的类型，我们在连蒙带猜，猜测一下
  // 用屁股想想，人家一定是一个包装流，装饰流，里面封装了好几个其他的底层的流
  // EditLogOutputStream里，封装了一个写本地磁盘的流，还封装了一个写JournalNode流
  // 你在这里写一条edits log的时候
  // EditLogOutputStream就把这条数据通过两个流写到了两个地方去
  private EditLogOutputStream editLogStream = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the first txid of the log that's currently open for writing.
  // If this value is N, we are currently writing to edits_inprogress_N
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // is an automatic sync scheduled?
  private volatile boolean isAutoSyncScheduled = false;
  
  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private final NNStorage storage;
  private final Configuration conf;
  
  private final List<URI> editsDirs;

  private final ThreadLocal<OpInstanceCache> cache =
      new ThreadLocal<OpInstanceCache>() {
    @Override
    protected OpInstanceCache initialValue() {
      return new OpInstanceCache();
    }
  };
  
  /**
   * The edit directories that are shared between primary and secondary.
   */
  private final List<URI> sharedEditsDirs;

  /**
   * Take this lock when adding journals to or closing the JournalSet. Allows
   * us to ensure that the JournalSet isn't closed or updated underneath us
   * in selectInputStreams().
   */
  private final Object journalSetLock = new Object();

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  // ThreadLocal，也就是说每个线程都可以有一个副本，每个线程在同步代码块里，通过txid++全局唯一递增之后
  // 就将这个递增好的txid放入自己的THreadLocal变量副本中
  // 后面他就使用自己的ThreadLocal中的txid就可以了，如果有其他线程再次对txid++，无所谓
  // 如果有一些搞大数据的同学，java基础比较薄弱，连ThreadLocal是什么，源码，原理是什么都不知道
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    @Override
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  /**
   * Constructor for FSEditLog. Underlying journals are constructed, but 
   * no streams are opened until open() is called.
   * 
   * @param conf The namenode configuration
   * @param storage Storage object used by namenode
   * @param editsDirs List of journals to use
   */
  FSEditLog(Configuration conf, NNStorage storage, List<URI> editsDirs) {
    isSyncRunning = false;
    this.conf = conf;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();
     
    // If this list is empty, an error will be thrown on first use
    // of the editlog, as no journals will exist
    // 我们推测，这个位置就是代表说namenode是将edits log写入到自己本地磁盘的哪个目录中
    // 默认应该就是hadoop.tmp.dir下面的一个路径
    this.editsDirs = Lists.newArrayList(editsDirs);

    // 这个是我们自己指定的就是将数据写入到哪些journal node集群上去
    this.sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
  }
  
  public synchronized void initJournalsForWrite() {
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED, "Unexpected state: %s", state);
    
    initJournals(this.editsDirs);
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  public synchronized void initSharedJournalsForRead() {
    if (state == State.OPEN_FOR_READING) {
      LOG.warn("Initializing shared journals for READ, already open for READ",
          new Exception());
      return;
    }
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED);
    
    initJournals(this.sharedEditsDirs);
    state = State.OPEN_FOR_READING;
  }
  
  private synchronized void initJournals(List<URI> dirs) {
    int minimumRedundantJournals = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT);

    synchronized(journalSetLock) {
      journalSet = new JournalSet(minimumRedundantJournals);

      for (URI u : dirs) {
        boolean required = FSNamesystem.getRequiredNamespaceEditsDirs(conf)
            .contains(u);
        // 如果当前传入进来的URI，是本地文件系统的话
        if (u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
          StorageDirectory sd = storage.getStorageDirectory(u);
          if (sd != null) {
        	// 就会创建FileJournalManager，就是专门负责将edits log写入到本次磁盘的
            journalSet.add(new FileJournalManager(conf, sd, storage),
                required, sharedEditsDirs.contains(u));
          }
        } 
        // 如果不是本地文件系统的话，那么就会在这里走createJournal()
        // 会创建出来QuorumJournalManager，是专门负责将edits log写入到JournalNode上去的
        else {
          journalSet.add(createJournal(u), required,
              sharedEditsDirs.contains(u));
        }
      }
    }
 
    if (journalSet.isEmpty()) {
      LOG.error("No edits directories configured!");
    } 
  }

  /**
   * Get the list of URIs the editlog is using for storage
   * @return collection of URIs in use by the edit log
   */
  Collection<URI> getEditURIs() {
    return editsDirs;
  }

  /**
   * Initialize the output stream for logging, opening the first
   * log segment.
   */
  synchronized void openForWrite() throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);

    long segmentTxId = getLastWrittenTxId() + 1;
    // Safety check: we should never start a segment if there are
    // newer txids readable.
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    journalSet.selectInputStreams(streams, segmentTxId, true);
    if (!streams.isEmpty()) {
      String error = String.format("Cannot start writing at txid %s " +
        "when there is a stream available for read: %s",
        segmentTxId, streams.get(0));
      IOUtils.cleanup(LOG, streams.toArray(new EditLogInputStream[0]));
      throw new IllegalStateException(error);
    }
    
    startLogSegment(segmentTxId, true);
    assert state == State.IN_SEGMENT : "Bad state: " + state;
  }
  
  /**
   * @return true if the log is currently open in write mode, regardless
   * of whether it actually has an open segment.
   */
  synchronized boolean isOpenForWrite() {
    return state == State.IN_SEGMENT ||
      state == State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * @return true if the log is open in write mode and has a segment open
   * ready to take edits.
   */
  synchronized boolean isSegmentOpen() {
    return state == State.IN_SEGMENT;
  }

  /**
   * @return true if the log is open in read mode.
   */
  public synchronized boolean isOpenForRead() {
    return state == State.OPEN_FOR_READING;
  }

  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    if (state == State.CLOSED) {
      LOG.debug("Closing log when already closed");
      return;
    }

    try {
      if (state == State.IN_SEGMENT) {
        assert editLogStream != null;
        waitForSyncToFinish();
        endCurrentLogSegment(true);
      }
    } finally {
      if (journalSet != null && !journalSet.isEmpty()) {
        try {
          synchronized(journalSetLock) {
            journalSet.close();
          }
        } catch (IOException ioe) {
          LOG.warn("Error closing journalSet", ioe);
        }
      }
      state = State.CLOSED;
    }
  }


  /**
   * Format all configured journals which are not file-based.
   * 
   * File-based journals are skipped, since they are formatted by the
   * Storage format code.
   */
  synchronized void formatNonFileJournals(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    
    for (JournalManager jm : journalSet.getJournalManagers()) {
      if (!(jm instanceof FileJournalManager)) {
        jm.format(nsInfo);
      }
    }
  }
  
  synchronized List<FormatConfirmable> getFormatConfirmables() {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);

    List<FormatConfirmable> ret = Lists.newArrayList();
    for (final JournalManager jm : journalSet.getJournalManagers()) {
      // The FJMs are confirmed separately since they are also
      // StorageDirectories
      if (!(jm instanceof FileJournalManager)) {
        ret.add(jm);
      }
    }
    return ret;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  void logEdit(final FSEditLogOp op) {
	// 这块代码，其实就是写edits log的一个主要的流程
	// 用屁股想想，FSEditlog这个组件，这个实例对象，他其实在namenode里全局唯一的
	// 他就是一个核心组件，synchronized(this)，基本上就是保证多线程并发写edits log的时候
	// 一定是同步的
	  
	// 人家凭什么保证transactionId必须是全局唯一递增的？
	// 秘诀就一个，为了避免多线程并发冲突的问题，导致transactionId有问题
	// 直接synchronized重量级锁，把这段代码块给锁住了
	// 也就是多个线程过来，如果要记录操作日志的话，多个线程也许可以并发的记录修改元数据，不光是文件目录树
	// 其实还有很多别的东西，比如说修改一些配置
	// 多个线程要去写edits log的话，在这里就会被卡住，这是重量级同步
	// 整个namenode中，同一时间，只能有一个线程进这段代码，去尝试记录edit log
    synchronized (this) {
      assert isOpenForWrite() :
        "bad state: " + state;
      
      // wait if an automatic sync is scheduled
      waitIfAutoSyncScheduled();
      
      // 这里会有一个transaction机制相关的代码，开启了一个transaction
      // 在这里一定会分配一个唯一的transactionId
      // txid，全局、唯一、递增
      // 在执行这个方法的时候，他在分配transactionId的时候，外面的那个synchronized同步代码块
      // 其实就是已经保证了人家是可以，只有一个线程在这里执行这个方法
      long start = beginTransaction();
      // 此时还是处于synchronized锁定代码块中，所以此时txid++后是不会再次改变的
      // 每个FSEditlogOp就代表了一次元数据的操作，就会有一个全局唯一的txid，transactionId
      // 很重要，你要是不明白这个东西向的话，你都看不懂edits log文件和fsimage文件
      op.setTransactionId(txid);
      
      try {
        // 这边是用EditlogOutputStream在对外输出操作日志
    	// 如果是同时干了两件事情：1、将edits log写入本地磁盘文件；2、将edtis log写入JournalNode
    	// 后面standby namenode会从journalnode来同步edits log
    	// FSEditLog初始化的时候，肯定会初始化好这个EditLogOuputStream，不可能是在其他的地方初始化的啊
    	  
    	// 无论你是将edits log写入磁盘，还是写入网络给journalnode
    	// 在这个步骤里，其实都是将数据写入双缓冲的其中一块区域的
    	  
        editLogStream.write(op);
      } catch (IOException ex) {
        // All journals failed, it is handled in logSync.
      }

      // 结束当前的这个transaction
      endTransaction(start);
      
      // check if it is time to schedule an automatic sync
      if (!shouldForceSync()) {
        return;
      }
      isAutoSyncScheduled = true;
    }
    
    // sync buffered edit log entries to persistent store
    // 在synchronized同步代码块之外，调用了logSync()方法，由他将edits log强制同步到磁盘上去
    // edits log很可能刚开始是写入内存缓冲里的，然后写完内存缓冲之后，可以一次性将内存缓冲中的edits log
    // 都写入到磁盘文件里去，做一次sync同步到磁盘操作
    logSync();
  }

  /**
   * Wait if an automatic sync is scheduled
   */
  synchronized void waitIfAutoSyncScheduled() {
    try {
      while (isAutoSyncScheduled) {
        this.wait(1000);
      }
    } catch (InterruptedException e) {
    }
  }
  
  /**
   * Signal that an automatic sync scheduling is done if it is scheduled
   */
  synchronized void doneWithAutoSyncScheduling() {
    if (isAutoSyncScheduled) {
      isAutoSyncScheduled = false;
      notifyAll();
    }
  }
  
  /**
   * Check if should automatically sync buffered edits to 
   * persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    return editLogStream.shouldForceSync();
  }
  
  private long beginTransaction() {
    // 当前这个线程是否持有了锁，如果没有的话，就不要瞎折腾
    assert Thread.holdsLock(this);
    // get a new transactionId
    
    // 很简单，就是对txid进行一个++，不过我在这里给大家稍微提一点
    // 如果说大家对java并发编程这块稍微有一些了解的话，应该知道，比如txid这种long型的数据
    // 如果要全局唯一的递增， 其实不一定要用synchronized去锁定
    // 其实可以用AtomicLong这个数据类型，他可以保证说全局唯一的递增
    txid++;
    
    // 线程1：txid++，此时txid = 76
    // 线程2：又进来了，txid++，此时txid = 77
    
    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    
    // 将txid = 76放入了自己的ThreadLocal本地变量副本中
    // 线程1，是无所谓的，线程1后面在任何时刻如果要取自己的txid，就是从THreadLocal中获取本地变量副本就可以了
    // 一直看到的就是txid =76
    
    return now();
  }
  
  private void endTransaction(long start) {
    assert Thread.holdsLock(this);
    
    // update statistics
    long end = now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.addTransaction(end-start);
  }

  /**
   * Return the transaction ID of the last transaction written to the log.
   */
  public synchronized long getLastWrittenTxId() {
    return txid;
  }
  
  /**
   * @return the first transaction ID in the current log segment
   */
  synchronized long getCurSegmentTxId() {
    Preconditions.checkState(isSegmentOpen(),
        "Bad state: %s", state);
    return curSegmentTxId;
  }
  
  /**
   * Set the transaction ID to use for the next transaction written.
   */
  synchronized void setNextTxId(long nextTxId) {
    Preconditions.checkArgument(synctxid <= txid &&
       nextTxId >= txid,
       "May not decrease txid." +
      " synctxid=%s txid=%s nextTxId=%s",
      synctxid, txid, nextTxId);
      
    txid = nextTxId - 1;
  }
    
  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *     
   *   - 每个edtis log都会被串行化同步写入一个内存缓冲区，而且都被分配了一个全局唯一递增的transacitonId
   *   - 如果一个线程要执行sync操作，此时会从ThreadLocal获取他的txid，然后根据txid来判断是否要让这个线程来执行flush
   *   - isSyncRunning变量是标志出来flush操作是否正在执行的
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * 每种EditLogOutputStream的流里都有包含这个双缓冲机制，所以说多个线程可以快速的串行写入内存缓冲
   * 同时还可以允许某个线程在执行flush操作到磁盘
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   *   1、synchronized，交换双缓冲区，设置isSyncRunning标志位
   *   2、unsynchronized，刷新内存数据到磁盘上去
   *   3、synchronized，重置isSyncRunning标志位，通知在阻塞的线程
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   * 
   * 第二个步骤，没有用synchronized加锁，是因为要允许在flush数据到磁盘的时候，还可以让其他的线程
   * 继续往内存缓冲里写入这个edits log，这两个步骤可以同步执行，提升写edits log多线程并发的能力，大家思考一下
   * 
   */
  public void logSync() {
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;
    
    boolean sync = false;
    try {
      EditLogOutputStream logStream = null;
      
      // txid = 2的线程，此时也过来了
      // 但是会阻塞在这里，因为这里是synchronized锁的
      
      synchronized (this) {
        try {
          printStatistics(false);

          // if somebody is already syncing, then wait
          // 同一时间只能有一个线程来执行同步内存buffer到磁盘上的这个工作，所以在这里
          // 有一个isSyncRunning标志位，如果是true的话，说明有某个线程正在同步buffer到磁盘上去
          // while true的循环和等待，等前面的那个线程先同步完了，他再来同步
          
          // 三个线程，transactionId分别1、2、3
          // 此时txid = 1的线程，正在执行sync到磁盘的操作
          // 此时txid = 3的线程，获取到了synchronized锁，进入了这个代码块
          // 此时，mytxid = 3 > synctxid = 1，isSyncRunning = true，就会陷入while true等待
          
          // 过了一会儿，txid = 1的线程已经完成了内存的flush，此时txid = 1的edits log已经进入了磁盘的
          // 一个segment文件中去了，isSyncRunning会修改为false
          // 此时txid = 3的线程就会逃离出这个while true，isSyncRunning = false
          
          // 此时txid = 2的线程，获取锁，进入了这里
          // mytxid = 2 > synctxid = 1 & isSyncRunning = true
          // 也会在这里卡住
          // txid = 2的线程，此时就会脱离这个while true循环
          while (mytxid > synctxid && isSyncRunning) {
            try {
              wait(1000);
            } catch (InterruptedException ie) {
            }
          }
          
          //
          // If this transaction was already flushed, then nothing to do
          // mytxid = 3 > synctxid = 1
          // mytxid = 2 < synctxid = 3，此时会进入这个代码片段
          if (mytxid <= synctxid) {
            numTransactionsBatchedInSync++;
            if (metrics != null) {
              // Metrics is non-null only when used inside name node
              metrics.incrTransactionsBatchedInSync();
            }
            // 直接就return掉了，此时就说明这个txid的线程，不需要再flush任何数据了
            return;
          }
     
          // now, this thread will do the sync
          syncStart = txid;
          isSyncRunning = true;
          sync = true;
  
          // swap buffers
          try {
            if (journalSet.isEmpty()) {
              throw new IOException("No journals available to flush");
            }
            // 交换两个buffer缓冲区
            editLogStream.setReadyToFlush();
          } catch (IOException e) {
            final String msg =
                "Could not sync enough journals to persistent storage " +
                "due to " + e.getMessage() + ". " +
                "Unsynced transactions: " + (txid - synctxid);
            LOG.fatal(msg, new Exception());
            synchronized(journalSetLock) {
              IOUtils.cleanup(LOG, journalSet);
            }
            terminate(1, msg);
          }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
        //editLogStream may become null,
        //so store a local variable for flush.
        logStream = editLogStream;
      }
      
      // do the sync
      long start = now();
      try {
        if (logStream != null) {
          // 在这里，人家在logSync()方法中，就调用了EditLogOutputStream.flush()方法
          // 他底层会调用所有流的flush()方法
          // 将之前写入缓冲区的数据，此时刷新到磁盘或者网络（journalnode）
          
          // 这个代码块是不加锁的，txid = 1的线程，正在这里执行sync操作
          // 将内存的数据flush到磁盘上去
        	
          // 此时txid = 3的线程，会同时将txid = 2和txid = 3的两个edits log，都在这里
          // flush到磁盘里去
        	
          // txid = 3的线程，将txid=2和txid=3的两个edits log都成功flush到磁盘里去了
        
          // 如果在这里加synchronized会如何呢？
          // 此时就是每个线程，从写内存buffer，一直到flush到磁盘，都是串行化的
          // 多线程并发写edits log的能力很弱
        	
          logStream.flush();
        }
      } catch (IOException ex) {
        synchronized (this) {
          final String msg =
              "Could not sync enough journals to persistent storage. "
              + "Unsynced transactions: " + (txid - synctxid);
          LOG.fatal(msg, new Exception());
          synchronized(journalSetLock) {
            IOUtils.cleanup(LOG, journalSet);
          }
          terminate(1, msg);
        }
      }
      long elapsed = now() - start;
  
      if (metrics != null) { // Metrics non-null only when used inside name node
        metrics.addSync(elapsed);
      }
      
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 
      synchronized (this) {
        if (sync) {
          // synctxid = 3
          synctxid = syncStart;
          // isSyncRunning = false
          isSyncRunning = false;
        }
        this.notifyAll();
     }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editLogStream.getNumSync());
    buf.append(" SyncTimes(ms): ");
    buf.append(journalSet.getSyncTimes());
    LOG.info(buf);
  }

  /** Record the RPC IDs if necessary */
  private void logRpcIds(FSEditLogOp op, boolean toLogRpcIds) {
    if (toLogRpcIds) {
      op.setRpcClientId(Server.getClientId());
      op.setRpcCallId(Server.getCallId());
    }
  }
  
  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFile newNode, boolean overwrite,
      boolean toLogRpcIds) {
    Preconditions.checkArgument(newNode.isUnderConstruction());
    PermissionStatus permissions = newNode.getPermissionStatus();
    AddOp op = AddOp.getInstance(cache.get())
      .reset()
      .setInodeId(newNode.getId())
      .setPath(path)
      .setReplication(newNode.getFileReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(permissions)
      .setClientName(newNode.getFileUnderConstructionFeature().getClientName())
      .setClientMachine(
          newNode.getFileUnderConstructionFeature().getClientMachine())
      .setOverwrite(overwrite)
      .setStoragePolicyId(newNode.getLocalStoragePolicyID());

    AclFeature f = newNode.getAclFeature();
    if (f != null) {
      op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
    }

    XAttrFeature x = newNode.getXAttrFeature();
    if (x != null) {
      op.setXAttrs(x.getXAttrs());
    }

    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    CloseOp op = CloseOp.getInstance(cache.get())
      .setPath(path)
      .setReplication(newNode.getFileReplication())
      .setModificationTime(newNode.getModificationTime())
      .setAccessTime(newNode.getAccessTime())
      .setBlockSize(newNode.getPreferredBlockSize())
      .setBlocks(newNode.getBlocks())
      .setPermissionStatus(newNode.getPermissionStatus());
    
    logEdit(op);
  }
  
  public void logAddBlock(String path, INodeFile file) {
    Preconditions.checkArgument(file.isUnderConstruction());
    BlockInfo[] blocks = file.getBlocks();
    Preconditions.checkState(blocks != null && blocks.length > 0);
    BlockInfo pBlock = blocks.length > 1 ? blocks[blocks.length - 2] : null;
    BlockInfo lastBlock = blocks[blocks.length - 1];
    AddBlockOp op = AddBlockOp.getInstance(cache.get()).setPath(path)
        .setPenultimateBlock(pBlock).setLastBlock(lastBlock);
    logEdit(op);
  }
  
  public void logUpdateBlocks(String path, INodeFile file, boolean toLogRpcIds) {
    Preconditions.checkArgument(file.isUnderConstruction());
    UpdateBlocksOp op = UpdateBlocksOp.getInstance(cache.get())
      .setPath(path)
      .setBlocks(file.getBlocks());
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    PermissionStatus permissions = newNode.getPermissionStatus();
    
    // 这段代码，其实很明显的，是用的构造器的模式
    // 在构造了一个代表创建目录的一个日志对象，MkdirOp
    MkdirOp op = MkdirOp.getInstance(cache.get())
      .reset()
      .setInodeId(newNode.getId())
      .setPath(path)
      .setTimestamp(newNode.getModificationTime())
      .setPermissionStatus(permissions);

    AclFeature f = newNode.getAclFeature();
    if (f != null) {
      op.setAclEntries(AclStorage.readINodeLogicalAcl(newNode));
    }

    XAttrFeature x = newNode.getXAttrFeature();
    if (x != null) {
      op.setXAttrs(x.getXAttrs());
    }
    
    // 构造完了以后，就会在这里logEdit(op)，实际完成日志的一个记录
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds) {
    RenameOldOp op = RenameOldOp.getInstance(cache.get())
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds,
      Options.Rename... options) {
    RenameOp op = RenameOp.getInstance(cache.get())
      .setSource(src)
      .setDestination(dst)
      .setTimestamp(timestamp)
      .setOptions(options);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    SetReplicationOp op = SetReplicationOp.getInstance(cache.get())
      .setPath(src)
      .setReplication(replication);
    logEdit(op);
  }

  /** 
   * Add set storage policy id record to edit log
   */
  void logSetStoragePolicy(String src, byte policyId) {
    SetStoragePolicyOp op = SetStoragePolicyOp.getInstance(cache.get())
        .setPath(src).setPolicyId(policyId);
    logEdit(op);
  }

  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param nsQuota namespace quota
   * @param dsQuota diskspace quota
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    SetQuotaOp op = SetQuotaOp.getInstance(cache.get())
      .setSource(src)
      .setNSQuota(nsQuota)
      .setDSQuota(dsQuota);
    logEdit(op);
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    SetPermissionsOp op = SetPermissionsOp.getInstance(cache.get())
      .setSource(src)
      .setPermissions(permissions);
    logEdit(op);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    SetOwnerOp op = SetOwnerOp.getInstance(cache.get())
      .setSource(src)
      .setUser(username)
      .setGroup(groupname);
    logEdit(op);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String[] srcs, long timestamp, boolean toLogRpcIds) {
    ConcatDeleteOp op = ConcatDeleteOp.getInstance(cache.get())
      .setTarget(trg)
      .setSources(srcs)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp, boolean toLogRpcIds) {
    DeleteOp op = DeleteOp.getInstance(cache.get())
      .setPath(src)
      .setTimestamp(timestamp);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  /**
   * Add legacy block generation stamp record to edit log
   */
  void logGenerationStampV1(long genstamp) {
    SetGenstampV1Op op = SetGenstampV1Op.getInstance(cache.get())
        .setGenerationStamp(genstamp);
    logEdit(op);
  }

  /**
   * Add generation stamp record to edit log
   */
  void logGenerationStampV2(long genstamp) {
    SetGenstampV2Op op = SetGenstampV2Op.getInstance(cache.get())
        .setGenerationStamp(genstamp);
    logEdit(op);
  }

  /**
   * Record a newly allocated block ID in the edit log
   */
  void logAllocateBlockId(long blockId) {
    AllocateBlockIdOp op = AllocateBlockIdOp.getInstance(cache.get())
      .setBlockId(blockId);
    logEdit(op);
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    TimesOp op = TimesOp.getInstance(cache.get())
      .setPath(src)
      .setModificationTime(mtime)
      .setAccessTime(atime);
    logEdit(op);
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, long atime,
      INodeSymlink node, boolean toLogRpcIds) {
    SymlinkOp op = SymlinkOp.getInstance(cache.get())
      .setId(node.getId())
      .setPath(path)
      .setValue(value)
      .setModificationTime(mtime)
      .setAccessTime(atime)
      .setPermissionStatus(node.getPermissionStatus());
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    GetDelegationTokenOp op = GetDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    RenewDelegationTokenOp op = RenewDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id)
      .setExpiryTime(expiryTime);
    logEdit(op);
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    CancelDelegationTokenOp op = CancelDelegationTokenOp.getInstance(cache.get())
      .setDelegationTokenIdentifier(id);
    logEdit(op);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    UpdateMasterKeyOp op = UpdateMasterKeyOp.getInstance(cache.get())
      .setDelegationKey(key);
    logEdit(op);
  }

  void logReassignLease(String leaseHolder, String src, String newHolder) {
    ReassignLeaseOp op = ReassignLeaseOp.getInstance(cache.get())
      .setLeaseHolder(leaseHolder)
      .setPath(src)
      .setNewHolder(newHolder);
    logEdit(op);
  }
  
  void logCreateSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
    CreateSnapshotOp op = CreateSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logDeleteSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
    DeleteSnapshotOp op = DeleteSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logRenameSnapshot(String path, String snapOldName, String snapNewName,
      boolean toLogRpcIds) {
    RenameSnapshotOp op = RenameSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path).setSnapshotOldName(snapOldName)
        .setSnapshotNewName(snapNewName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logAllowSnapshot(String path) {
    AllowSnapshotOp op = AllowSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path);
    logEdit(op);
  }

  void logDisallowSnapshot(String path) {
    DisallowSnapshotOp op = DisallowSnapshotOp.getInstance(cache.get())
        .setSnapshotRoot(path);
    logEdit(op);
  }

  /**
   * Log a CacheDirectiveInfo returned from
   * {@link CacheManager#addDirective(CacheDirectiveInfo, FSPermissionChecker)}
   */
  void logAddCacheDirectiveInfo(CacheDirectiveInfo directive,
      boolean toLogRpcIds) {
    AddCacheDirectiveInfoOp op =
        AddCacheDirectiveInfoOp.getInstance(cache.get())
            .setDirective(directive);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logModifyCacheDirectiveInfo(
      CacheDirectiveInfo directive, boolean toLogRpcIds) {
    ModifyCacheDirectiveInfoOp op =
        ModifyCacheDirectiveInfoOp.getInstance(
            cache.get()).setDirective(directive);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logRemoveCacheDirectiveInfo(Long id, boolean toLogRpcIds) {
    RemoveCacheDirectiveInfoOp op =
        RemoveCacheDirectiveInfoOp.getInstance(cache.get()).setId(id);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logAddCachePool(CachePoolInfo pool, boolean toLogRpcIds) {
    AddCachePoolOp op =
        AddCachePoolOp.getInstance(cache.get()).setPool(pool);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logModifyCachePool(CachePoolInfo info, boolean toLogRpcIds) {
    ModifyCachePoolOp op =
        ModifyCachePoolOp.getInstance(cache.get()).setInfo(info);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logRemoveCachePool(String poolName, boolean toLogRpcIds) {
    RemoveCachePoolOp op =
        RemoveCachePoolOp.getInstance(cache.get()).setPoolName(poolName);
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  void logStartRollingUpgrade(long startTime) {
    RollingUpgradeOp op = RollingUpgradeOp.getStartInstance(cache.get());
    op.setTime(startTime);
    logEdit(op);
  }

  void logFinalizeRollingUpgrade(long finalizeTime) {
    RollingUpgradeOp op = RollingUpgradeOp.getFinalizeInstance(cache.get());
    op.setTime(finalizeTime);
    logEdit(op);
  }

  void logSetAcl(String src, List<AclEntry> entries) {
    SetAclOp op = SetAclOp.getInstance();
    op.src = src;
    op.aclEntries = entries;
    logEdit(op);
  }
  
  void logSetXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
    final SetXAttrOp op = SetXAttrOp.getInstance();
    op.src = src;
    op.xAttrs = xAttrs;
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }
  
  void logRemoveXAttrs(String src, List<XAttr> xAttrs, boolean toLogRpcIds) {
    final RemoveXAttrOp op = RemoveXAttrOp.getInstance();
    op.src = src;
    op.xAttrs = xAttrs;
    logRpcIds(op, toLogRpcIds);
    logEdit(op);
  }

  /**
   * Get all the journals this edit log is currently operating on.
   */
  synchronized List<JournalAndStream> getJournals() {
    return journalSet.getAllJournalStreams();
  }
  
  /**
   * Used only by tests.
   */
  @VisibleForTesting
  synchronized public JournalSet getJournalSet() {
    return journalSet;
  }
  
  @VisibleForTesting
  synchronized void setJournalSetForTesting(JournalSet js) {
    this.journalSet = js;
  }
  
  /**
   * Used only by tests.
   */
  @VisibleForTesting
  void setMetricsForTests(NameNodeMetrics metrics) {
    this.metrics = metrics;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    return journalSet.getEditLogManifest(fromTxId);
  }
 
  /**
   * Finalizes the current edit log and opens a new log segment.
   * @return the transaction id of the BEGIN_LOG_SEGMENT transaction
   * in the new log.
   */
  synchronized long rollEditLog() throws IOException {
    LOG.info("Rolling edit logs");
    endCurrentLogSegment(true);
    
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId, true);
    
    assert curSegmentTxId == nextTxId;
    return nextTxId;
  }
  
  /**
   * Start writing to the log segment with the given txid.
   * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state. 
   * 
   * 开启一个新的log segment，日志段，edits log是分段存储的，基于txid分段存储的
   * 
   * 不管怎么说，在namenode启动的时候，人家加载磁盘上的fsimage和edits合并之后，会写fsimage到磁盘上去
   * 同时人家还会打开一个新的edits log文件供后续来写
   * 在这里应该就会调用到FSEditLog的startLogSegment()方法里来，在这里会初始化EditLogOutputStream
   * 
   */
  synchronized void startLogSegment(final long segmentTxId,
      boolean writeHeaderTxn) throws IOException {
    LOG.info("Starting log segment at " + segmentTxId);
    Preconditions.checkArgument(segmentTxId > 0,
        "Bad txid: %s", segmentTxId);
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    Preconditions.checkState(segmentTxId > curSegmentTxId,
        "Cannot start writing to log segment " + segmentTxId +
        " when previous log segment started at " + curSegmentTxId);
    Preconditions.checkArgument(segmentTxId == txid + 1,
        "Cannot start log segment at txid %s when next expected " +
        "txid is %s", segmentTxId, txid + 1);
    
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    // TODO no need to link this back to storage anymore!
    // See HDFS-2174.
    storage.attemptRestoreRemovedStorage();
    
    try {
      // 在这里调用journalSet初始化了一个新的EditLogOutputStream
      // 这个OutputStream在write()时候，就会依次调用底层封装的FileJournalManager写入磁盘
      // 同时也会调用QuorumJournalManager写入到JournalManager里去
      editLogStream = journalSet.startLogSegment(segmentTxId,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    } catch (IOException ex) {
      throw new IOException("Unable to start log segment " +
          segmentTxId + ": too few journals successfully started.", ex);
    }
    
    curSegmentTxId = segmentTxId;
    state = State.IN_SEGMENT;

    if (writeHeaderTxn) {
      logEdit(LogSegmentOp.getInstance(cache.get(),
          FSEditLogOpCodes.OP_START_LOG_SEGMENT));
      logSync();
    }
  }

  /**
   * Finalize the current log segment.
   * Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
   */
  public synchronized void endCurrentLogSegment(boolean writeEndTxn) {
    LOG.info("Ending log segment " + curSegmentTxId);
    Preconditions.checkState(isSegmentOpen(),
        "Bad state: %s", state);
    
    if (writeEndTxn) {
      logEdit(LogSegmentOp.getInstance(cache.get(), 
          FSEditLogOpCodes.OP_END_LOG_SEGMENT));
      logSync();
    }

    printStatistics(true);
    
    final long lastTxId = getLastWrittenTxId();
    
    try {
      journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
      editLogStream = null;
    } catch (IOException e) {
      //All journals have failed, it will be handled in logSync.
    }
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * Abort all current logs. Called from the backup node.
   */
  synchronized void abortCurrentLogSegment() {
    try {
      //Check for null, as abort can be called any time.
      if (editLogStream != null) {
        editLogStream.abort();
        editLogStream = null;
        state = State.BETWEEN_LOG_SEGMENTS;
      }
    } catch (IOException e) {
      LOG.warn("All journals failed to abort", e);
    }
  }

  /**
   * Archive any log files that are older than the given txid.
   * 
   * If the edit log is not open for write, then this call returns with no
   * effect.
   */
  @Override
  public synchronized void purgeLogsOlderThan(final long minTxIdToKeep) {
    // Should not purge logs unless they are open for write.
    // This prevents the SBN from purging logs on shared storage, for example.
    if (!isOpenForWrite()) {
      return;
    }
    
    assert curSegmentTxId == HdfsConstants.INVALID_TXID || // on format this is no-op
      minTxIdToKeep <= curSegmentTxId :
      "cannot purge logs older than txid " + minTxIdToKeep +
      " when current segment starts at " + curSegmentTxId;
    if (minTxIdToKeep == 0) {
      return;
    }
    
    // This could be improved to not need synchronization. But currently,
    // journalSet is not threadsafe, so we need to synchronize this method.
    try {
      journalSet.purgeLogsOlderThan(minTxIdToKeep);
    } catch (IOException ex) {
      //All journals have failed, it will be handled in logSync.
    }
  }

  
  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Return the txid of the last synced transaction.
   */
  public synchronized long getSyncTxId() {
    return synctxid;
  }


  // sets the initial capacity of the flush buffer.
  synchronized void setOutputBufferCapacity(int size) {
    journalSet.setOutputBufferCapacity(size);
  }

  /**
   * Create (or find if already exists) an edit output stream, which
   * streams journal records (edits) to the specified backup node.<br>
   * 
   * The new BackupNode will start receiving edits the next time this
   * NameNode's logs roll.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void registerBackupNode(
      NamenodeRegistration bnReg, // backup node
      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    
    JournalManager jas = findBackupJournal(bnReg);
    if (jas != null) {
      // already registered
      LOG.info("Backup node " + bnReg + " re-registers");
      return;
    }
    
    LOG.info("Registering new backup node: " + bnReg);
    BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
    synchronized(journalSetLock) {
      journalSet.add(bjm, false);
    }
  }
  
  synchronized void releaseBackupStream(NamenodeRegistration registration)
      throws IOException {
    BackupJournalManager bjm = this.findBackupJournal(registration);
    if (bjm != null) {
      LOG.info("Removing backup journal " + bjm);
      synchronized(journalSetLock) {
        journalSet.remove(bjm);
      }
    }
  }
  
  /**
   * Find the JournalAndStream associated with this BackupNode.
   * 
   * @return null if it cannot be found
   */
  private synchronized BackupJournalManager findBackupJournal(
      NamenodeRegistration bnReg) {
    for (JournalManager bjm : journalSet.getJournalManagers()) {
      if ((bjm instanceof BackupJournalManager)
          && ((BackupJournalManager) bjm).matchesRegistration(bnReg)) {
        return (BackupJournalManager) bjm;
      }
    }
    return null;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */   
  synchronized void logEdit(final int length, final byte[] data) {
    long start = beginTransaction();

    try {
      editLogStream.writeRaw(data, 0, length);
    } catch (IOException ex) {
      // All journals have failed, it will be handled in logSync.
    }
    endTransaction(start);
  }

  /**
   * Run recovery on all journals to recover any unclosed segments
   */
  synchronized void recoverUnclosedStreams() {
    Preconditions.checkState(
        state == State.BETWEEN_LOG_SEGMENTS,
        "May not recover segments - wrong state: %s", state);
    try {
      journalSet.recoverUnfinalizedSegments();
    } catch (IOException ex) {
      // All journals have failed, it is handled in logSync.
      // TODO: are we sure this is OK?
    }
  }

  public long getSharedLogCTime() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        return jas.getManager().getJournalCTime();
      }
    }
    throw new IOException("No shared log found.");
  }

  public synchronized void doPreUpgradeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doPreUpgrade();
      }
    }
  }

  public synchronized void doUpgradeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doUpgrade(storage);
      }
    }
  }

  public synchronized void doFinalizeOfSharedLog() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doFinalize();
      }
    }
  }

  public synchronized boolean canRollBackSharedLog(StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        return jas.getManager().canRollBack(storage, prevStorage,
            targetLayoutVersion);
      }
    }
    throw new IOException("No shared log found.");
  }

  public synchronized void doRollback() throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      if (jas.isShared()) {
        jas.getManager().doRollback();
      }
    }
  }

  public synchronized void discardSegments(long markerTxid)
      throws IOException {
    for (JournalAndStream jas : journalSet.getAllJournalStreams()) {
      jas.getManager().discardSegments(markerTxid);
    }
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk) throws IOException {
    journalSet.selectInputStreams(streams, fromTxId, inProgressOk);
  }

  public Collection<EditLogInputStream> selectInputStreams(
      long fromTxId, long toAtLeastTxId) throws IOException {
    return selectInputStreams(fromTxId, toAtLeastTxId, null, true);
  }
  
  /**
   * Select a list of input streams.
   * 
   * @param fromTxId first transaction in the selected streams
   * @param toAtLeastTxId the selected streams must contain this transaction
   * @param recovery recovery context
   * @param inProgressOk set to true if in-progress streams are OK
   */
  public Collection<EditLogInputStream> selectInputStreams(
      long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
      boolean inProgressOk) throws IOException {

    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    synchronized(journalSetLock) {
      Preconditions.checkState(journalSet.isOpen(), "Cannot call " +
          "selectInputStreams() on closed FSEditLog");
      selectInputStreams(streams, fromTxId, inProgressOk);
    }

    try {
      checkForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk);
    } catch (IOException e) {
      if (recovery != null) {
        // If recovery mode is enabled, continue loading even if we know we
        // can't load up to toAtLeastTxId.
        LOG.error(e);
      } else {
        closeAllStreams(streams);
        throw e;
      }
    }
    return streams;
  }
  
  /**
   * Check for gaps in the edit log input stream list.
   * Note: we're assuming that the list is sorted and that txid ranges don't
   * overlap.  This could be done better and with more generality with an
   * interval tree.
   */
  private void checkForGaps(List<EditLogInputStream> streams, long fromTxId,
      long toAtLeastTxId, boolean inProgressOk) throws IOException {
    Iterator<EditLogInputStream> iter = streams.iterator();
    long txId = fromTxId;
    while (true) {
      if (txId > toAtLeastTxId) return;
      if (!iter.hasNext()) break;
      EditLogInputStream elis = iter.next();
      if (elis.getFirstTxId() > txId) break;
      long next = elis.getLastTxId();
      if (next == HdfsConstants.INVALID_TXID) {
        if (!inProgressOk) {
          throw new RuntimeException("inProgressOk = false, but " +
              "selectInputStreams returned an in-progress edit " +
              "log input stream (" + elis + ")");
        }
        // We don't know where the in-progress stream ends.
        // It could certainly go all the way up to toAtLeastTxId.
        return;
      }
      txId = next + 1;
    }
    throw new IOException(String.format("Gap in transactions. Expected to "
        + "be able to read up until at least txid %d but unable to find any "
        + "edit logs containing txid %d", toAtLeastTxId, txId));
  }

  /** 
   * Close all the streams in a collection
   * @param streams The list of streams to close
   */
  static void closeAllStreams(Iterable<EditLogInputStream> streams) {
    for (EditLogInputStream s : streams) {
      IOUtils.closeStream(s);
    }
  }

  /**
   * Retrieve the implementation class for a Journal scheme.
   * @param conf The configuration to retrieve the information from
   * @param uriScheme The uri scheme to look up.
   * @return the class of the journal implementation
   * @throws IllegalArgumentException if no class is configured for uri
   */
  static Class<? extends JournalManager> getJournalClass(Configuration conf,
                               String uriScheme) {
    String key
      = DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + "." + uriScheme;
    Class <? extends JournalManager> clazz = null;
    try {
      clazz = conf.getClass(key, null, JournalManager.class);
    } catch (RuntimeException re) {
      throw new IllegalArgumentException(
          "Invalid class specified for " + uriScheme, re);
    }
      
    if (clazz == null) {
      LOG.warn("No class configured for " +uriScheme
               + ", " + key + " is empty");
      throw new IllegalArgumentException(
          "No class configured for " + uriScheme);
    }
    return clazz;
  }

  /**
   * Construct a custom journal manager.
   * The class to construct is taken from the configuration.
   * @param uri Uri to construct
   * @return The constructed journal manager
   * @throws IllegalArgumentException if no class is configured for uri
   */
  private JournalManager createJournal(URI uri) {
    Class<? extends JournalManager> clazz
      = getJournalClass(conf, uri.getScheme());

    try {
      Constructor<? extends JournalManager> cons
        = clazz.getConstructor(Configuration.class, URI.class,
            NamespaceInfo.class);
      return cons.newInstance(conf, uri, storage.getNamespaceInfo());
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to construct journal, "
                                         + uri, e);
    }
  }

}
