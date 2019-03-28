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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;

import static org.apache.hadoop.util.Time.now;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 * 
 * EditLogTailer代表了一个后台线程，这个后台线程会不断的周期性的从journal nodes集群上拉取edits log数据流
 * 接着就将edits log数据流给应用到自己的FSNamesystem上去，也就是自己本地的元数据上去
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Log LOG = LogFactory.getLog(EditLogTailer.class);
  
  // 这个东西一看就是人家内部的线程
  private final EditLogTailerThread tailerThread;
  
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private FSEditLog editLog;

  private InetSocketAddress activeAddr;
  private NamenodeProtocol cachedActiveProxy = null;

  /**
   * The last transaction ID at which an edit log roll was initiated.
   */
  private long lastRollTriggerTxId = HdfsConstants.INVALID_TXID;
  
  /**
   * The highest transaction ID loaded by the Standby.
   */
  private long lastLoadedTxnId = HdfsConstants.INVALID_TXID;

  /**
   * The last time we successfully loaded a non-zero number of edits from the
   * shared directory.
   */
  private long lastLoadTimestamp;

  /**
   * How often the Standby should roll edit logs. Since the Standby only reads
   * from finalized log segments, the Standby will only be as up-to-date as how
   * often the logs are rolled.
   */
  private final long logRollPeriodMs;

  /**
   * How often the Standby should check if there are new finalized segment(s)
   * available to be read from.
   */
  private final long sleepTimeMs;
  
  public EditLogTailer(FSNamesystem namesystem, Configuration conf) {
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;
    this.namesystem = namesystem;
    this.editLog = namesystem.getEditLog();
    
    lastLoadTimestamp = now();

    logRollPeriodMs = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT) * 1000;
    if (logRollPeriodMs >= 0) {
      this.activeAddr = getActiveNodeAddress();
      Preconditions.checkArgument(activeAddr.getPort() > 0,
          "Active NameNode must have an IPC port configured. " +
          "Got address '%s'", activeAddr);
      LOG.info("Will roll logs on active node at " + activeAddr + " every " +
          (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
          DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }
    
    sleepTimeMs = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT) * 1000;
    
    LOG.debug("logRollPeriodMs=" + logRollPeriodMs +
        " sleepTime=" + sleepTimeMs);
  }
  
  private InetSocketAddress getActiveNodeAddress() {
    Configuration activeConf = HAUtil.getConfForOtherNode(conf);
    return NameNode.getServiceAddress(activeConf, true);
  }
  
  private NamenodeProtocol getActiveNodeProxy() throws IOException {
    if (cachedActiveProxy == null) {
      int rpcTimeout = conf.getInt(
          DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_KEY,
          DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT);
      NamenodeProtocolPB proxy = RPC.waitForProxy(NamenodeProtocolPB.class,
          RPC.getProtocolVersion(NamenodeProtocolPB.class), activeAddr, conf,
          rpcTimeout, Long.MAX_VALUE);
      cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
    }
    assert cachedActiveProxy != null;
    return cachedActiveProxy;
  }

  public void start() {
    tailerThread.start();
  }
  
  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  public void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }
  
  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    // Important to do tailing as the login user, in case the shared
    // edits storage is implemented by a JournalManager that depends
    // on security credentials to access the logs (eg QuorumJournalManager).
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          doTailEdits();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return null;
      }
    });
  }
  
  @VisibleForTesting
  void doTailEdits() throws IOException, InterruptedException {
    // Write lock needs to be interruptible here because the 
    // transitionToActive RPC takes the write lock before calling
    // tailer.stop() -- so if we're not interruptible, it will
    // deadlock.
    namesystem.writeLockInterruptibly();
    try {
      FSImage image = namesystem.getFSImage();

      // 都能看懂了，lastTxnId，其实说的就是最近一个应用到standby namenode本地
      // 元数据上的edits log的txid
      // 比如说，最近一次的txid是171，应用到了本地的元数据中
      long lastTxnId = image.getLastAppliedTxId();
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("lastTxnId: " + lastTxnId);
      }
      Collection<EditLogInputStream> streams;
      try {
    	// selectInputStreams，这里他的意思是说，希望尝试去读取
    	// lastTxnId + 1 = 172，意思说，他希望从journal node上获取txid = 172开始的edits log
        streams = editLog.selectInputStreams(lastTxnId + 1, 0, null, false);
      } catch (IOException ioe) {
        // This is acceptable. If we try to tail edits in the middle of an edits
        // log roll, i.e. the last one has been finalized but the new inprogress
        // edits file hasn't been started yet.
        LOG.warn("Edits tailer failed to find any streams. Will try again " +
            "later.", ioe);
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("edit streams to load from: " + streams.size());
      }
      
      // Once we have streams to load, errors encountered are legitimate cause
      // for concern, so we don't catch them here. Simple errors reading from
      // disk are ignored.
      long editsLoaded = 0;
      try {
    	// 执行FSImage的loadEdits()方法，将edits log输入流传入了进去
    	// FSImage代表了人家的这个元数据
    	// 他肯定是说，底层使用了EditLogInputStream从journalnodes读尝试拉取更新的edits log
    	// 如果拉取到了，肯定在这个方法的内部，就会应用到元数据里去
        editsLoaded = image.loadEdits(streams, namesystem);
      } catch (EditLogInputException elie) {
        editsLoaded = elie.getNumEditsLoaded();
        throw elie;
      } finally {
        if (editsLoaded > 0 || LOG.isDebugEnabled()) {
          LOG.info(String.format("Loaded %d edits starting from txid %d ",
              editsLoaded, lastTxnId));
        }
      }

      if (editsLoaded > 0) {
        lastLoadTimestamp = now();
      }
      // 再次更新一下最近一次加载到的lastLoadedTxnId
      lastLoadedTxnId = image.getLastAppliedTxId();
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * @return timestamp (in msec) of when we last loaded a non-zero number of edits.
   */
  public long getLastLoadTimestamp() {
    return lastLoadTimestamp;
  }

  /**
   * @return true if the configured log roll period has elapsed.
   */
  private boolean tooLongSinceLastLoad() {
    return logRollPeriodMs >= 0 && 
      (now() - lastLoadTimestamp) > logRollPeriodMs ;
  }

  /**
   * Trigger the active node to roll its logs.
   */
  private void triggerActiveLogRoll() {
    LOG.info("Triggering log roll on remote NameNode " + activeAddr);
    try {
      getActiveNodeProxy().rollEditLog();
      lastRollTriggerTxId = lastLoadedTxnId;
    } catch (IOException ioe) {
      LOG.warn("Unable to trigger a roll of the active NN", ioe);
    }
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;
    
    private EditLogTailerThread() {
      super("Edit log tailer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    @Override
    public void run() {
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            doWork();
            return null;
          }
        });
    }
    
    private void doWork() {
      // 进入了一个while true死循环，不断的运行
      while (shouldRun) {
        try {
          // There's no point in triggering a log roll if the Standby hasn't
          // read any more transactions since the last time a roll was
          // triggered. 
        	
          // 看起来这个好像是说，如果距离上一次roll edits log以后，standby一直没有接收到更多的edits log
          // 触发一次active namenode的edits log roll
          // edits log roll是个什么操作？
          // 大概可以理解为，重新创建一个新的edits_inprogress文件
          // 之前的文件就固定为startTransactionId~endTransactionId的一个文件
          if (tooLongSinceLastLoad() &&
              lastRollTriggerTxId < lastLoadedTxnId) {
            triggerActiveLogRoll();
          }
          /**
           * Check again in case someone calls {@link EditLogTailer#stop} while
           * we're triggering an edit log roll, since ipc.Client catches and
           * ignores {@link InterruptedException} in a few places. This fixes
           * the bug described in HDFS-2823.
           */
          if (!shouldRun) {
            break;
          }
          doTailEdits();
        } catch (EditLogInputException elie) {
          LOG.warn("Error while reading edits from disk. Will try again.", elie);
        } catch (InterruptedException ie) {
          // interrupter should have already set shouldRun to false
          continue;
        } catch (Throwable t) {
          LOG.fatal("Unknown error encountered while tailing edits. " +
              "Shutting down standby NN.", t);
          terminate(1, t);
        }

        try {
          // 按照默认的配置来说
          // 这里是standby namenode默认是每隔60秒（1分钟）去尝试获取journal node上的edits log
          // 这个时间很重要
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted", e);
        }
      }
    }
  }

}
