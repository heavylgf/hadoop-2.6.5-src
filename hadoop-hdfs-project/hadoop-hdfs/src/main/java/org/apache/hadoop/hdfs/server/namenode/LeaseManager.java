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

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 *
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
@InterfaceAudience.Private
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;

  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  // TreeMap
  // 如果大家去关注一下我之前讲解的那个JDK源码剖析系列的课程
  // 里面就有集合包的源码剖析
  // 里面是提到了这个TreeMap的，红黑树保存的集合数据结构，特点就是说放入里面的数据
  // 可以默认自动根据key的自然大小来排序
  // 排序好了以后，下次你如果遍历TreeMap的时候，就是根据key自然大小排序后的顺序来遍历的

  // HashMap遍历的时候是无序的
  // LinkedHashMap是用一个双向链表保存了插入map的key-value对的顺序
  // 遍历LinkedHashMap的时候是根据插入的顺序来遍历的
  // TreeMap是默认根据key的自然大小来排序的，遍历的时候也是按照这个顺序来遍历的

  // 用TreeMap保存了一个lease数据集合
  // 用屁股想想都知道，Lease一看就是契约，是LeaseManager管理的核心数据
  // 一个Lease就是代表了某个客户端对一个文件拥有的契约
  // leases数据结构里，key是客户端名称，value就是那个客户端的lease契约
  // TreeMap的话，默认会根据客户端的名称来进行排序
  private final SortedMap<String, Lease> leases = new TreeMap<String, Lease>();
  // Set of: Lease
  // TreeSet，如果看过我的那个JDK集合源码剖析的课程
  // Set系列的东西是没有源码的，他全部都是基于Map系列的类来实现的
  // TreeSet底层就是基于TreeMap来实现的，默认里面的元素是按照自然大小排序的
  // 普通的HashSet是无序的
  // 相当于默认就是根据Lease来自然排序了
  // 默认是根据lease的lastUpdate续约时间来排序，如果一样，就根据客户端名称来排序
  private final NavigableSet<Lease> sortedLeases = new TreeSet<Lease>();

  //
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  // TreeMap，根据path来排序的契约数据
  // key是文件路径名，默认是根据文件路径名来进行排序的
  private final SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();

  // 核心：Daemon，本身就是基于他创建的线程，都是daemon后台线程
  private Daemon lmthread;

  // 有一个东西，是叫做shouldRunMonitor，标志位
  // volatile修饰的标志位，volatilie是什么东西？请参加我的java并发编程课程
  // 我会详细的剖析volatile和java内存模型的原理
  private volatile boolean shouldRunMonitor;

  LeaseManager(FSNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}

  Lease getLease(String holder) {
    return leases.get(holder);
  }

  @VisibleForTesting
  int getNumSortedLeases() {return sortedLeases.size();}

  /**
   * This method iterates through all the leases and counts the number of blocks
   * which are not COMPLETE. The FSNamesystem read lock MUST be held before
   * calling this method.
   *
   * 在namenode启动的时候，会进行一个safe mode的检查
   * 会调用这个方法
   *
   * @return
   */
  synchronized long getNumUnderConstructionBlocks() {
    assert this.fsnamesystem.hasReadLock() : "The FSNamesystem read lock wasn't"
            + "acquired before counting under construction blocks";
    long numUCBlocks = 0;
    // 这里的话呢就是在遍历所有的lease，Lease契约的东西，这个东西得在后面来讲解
    // 每个lease里面有多个path，你可以认为一个path就是代表了一个文件的路径，/usr/warehosue/hive/access.log
    for (Lease lease : sortedLeases) {
      for (String path : lease.getPaths()) {
        final INodeFile cons;
        try {
          // 会通过FSNamesystem里面有一个关键的东东，FSDirectory，getINode(path).asFile()
          // 就可以获取到代表了/usr/warehosue/hive/access.log这个东西的一个文件对象，INodeFile
          cons = this.fsnamesystem.getFSDirectory().getINode(path).asFile();
          // 在这里他会判断一下，这个文件当前是否处于under construction状态，如果不是处于这个状态，那么就直接看下一个path
          if (!cons.isUnderConstruction()) {
            LOG.warn("The file " + cons.getFullPathName()
                    + " is not under construction but has lease.");
            continue;
          }
        } catch (UnresolvedLinkException e) {
          throw new AssertionError("Lease files should reside on this FS");
        }

        // 这边是通过INodeFile.getBlocks()方法
        // 获取/usr/warehosue/hive/access.log文件的blocks
        // 每个文件都可以对应多个block，可以拆分为128m一个的block，所以这里一个文件可能有多个block
        BlockInfo[] blocks = cons.getBlocks();
        if(blocks == null)
          continue;
        // 遍历这个文件的block
        for(BlockInfo b : blocks) {
          // 如果这个block的状态不是complete，那么这个block就属于一个under construction的block
          if(!b.isComplete())
            numUCBlocks++;
        }
      }
    }
    LOG.info("Number of blocks under construction: " + numUCBlocks);
    return numUCBlocks;
  }

  /** @return the lease containing src */
  public Lease getLeaseByPath(String src) {return sortedLeasesByPath.get(src);}

  /** @return the number of leases currently in the system */
  public synchronized int countLease() {return sortedLeases.size();}

  /** @return the number of paths contained in all leases */
  synchronized int countPath() {
    int count = 0;
    for(Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }

  /**
   * Adds (or re-adds) the lease for the specified file.
   *
   * 为某个客户端加入一个针对某个文件的契约
   *
   */
  synchronized Lease addLease(String holder, String src) {
    // holder一看就是代表客户端的名字
    // 刚开始肯定是找不到的
    Lease lease = getLease(holder);
    if (lease == null) {
      // 直接构造一个内部类，Lease对象，传入进去客户端的名称
      // 直接以<客户端名称, Lease契约>的key-value对格式，将他放入leased数据结构中
      // 同时将Lease数据加入了一个sortedLeases数据结构
      lease = new Lease(holder);
      leases.put(holder, lease);
      sortedLeases.add(lease);
    } else {
      renewLease(lease);
    }
    // 往sortedLeasesByPath的数据结构中加入契约
    // 这个数据结构的key，不是客户端名称，而是文件路径名，所以他默认是根据文件路径名来排序的
    sortedLeasesByPath.put(src, lease);
    lease.paths.add(src);
    return lease;
  }

  /**
   * Remove the specified lease and src.
   *
   * 删除某个客户端针对某个文件的契约
   * 从他所有的数据结构中，将这个文件契约给删除掉
   *
   */
  synchronized void removeLease(Lease lease, String src) {
    sortedLeasesByPath.remove(src);
    if (!lease.removePath(src)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(src + " not found in lease.paths (=" + lease.paths + ")");
      }
    }

    if (!lease.hasPath()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
      }
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(String holder, String src) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, src);
    } else {
      LOG.warn("Removing non-existent lease! holder=" + holder +
              " src=" + src);
    }
  }

  synchronized void removeAllLeases() {
    sortedLeases.clear();
    sortedLeasesByPath.clear();
    leases.clear();
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  synchronized Lease reassignLease(Lease lease, String src, String newHolder) {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, src);
    }
    return addLease(newHolder, src);
  }

  /**
   * Renew the lease(s) held by the given client
   *
   * renew，如果大家看过我之前分析过的spring cloud源码的课的话，就知道在里面
   * 微服务注册中心，eureka里面有lease和renew的概念
   * 各个微服务都会不断的发送心跳给eureka server，里面会维护每个服务跟eureka server之间的lease
   * 每次发送一次心跳，就会renew lease，续约
   *
   */
  synchronized void renewLease(String holder) {
    renewLease(getLease(holder));
  }
  synchronized void renewLease(Lease lease) {
    if (lease != null) {
      // 先从sortedLeases里面删除掉，因为他要重新续约
      // 更新最近一次续约的时间
      // 重新加入sortedLeases里面去，根据最新的续约时间重新排序
      sortedLeases.remove(lease);
      lease.renew();
      sortedLeases.add(lease);
    }
  }

  /**
   * Renew all of the currently open leases.
   */
  synchronized void renewAllLeases() {
    for (Lease l : leases.values()) {
      renewLease(l);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    // holder，哪个客户端持有的这份契约
    private final String holder;
    // 客户端，一看就是说，肯定是有了一份契约之后，必须得用一个后台线程不断发送请求
    // 来进行renew lease，续约
    // 这个lastUpdate一般存储的是最近一次他进行续约的时间
    private long lastUpdate;
    // 这个客户端在这份契约里针对哪些文件声明了自己的所有权
    private final Collection<String> paths = new TreeSet<String>();

    /** Only LeaseManager object can create a lease */
    private Lease(String holder) {
      this.holder = holder;
      renew();
    }
    /** Only LeaseManager object can renew a lease */
    private void renew() {
      this.lastUpdate = now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      // 当前时间 - 上次续约时间 > 1小时
      return now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return now() - lastUpdate > softLimit;
    }

    /** Does this lease contain any path? */
    boolean hasPath() {return !paths.isEmpty();}

    boolean removePath(String src) {
      return paths.remove(src);
    }

    @Override
    public String toString() {
      return "[Lease.  Holder: " + holder
              + ", pendingcreates: " + paths.size() + "]";
    }

    @Override
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      // 研究一下，各个Lease是根据什么来排序的？
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      // 是根据lastUpdate也就是最近一次续约的时间来进行排序的
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        // 如果两个客户端的续约的时间是一样的 ，那么就根据客户端的名称来排序
        return l1.holder.compareTo(l2.holder);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
              holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return holder.hashCode();
    }

    Collection<String> getPaths() {
      return paths;
    }

    String getHolder() {
      return holder;
    }

    void replacePath(String oldpath, String newpath) {
      paths.remove(oldpath);
      paths.add(newpath);
    }

    @VisibleForTesting
    long getLastUpdate() {
      return lastUpdate;
    }
  }

  synchronized void changeLease(String src, String dst) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
              " src=" + src + ", dest=" + dst);
    }

    final int len = src.length();
    for(Map.Entry<String, Lease> entry
            : findLeaseWithPrefixPath(src, sortedLeasesByPath).entrySet()) {
      final String oldpath = entry.getKey();
      final Lease lease = entry.getValue();
      // replace stem of src with new destination
      final String newpath = dst + oldpath.substring(len);
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
      sortedLeasesByPath.remove(oldpath);
      sortedLeasesByPath.put(newpath, lease);
    }
  }

  synchronized void removeLeaseWithPrefixPath(String prefix) {
    for(Map.Entry<String, Lease> entry
            : findLeaseWithPrefixPath(prefix, sortedLeasesByPath).entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
                + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());
    }
  }

  static private Map<String, Lease> findLeaseWithPrefixPath(
          String prefix, SortedMap<String, Lease> path2lease) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    final Map<String, Lease> entries = new HashMap<String, Lease>();
    int srclen = prefix.length();

    // prefix may ended with '/'
    if (prefix.charAt(srclen - 1) == Path.SEPARATOR_CHAR) {
      srclen -= 1;
    }

    for(Map.Entry<String, Lease> entry : path2lease.tailMap(prefix).entrySet()) {
      final String p = entry.getKey();
      if (!p.startsWith(prefix)) {
        return entries;
      }
      if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
        entries.put(entry.getKey(), entry.getValue());
      }
    }
    return entries;
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
  }

  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  // 这是一个后台线程
  // 这个东西默认一定会运行起来，在后台不断的监控各个lease契约
  // 如果某个客户端申请持有了一个文件的契约
  // 结果，不幸的事情发生了，也就是那个客户端莫名其妙的死亡了，挂了，然后没有删除这个契约
  // 那么此时就需要namenode里，LeaseManager的Monitor后台线程
  // 不断的监控各个lease，判断如果说某个客户端申请了契约之后，长时间范围内都没有来续约，认为他死了
  // 此时就要自动释放这个lease，不让一个客户端长时间占有这个文件的所有权
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    /** Check leases periodically. */
    @Override
    public void run() {
      for(; shouldRunMonitor && fsnamesystem.isRunning(); ) {
        boolean needSync = false;
        try {
          fsnamesystem.writeLockInterruptibly();
          try {
            if (!fsnamesystem.isInSafeMode()) {
              // 检查契约的核心逻辑，是在checkLeases()方法里
              // 在这个检查契约的逻辑中，可能会触发一些写edits log元数据的操作
              // 如果触发了的话，还让edits log sync一下
              needSync = checkLeases();
            }
          } finally {
            fsnamesystem.writeUnlock();
            // lease reassignments should to be sync'ed.
            if (needSync) {
              fsnamesystem.getEditLog().logSync();
            }
          }

          // 默认是每隔2秒，对所有的契约做一次检查
          Thread.sleep(HdfsServerConstants.NAMENODE_LEASE_RECHECK_INTERVAL);
        } catch(InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
  }

  /**
   * Get the list of inodes corresponding to valid leases.
   * @return list of inodes
   */
  Map<String, INodeFile> getINodesUnderConstruction() {
    Map<String, INodeFile> inodes = new TreeMap<String, INodeFile>();
    for (String p : sortedLeasesByPath.keySet()) {
      // verify that path exists in namespace
      try {
        INodeFile node = INodeFile.valueOf(fsnamesystem.dir.getINode(p), p);
        if (node.isUnderConstruction()) {
          inodes.put(p, node);
        } else {
          LOG.warn("Ignore the lease of file " + p
                  + " for checkpoint since the file is not under construction");
        }
      } catch (IOException ioe) {
        LOG.error(ioe);
      }
    }
    return inodes;
  }

  /** Check the leases beginning from the oldest.
   *
   *  sortedLeases默认是根据lease续约时间来进行排序的，默认是续约时间越旧，越老的，越靠前的
   *  是放在第一个
   *  所以在这里直接可以first()方法获取第一个契约
   *
   *  @return true is sync is needed.
   */
  @VisibleForTesting
  synchronized boolean checkLeases() {
    boolean needSync = false;
    assert fsnamesystem.hasWriteLock();
    Lease leaseToCheck = null;
    try {
      leaseToCheck = sortedLeases.first();
    } catch(NoSuchElementException e) {}

    while(leaseToCheck != null) {
      // 如果最老的一个契约
      // 所有的契约都有一个续约的时间
      // 比如说，契约1：10:00:00，契约2：10:05:00，契约3：10:20:00
      // 人家在这里做检查，检查契约过期的时候，不是很僵硬的遍历所有的契约来检查他的时间的，那样的话性能不是太好
      // 他就根据一个sortedLeases排序的数据结构，取出来续约时间最老的那个
      // 比如说10:00:00
      // 如果说那个最老的契约，都没有超过1小时还没续约，那么就不用检查了，因为就意味着后面的契约肯定续约时间都没超过1小时
      if (!leaseToCheck.expiredHardLimit()) {
        break;
      }

      // 如果进入到这里，发现有某个契约，已经超过了1小时还没续约了

      LOG.info(leaseToCheck + " has expired hard limit");

      final List<String> removing = new ArrayList<String>();
      // need to create a copy of the oldest lease paths, because
      // internalReleaseLease() removes paths corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      String[] leasePaths = new String[leaseToCheck.getPaths().size()];
      leaseToCheck.getPaths().toArray(leasePaths);
      // 变量这个契约中有权限的文件的路径
      for(String p : leasePaths) {
        try {
          // 此时就会自动释放掉那些契约
          // 释放掉一个契约针对某个文件路径的所有权
          boolean completed = fsnamesystem.internalReleaseLease(leaseToCheck, p,
                  HdfsServerConstants.NAMENODE_LEASE_HOLDER);
          if (LOG.isDebugEnabled()) {
            if (completed) {
              LOG.debug("Lease recovery for " + p + " is complete. File closed.");
            } else {
              LOG.debug("Started block recovery " + p + " lease " + leaseToCheck);
            }
          }
          // If a lease recovery happened, we need to sync later.
          if (!needSync && !completed) {
            needSync = true;
          }
        } catch (IOException e) {
          LOG.error("Cannot release the path " + p + " in the lease "
                  + leaseToCheck, e);
          removing.add(p);
        }
      }

      for(String p : removing) {
        // 删除这个契约
        removeLease(leaseToCheck, p);
      }
      // 获取到sortedLeases中的第二个契约，发现第一个契约是过期的
      leaseToCheck = sortedLeases.higher(leaseToCheck);
    }

    try {
      if(leaseToCheck != sortedLeases.first()) {
        LOG.warn("Unable to release hard-limit expired lease: "
                + sortedLeases.first());
      }
    } catch(NoSuchElementException e) {}
    return needSync;
  }

  @Override
  public synchronized String toString() {
    return getClass().getSimpleName() + "= {"
            + "\n leases=" + leases
            + "\n sortedLeases=" + sortedLeases
            + "\n sortedLeasesByPath=" + sortedLeasesByPath
            + "\n}";
  }

  /**
   * 这个方法，一看就是在这里构造了这个后台线程
   * 然后同时启动了这个后台线程
   * 这个方法一定是在FSNamesystem初始化的时候来调用LeaseManager的方法，启动他的后台线程
   */
  void startMonitor() {
    Preconditions.checkState(lmthread == null,
            "Lease Monitor already running");
    shouldRunMonitor = true;
    lmthread = new Daemon(new Monitor());
    lmthread.start();
  }

  void stopMonitor() {
    if (lmthread != null) {
      shouldRunMonitor = false;
      try {
        lmthread.interrupt();
        lmthread.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      lmthread = null;
    }
  }

  /**
   * Trigger the currently-running Lease monitor to re-check
   * its leases immediately. This is for use by unit tests.
   */
  @VisibleForTesting
  void triggerMonitorCheckNow() {
    Preconditions.checkState(lmthread != null,
            "Lease monitor is not running");
    lmthread.interrupt();
  }
}
