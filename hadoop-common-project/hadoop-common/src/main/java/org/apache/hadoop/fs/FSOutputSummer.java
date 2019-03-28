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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends OutputStream {
  // data checksum
  private final DataChecksum sum;
  // internal buffer for storing data before it is checksumed
  private byte buf[];
  // internal buffer for storing checksum
  private byte checksum[];
  // The number of valid bytes in the buffer.
  private int count;
  
  // We want this value to be a multiple of 3 because the native code checksums
  // 3 chunks simultaneously. The chosen value of 9 strikes a balance between
  // limiting the number of JNI calls and flushing to the underlying stream
  // relatively frequently.
  private static final int BUFFER_NUM_CHUNKS = 9;
  
  protected FSOutputSummer(DataChecksum sum) {
    this.sum = sum;
    // 一个buf缓冲数组的大小，是一个chunk的512字节的大小 * 9个chunk
    this.buf = new byte[sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS];
    this.checksum = new byte[getChecksumSize() * BUFFER_NUM_CHUNKS];
    this.count = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len > 0</code>, and its checksum
   */
  protected abstract void writeChunk(byte[] b, int bOffset, int bLen,
      byte[] checksum, int checksumOffset, int checksumLen) throws IOException;
  
  /**
   * Check if the implementing OutputStream is closed and should no longer
   * accept writes. Implementations should do nothing if this stream is not
   * closed, and should throw an {@link IOException} if it is closed.
   * 
   * @throws IOException if this stream is already closed.
   */
  protected abstract void checkClosed() throws IOException;

  /** Write one byte */
  @Override
  public synchronized void write(int b) throws IOException {
    buf[count++] = (byte)b;
    if(count == buf.length) {
      flushBuffer();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * 稍微普及一下所谓的checksum，他主要是保证一个数据传输过程中不会发生破损，或者是存储到了磁盘以后不会发生破损
   * 如果发生了数据破损，那么可以及时发现
   * 
   * 本来你有一个chunk，里面存储的数据是：张三很帅！
   * 然后呢，你可以针对这个chunk的数据，计算一个checksum校验和出来，比如：342kddd00
   * 
   * 万一在传输的过程中，chunk里的一些数据传丢了，导致传送到datanode的时候，数据变为了：张三很！
   * 然后呢，比方说，在daatanode那儿，他要确保一下，你传递的过程中，数据有没有出现破损的话，就可以重新基于内容算一个checksum
   * 比如说基于“张三很！”算一个checksum：556kk908
   * 
   * 发现人家传递过来的那个checksum，跟你计算出来的checksum，不一样的
   * 
   * 说明你在传递的过程中，出现了一些数据的破损，这个chunk的数据内容是不能用的
   * 
   * 每个chunk都有一个checksum，在packet里，其实是有一大堆的chunk和一大堆的checksum，还有一些header
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids uneccessary data copy.
   *
   * byte b[]，这个字节数组是从哪儿来的？
   * 这东西，就是你的文件输入流，从你本地的1TB的大文件里读出来的，你可以读个几百个字节，发送给DFSOutputStream
   * 人家就可以获取到一个字节数组
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    
    checkClosed();
    
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // 人家其实就是调用write1方法
    // 尝试将byte[]数组中的数据全部通过write1写入一个底层的地方去
    for (int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   */
  private int write1(byte b[], int off, int len) throws IOException {
    if(count==0 && len>=buf.length) {
      // local buffer is empty and user buffer size >= local buffer size, so
      // simply checksum the user buffer and send it directly to the underlying
      // stream
      // 这个的意思就是说，你写入的这个字节数组的大小，直接就达到了一个chunk的大小了
      // 你传递进来的字节数组的大小 >= 缓冲数组的大小
      // 所以此时，就不需要将字节数组的数据写入一个缓冲，直接就将这个字节数组的数据作为一个chunk写入底层的流就可以了
      // 写入底层的流中，将这个数组切割为chunk
      final int length = buf.length;
      writeChecksumChunks(b, off, length);
      return length;
    }
    
    // copy user data to local buffer
    // 或者，比如说，如果你当前这个字节数组的数据还没达到一个chunk的大小
    // 此时可以将这个字节数组的数据先写入一个buffer缓冲中
    int bytesToCopy = buf.length-count;
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
    // 当你不断的往缓冲数组里拷入数据，直到拷贝进缓冲数组的数据，他的总大小，等于了缓冲数组最大的长度之后
    // 你的缓冲数组被拷贝满了
    // 此时就会执行flushBuffer方法，刷新缓冲数组中的数据到底层流里去，切割成chunk
    if (count == buf.length) {
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream. 
   */
  protected synchronized void flushBuffer() throws IOException {
    flushBuffer(false, true);
  }

  /* Forces buffered output bytes to be checksummed and written out to
   * the underlying output stream. If there is a trailing partial chunk in the
   * buffer,
   * 1) flushPartial tells us whether to flush that chunk
   * 2) if flushPartial is true, keep tells us whether to keep that chunk in the
   * buffer (if flushPartial is false, it is always kept in the buffer)
   *
   * Returns the number of bytes that were flushed but are still left in the
   * buffer (can only be non-zero if keep is true).
   */
  protected synchronized int flushBuffer(boolean keep,
      boolean flushPartial) throws IOException {
    int bufLen = count;
    int partialLen = bufLen % sum.getBytesPerChecksum();
    int lenToFlush = flushPartial ? bufLen : bufLen - partialLen;
    if (lenToFlush != 0) {
      writeChecksumChunks(buf, 0, lenToFlush);
      if (!flushPartial || keep) {
        count = partialLen;
        System.arraycopy(buf, bufLen - count, buf, 0, count);
      } else {
      count = 0;
      }
    }

    // total bytes left minus unflushed bytes left
    return count - (bufLen - lenToFlush);
  }

  /**
   * Checksums all complete data chunks and flushes them to the underlying
   * stream. If there is a trailing partial chunk, it is not flushed and is
   * maintained in the buffer.
   */
  public void flush() throws IOException {
    flushBuffer(false, false);
  }

  /**
   * Return the number of valid bytes currently in the buffer.
   */
  protected synchronized int getBufferedDataSize() {
    return count;
  }

  /** @return the size for a checksum. */
  protected int getChecksumSize() {
    return sum.getChecksumSize();
  }

  /** Generate checksums for the given data chunks and output chunks & checksums
   * to the underlying output stream.
   */
  private void writeChecksumChunks(byte b[], int off, int len)
  throws IOException {
    sum.calculateChunkedSums(b, off, len, checksum, 0);
    for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
      // 核心的切割chunk的算法，就在这里
      // 缓冲数组里面可以放不止一个chunk的，看过源码了，知道那个缓冲数组里最多可以放达到9个chunk的数据
      // 当你的缓冲数组都写满了，开始到这里来切割chunk
      // 在这里，这个for循环，就是在切割chunk，每个chunk就是512字节
      int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
      int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();
      // 在这里其实就是把byte[]数组里的固定的512个字节的数据，写入底层，切割成一个chunk
      writeChunk(b, off + i, chunkLen, checksum, ckOffset, getChecksumSize());
    }
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
    return int2byte((int)sum.getValue(), new byte[checksumSize]);
  }

  static byte[] int2byte(int integer, byte[] bytes) {
    if (bytes.length != 0) {
      bytes[0] = (byte) ((integer >>> 24) & 0xFF);
      bytes[1] = (byte) ((integer >>> 16) & 0xFF);
      bytes[2] = (byte) ((integer >>> 8) & 0xFF);
      bytes[3] = (byte) ((integer >>> 0) & 0xFF);
      return bytes;
    }
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void setChecksumBufSize(int size) {
    this.buf = new byte[size];
    this.checksum = new byte[sum.getChecksumSize(size)];
    this.count = 0;
  }

  protected synchronized void resetChecksumBufSize() {
    setChecksumBufSize(sum.getBytesPerChecksum() * BUFFER_NUM_CHUNKS);
  }
}
