/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.transaction;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;

/**
 * This copies the code necessary for ensureKeyValue from KeyvalueUtil and
 * createFirstOnRow in PrivateCellUtil in HBase 3+. The signature changes
 * between HBase 3 and 2, which results in ensureKeyValue only working when Omid
 * is running with the same HBase version (2/3) as it was compiled with. By
 * copying the code here, we can work around this problem.
 */
public class OmidCellUtil {

    /**
     * @return <code>cell</code> if it is an object of class {@link KeyValue} else
     *         we will return a new {@link KeyValue} instance made from
     *         <code>cell</code> Note: Even if the cell is an object of any of the
     *         subclass of {@link KeyValue}, we will create a new {@link KeyValue}
     *         object wrapping same buffer. This API is used only with MR based
     *         tools which expect the type to be exactly KeyValue. That is the
     *         reason for doing this way.
     */
    public static KeyValue ensureKeyValue(final ExtendedCell cell) {
        if (cell == null)
            return null;
        if (cell instanceof KeyValue) {
            if (cell.getClass().getName().equals(KeyValue.class.getName())) {
                return (KeyValue) cell;
            }
            // Cell is an Object of any of the sub classes of KeyValue. Make a new KeyValue
            // wrapping the
            // same byte[]
            KeyValue kv = (KeyValue) cell;
            KeyValue newKv = new KeyValue(kv.getRowArray(), kv.getOffset(), kv.getLength());
            newKv.setSequenceId(kv.getSequenceId());
            return newKv;
        }
        return copyToNewKeyValue(cell);
    }

    public static KeyValue copyToNewKeyValue(final ExtendedCell cell) {
        byte[] bytes = copyToNewByteArray(cell);
        KeyValue kvCell = new KeyValue(bytes, 0, bytes.length);
        kvCell.setSequenceId(cell.getSequenceId());
        return kvCell;
    }

    public static byte[] copyToNewByteArray(final ExtendedCell cell) {
        int v1Length = cell.getSerializedSize();
        byte[] backingBytes = new byte[v1Length];
        appendToByteArray(cell, backingBytes, 0, true);
        return backingBytes;
    }

    public static int appendToByteArray(ExtendedCell cell, byte[] output, int offset, boolean withTags) {
        int pos = offset;
        pos = Bytes.putInt(output, pos, keyLength(cell));
        pos = Bytes.putInt(output, pos, cell.getValueLength());
        pos = appendKeyTo(cell, output, pos);
        pos = CellUtil.copyValueTo(cell, output, pos);
        if (withTags && (cell.getTagsLength() > 0)) {
            pos = Bytes.putAsShort(output, pos, cell.getTagsLength());
            pos = PrivateCellUtil.copyTagsTo(cell, output, pos);
        }
        return pos;
    }

    public static int length(short rlen, byte flen, int qlen, int vlen, int tlen, boolean withTags) {
        if (withTags) {
            return (int) KeyValue.getKeyValueDataStructureSize(rlen, flen, qlen, vlen, tlen);
        }
        return (int) KeyValue.getKeyValueDataStructureSize(rlen, flen, qlen, vlen);
    }

    /**
     * Returns number of bytes this cell's key part would have been used if
     * serialized as in {@link KeyValue}. Key includes rowkey, family, qualifier,
     * timestamp and type.
     * 
     * @return the key length
     */
    public static int keyLength(final Cell cell) {
        return keyLength(cell.getRowLength(), cell.getFamilyLength(), cell.getQualifierLength());
    }

    private static int keyLength(short rlen, byte flen, int qlen) {
        return (int) KeyValue.getKeyDataStructureSize(rlen, flen, qlen);
    }

    public static int appendKeyTo(final ExtendedCell cell, final byte[] output, final int offset) {
        int nextOffset = offset;
        nextOffset = Bytes.putShort(output, nextOffset, cell.getRowLength());
        nextOffset = CellUtil.copyRowTo(cell, output, nextOffset);
        nextOffset = Bytes.putByte(output, nextOffset, cell.getFamilyLength());
        nextOffset = CellUtil.copyFamilyTo(cell, output, nextOffset);
        nextOffset = CellUtil.copyQualifierTo(cell, output, nextOffset);
        nextOffset = Bytes.putLong(output, nextOffset, cell.getTimestamp());
        nextOffset = Bytes.putByte(output, nextOffset, cell.getTypeByte());
        return nextOffset;
    }

// =========================================================================================
// Code below is copied from PrivateCellUtil
// =========================================================================================

    public static ExtendedCell createFirstOnRow(final byte[] row, int roffset, short rlength,
            final byte[] family, int foffset, byte flength, final byte[] col, int coffset, int clength) {
            return new FirstOnRowColCell(row, roffset, rlength, family, foffset, flength, col, coffset,
              clength);
          }


    /**
     * These cells are used in reseeks/seeks to improve the read performance. They are not real cells
     * that are returned back to the clients
     */
    private static abstract class EmptyCell implements ExtendedCell {

      @Override
      public void setSequenceId(long seqId) {
        // Fake cells don't need seqId, so leaving it as a noop.
      }

      @Override
      public void setTimestamp(long ts) {
        // Fake cells can't be changed timestamp, so leaving it as a noop.
      }

      @Override
      public void setTimestamp(byte[] ts) {
        // Fake cells can't be changed timestamp, so leaving it as a noop.
      }

      @Override
      public byte[] getRowArray() {
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public int getRowOffset() {
        return 0;
      }

      @Override
      public short getRowLength() {
        return 0;
      }

      @Override
      public byte[] getFamilyArray() {
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public int getFamilyOffset() {
        return 0;
      }

      @Override
      public byte getFamilyLength() {
        return 0;
      }

      @Override
      public byte[] getQualifierArray() {
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public int getQualifierOffset() {
        return 0;
      }

      @Override
      public int getQualifierLength() {
        return 0;
      }

      @Override
      public long getSequenceId() {
        return 0;
      }

      @Override
      public byte[] getValueArray() {
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public int getValueOffset() {
        return 0;
      }

      @Override
      public int getValueLength() {
        return 0;
      }

      @Override
      public byte[] getTagsArray() {
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public int getTagsOffset() {
        return 0;
      }

      @Override
      public int getTagsLength() {
        return 0;
      }
    }

    
    private static class FirstOnRowCell extends EmptyCell {
        // @formatter:off
        private static final int FIXED_HEAPSIZE =
            ClassSize.OBJECT // object
          + ClassSize.REFERENCE // row array
          + Bytes.SIZEOF_INT // row offset
          + Bytes.SIZEOF_SHORT;  // row length
        // @formatter:on
        private final byte[] rowArray;
        private final int roffset;
        private final short rlength;

        public FirstOnRowCell(final byte[] row, int roffset, short rlength) {
          this.rowArray = row;
          this.roffset = roffset;
          this.rlength = rlength;
        }

        @Override
        public long heapSize() {
          return ClassSize.align(FIXED_HEAPSIZE)
            // array overhead
            + (rlength == 0 ? ClassSize.sizeOfByteArray(rlength) : rlength);
        }

        @Override
        public byte[] getRowArray() {
          return this.rowArray;
        }

        @Override
        public int getRowOffset() {
          return this.roffset;
        }

        @Override
        public short getRowLength() {
          return this.rlength;
        }

        @Override
        public long getTimestamp() {
          return HConstants.LATEST_TIMESTAMP;
        }

        @Override
        public byte getTypeByte() {
          return KeyValue.Type.Maximum.getCode();
        }

        @Override
        public Cell.Type getType() {
          throw new UnsupportedOperationException();
        }
      }

    
    private static class FirstOnRowColCell extends FirstOnRowCell {
        // @formatter:off
        private static final long FIXED_HEAPSIZE = (long) FirstOnRowCell.FIXED_HEAPSIZE
          + Bytes.SIZEOF_BYTE // flength
          + Bytes.SIZEOF_INT * 3 // foffset, qoffset, qlength
          + ClassSize.REFERENCE * 2; // fArray, qArray
        // @formatter:on
        private final byte[] fArray;
        private final int foffset;
        private final byte flength;
        private final byte[] qArray;
        private final int qoffset;
        private final int qlength;

        public FirstOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray, int foffset,
          byte flength, byte[] qArray, int qoffset, int qlength) {
          super(rArray, roffset, rlength);
          this.fArray = fArray;
          this.foffset = foffset;
          this.flength = flength;
          this.qArray = qArray;
          this.qoffset = qoffset;
          this.qlength = qlength;
        }

        @Override
        public long heapSize() {
          return ClassSize.align(FIXED_HEAPSIZE)
            // array overhead
            + (flength == 0 ? ClassSize.sizeOfByteArray(flength) : flength)
            + (qlength == 0 ? ClassSize.sizeOfByteArray(qlength) : qlength);
        }

        @Override
        public byte[] getFamilyArray() {
          return this.fArray;
        }

        @Override
        public int getFamilyOffset() {
          return this.foffset;
        }

        @Override
        public byte getFamilyLength() {
          return this.flength;
        }

        @Override
        public byte[] getQualifierArray() {
          return this.qArray;
        }

        @Override
        public int getQualifierOffset() {
          return this.qoffset;
        }

        @Override
        public int getQualifierLength() {
          return this.qlength;
        }
      }
    
    
}
