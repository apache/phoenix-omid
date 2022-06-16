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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Charsets;
import org.apache.phoenix.thirdparty.com.google.common.base.MoreObjects;
import org.apache.phoenix.thirdparty.com.google.common.base.Objects;
import org.apache.phoenix.thirdparty.com.google.common.base.MoreObjects.ToStringHelper;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.hash.Hasher;
import org.apache.phoenix.thirdparty.com.google.common.hash.Hashing;

@SuppressWarnings("all")
public final class CellUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CellUtils.class);
    static final byte[] SHADOW_CELL_SUFFIX = "\u0080".getBytes(Charsets.UTF_8); // Non printable char (128 ASCII)
    //Prefix starts with 0 to apear before other cells in TransactionVisibilityFilter
    static final byte[] SHADOW_CELL_PREFIX = "\u0000\u0080".getBytes(Charsets.UTF_8);
    static byte[] DELETE_TOMBSTONE = HConstants.EMPTY_BYTE_ARRAY;
    static byte[] LEGACY_DELETE_TOMBSTONE = Bytes.toBytes("__OMID_TOMBSTONE__");
    public static final byte[] FAMILY_DELETE_QUALIFIER = HConstants.EMPTY_BYTE_ARRAY;
    public static final String TRANSACTION_ATTRIBUTE = "__OMID_TRANSACTION__";
    /**/
    public static final String CLIENT_GET_ATTRIBUTE = "__OMID_CLIENT_GET__";
    public static final String LL_ATTRIBUTE = "__OMID_LL__";

    /**
     * Utility interface to get rid of the dependency on HBase server package
     */
    interface CellGetter {
        Result get(Get get) throws IOException;
    }

    /**
     * Returns true if the particular cell passed exists in the datastore.
     * @param row row
     * @param family column family
     * @param qualifier columnn name
     * @param version version
     * @param cellGetter an instance of CellGetter
     * @return true if the cell specified exists. false otherwise
     * @throws IOException
     */
    public static boolean hasCell(byte[] row,
                                  byte[] family,
                                  byte[] qualifier,
                                  long version,
                                  CellGetter cellGetter)
            throws IOException {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setTimeStamp(version);

        Result result = cellGetter.get(get);

        return result.containsColumn(family, qualifier);
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell in the datastore
     * @param row row
     * @param family column family
     * @param qualifier columnn name
     * @param version version
     * @param cellGetter an instance of CellGetter
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(byte[] row,
                                        byte[] family,
                                        byte[] qualifier,
                                        long version,
                                        CellGetter cellGetter) throws IOException {
        return hasCell(row, family, addShadowCellSuffixPrefix(qualifier),
                version, cellGetter);
    }

    /**
     * Builds a new qualifier composed of the HBase qualifier passed + the shadow cell suffix.
     * @param qualifierArray the qualifier to be suffixed
     * @param qualOffset the offset where the qualifier starts
     * @param qualLength the qualifier length
     * @return the suffixed qualifier
     */
    public static byte[] addShadowCellSuffixPrefix(byte[] qualifierArray, int qualOffset, int qualLength) {
        byte[] result = new byte[qualLength + SHADOW_CELL_SUFFIX.length + SHADOW_CELL_PREFIX.length];
        System.arraycopy(SHADOW_CELL_PREFIX, 0, result,0 , SHADOW_CELL_PREFIX.length);
        System.arraycopy(qualifierArray, qualOffset, result, SHADOW_CELL_PREFIX.length, qualLength);
        System.arraycopy(SHADOW_CELL_SUFFIX, 0, result, qualLength + SHADOW_CELL_PREFIX.length,
                SHADOW_CELL_SUFFIX.length);
        return result;
    }

    /**
     * Builds a new qualifier composed of the HBase qualifier passed + the shadow cell suffix.
     * Contains a reduced signature to avoid boilerplate code in client side.
     * @param qualifier
     *            the qualifier to be suffixed
     * @return the suffixed qualifier
     */
    public static byte[] addShadowCellSuffixPrefix(byte[] qualifier) {
        return addShadowCellSuffixPrefix(qualifier, 0, qualifier.length);
    }

    /**
     * Builds a new qualifier removing the shadow cell suffix from the
     * passed HBase qualifier.
     * @param qualifier the qualifier to remove the suffix from
     * @param qualOffset the offset where the qualifier starts
     * @param qualLength the qualifier length
     * @return the new qualifier without the suffix
     */
    public static byte[] removeShadowCellSuffixPrefix(byte[] qualifier, int qualOffset, int qualLength) {
        if (endsWith(qualifier, qualOffset, qualLength, SHADOW_CELL_SUFFIX)) {
            if (startsWith(qualifier, qualOffset,qualLength, SHADOW_CELL_PREFIX)) {
                return Arrays.copyOfRange(qualifier,
                        qualOffset + SHADOW_CELL_PREFIX.length,
                        qualOffset + (qualLength - SHADOW_CELL_SUFFIX.length));
            } else {
                //support backward competatbiliy
                return Arrays.copyOfRange(qualifier,
                        qualOffset,qualOffset + (qualLength - SHADOW_CELL_SUFFIX.length));
            }

        }

        throw new IllegalArgumentException(
                "Can't find shadow cell suffix in qualifier "
                        + Bytes.toString(qualifier));
    }

    /**
     * Returns the qualifier length removing the shadow cell suffix and prefix. In case that que suffix is not found,
     * just returns the length of the qualifier passed.
     * @param qualifier the qualifier to remove the suffix from
     * @param qualOffset the offset where the qualifier starts
     * @param qualLength the qualifier length
     * @return the qualifier length without the suffix
     */
    public static int qualifierLengthFromShadowCellQualifier(byte[] qualifier, int qualOffset, int qualLength) {

        if (endsWith(qualifier, qualOffset, qualLength, SHADOW_CELL_SUFFIX)) {
            if (startsWith(qualifier,qualOffset, qualLength, SHADOW_CELL_PREFIX)) {
                return qualLength - SHADOW_CELL_SUFFIX.length - SHADOW_CELL_PREFIX.length;
            } else {
                return qualLength - SHADOW_CELL_SUFFIX.length;
            }
        }
        return qualLength;
    }


    /**
     * Returns the qualifier length removing the shadow cell suffix and prefix. In case that que suffix is not found,
     * just returns the length of the qualifier passed.
     * @param qualifier the qualifier to remove the suffix from
     * @param qualOffset the offset where the qualifier starts
     * @param qualLength the qualifier length
     * @return the qualifier length without the suffix
     */
    public static int qualifierOffsetFromShadowCellQualifier(byte[] qualifier, int qualOffset, int qualLength) {

        if (startsWith(qualifier, qualOffset, qualLength, SHADOW_CELL_PREFIX)) {
            return qualOffset + SHADOW_CELL_PREFIX.length;
        }
        return qualOffset;
    }


    /**
     * Complement to matchingQualifier() methods in HBase's CellUtil.class
     * @param left the cell to compare the qualifier
     * @param qualArray the explicit qualifier array passed
     * @param qualOffset the explicit qualifier offset passed
     * @param qualLen the explicit qualifier length passed
     * @return whether the qualifiers are equal or not
     */
    public static boolean matchingQualifier(final Cell left, final byte[] qualArray, int qualOffset, int qualLen) {
        return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(),
                qualArray, qualOffset, qualLen);
    }

    /**
     * Check that the cell passed meets the requirements for a valid cell identifier with Omid. Basically, users can't:
     * 1) specify a timestamp
     * 2) use a particular suffix in the qualifier
     */
    public static void validateCell(Cell cell, long startTimestamp) {
        // Throw exception if timestamp is set by the user
        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP
                && cell.getTimestamp() != startTimestamp) {
            throw new IllegalArgumentException(
                    "Timestamp not allowed in transactional user operations");
        }
        // Throw exception if using a non-allowed qualifier
        if (isShadowCell(cell)) {
            throw new IllegalArgumentException(
                    "Reserved string used in column qualifier");
        }
    }

    /**
     * Returns whether a cell contains a qualifier that is a delete cell
     * column qualifier or not.
     * @param cell the cell to check if contains the delete cell qualifier
     * @return whether the cell passed contains a delete cell qualifier or not
     */
    public static boolean isFamilyDeleteCell(Cell cell) {
        return CellUtil.matchingQualifier(cell, CellUtils.FAMILY_DELETE_QUALIFIER) &&
                CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);
    }

    /**
     * Returns whether a cell contains a qualifier that is a shadow cell
     * column qualifier or not.
     * @param cell the cell to check if contains the shadow cell qualifier
     * @return whether the cell passed contains a shadow cell qualifier or not
     */
    public static boolean isShadowCell(Cell cell) {
        byte[] qualifier = cell.getQualifierArray();
        int qualOffset = cell.getQualifierOffset();
        int qualLength = cell.getQualifierLength();

        return endsWith(qualifier, qualOffset, qualLength, SHADOW_CELL_SUFFIX);
    }

    private static boolean endsWith(byte[] value, int offset, int length, byte[] suffix) {
        if (length <= suffix.length) {
            return false;
        }

        int suffixOffset = offset + length - suffix.length;
        int result = Bytes.compareTo(value, suffixOffset, suffix.length,
                suffix, 0, suffix.length);
        return result == 0;
    }

    private static boolean startsWith(byte[] value, int offset, int length, byte[] prefix) {
        if (length <= prefix.length) {
            return false;
        }

        int result = Bytes.compareTo(value, offset, prefix.length,
                prefix, 0, prefix.length);
        return result == 0;
    }

    /**
     * Returns if a cell is marked as a tombstone.
     * @param cell the cell to check
     * @return whether the cell is marked as a tombstone or not
     */
    public static boolean isTombstone(Cell cell) {
        return CellUtil.matchingValue(cell, DELETE_TOMBSTONE) ||
                CellUtil.matchingValue(cell, LEGACY_DELETE_TOMBSTONE);
    }


    /**
     * Returns a new shadow cell created from a particular cell.
     * @param cell
     *            the cell to reconstruct the shadow cell from.
     * @param shadowCellValue
     *            the value for the new shadow cell created
     * @return the brand-new shadow cell
     */
    public static Cell buildShadowCellFromCell(Cell cell, byte[] shadowCellValue) {
        byte[] shadowCellQualifier = addShadowCellSuffixPrefix(cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength());
        return new KeyValue(
                cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                shadowCellQualifier, 0, shadowCellQualifier.length,
                cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                shadowCellValue, 0, shadowCellValue.length);
    }

    /**
     * Analyzes a list of cells, associating the corresponding shadow cell if present.
     *
     * @param cells the list of cells to classify
     * @return a sorted map associating each cell with its shadow cell
     */
    public static SortedMap<Cell, Optional<Cell>> mapCellsToShadowCells(List<Cell> cells) {

        // Move CellComparator to HBaseSims for 2.0 support
        // Need to access through CellComparatorImpl.COMPARATOR
        SortedMap<Cell, Optional<Cell>> cellToShadowCellMap
                = new TreeMap<Cell, Optional<Cell>>(CellComparatorImpl.COMPARATOR);

        Map<CellId, Cell> cellIdToCellMap = new HashMap<CellId, Cell>();
        Map<CellId, Cell> cellIdToSCCellMap = new HashMap<CellId, Cell>();
        for (Cell cell : cells) {
            if (!isShadowCell(cell)) {
                CellId key = new CellId(cell, false);
                // Get the current cell and compare the values
                Cell storedCell = cellIdToCellMap.get(key);
                if (storedCell != null) {
                    if (CellUtil.matchingValue(cell, storedCell)) {
                        // TODO: Should we check also here the MVCC and swap if its greater???
                        // Values are the same, ignore
                    } else {
                        if (cell.getSequenceId() > storedCell.getSequenceId()) { // Swap values
                            Optional<Cell> previousValue = cellToShadowCellMap.remove(storedCell);
                            Preconditions.checkNotNull(previousValue, "Should contain an Optional<Cell> value");
                            cellIdToCellMap.put(key, cell);
                            cellToShadowCellMap.put(cell, previousValue);
                        } else {
                            LOG.warn("Cell {} with an earlier MVCC found. Ignoring...", cell);
                        }
                    }
                } else {
                    cellIdToCellMap.put(key, cell);
                    Cell sc = cellIdToSCCellMap.get(key);
                    if (sc != null) {
                        cellToShadowCellMap.put(cell, Optional.of(sc));
                    } else {
                        cellToShadowCellMap.put(cell, Optional.<Cell>absent());
                    }
                }
            } else {
                CellId key = new CellId(cell, true);
                Cell savedCell = cellIdToCellMap.get(key);
                if (savedCell != null) {
                    Cell originalCell = savedCell;
                    cellToShadowCellMap.put(originalCell, Optional.of(cell));
                } else {
                    cellIdToSCCellMap.put(key, cell);
                }
            }
        }

        return cellToShadowCellMap;
    }

    private static class CellId {

        private static final int MIN_BITS = 32;

        private final Cell cell;
        private final boolean isShadowCell;

        CellId(Cell cell, boolean isShadowCell) {

            this.cell = cell;
            this.isShadowCell = isShadowCell;

        }

        Cell getCell() {
            return cell;
        }

        boolean isShadowCell() {
            return isShadowCell;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof CellId))
                return false;
            CellId otherCellId = (CellId) o;
            Cell otherCell = otherCellId.getCell();

            // Row comparison
            if (!CellUtil.matchingRow(otherCell, cell)) {
                return false;
            }

            // Family comparison
            if (!CellUtil.matchingFamily(otherCell, cell)) {
                return false;
            }

            // Qualifier comparison
            int qualifierLength = cell.getQualifierLength();
            int qualifierOffset = cell.getQualifierOffset();
            int otherQualifierLength = otherCell.getQualifierLength();
            int otherQualifierOffset = otherCell.getQualifierOffset();

            if (isShadowCell()) {
                qualifierLength = qualifierLengthFromShadowCellQualifier(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                qualifierOffset = qualifierOffsetFromShadowCellQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength());
            }
            if (otherCellId.isShadowCell()) {
                otherQualifierLength = qualifierLengthFromShadowCellQualifier(otherCell.getQualifierArray(),
                        otherCell.getQualifierOffset(),
                        otherCell.getQualifierLength());
                otherQualifierOffset = qualifierOffsetFromShadowCellQualifier(otherCell.getQualifierArray(), otherCell.getQualifierOffset(),
                        otherCell.getQualifierLength());
            }

            if (!Bytes.equals(cell.getQualifierArray(), qualifierOffset, qualifierLength,
                    otherCell.getQualifierArray(), otherQualifierOffset, otherQualifierLength)) {
                return false;
            }

            // Timestamp comparison
            return otherCell.getTimestamp() == cell.getTimestamp();

        }

        @Override
        public int hashCode() {
            Hasher hasher = Hashing.goodFastHash(MIN_BITS).newHasher();
            hasher.putBytes(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            hasher.putBytes(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            int qualifierLength = cell.getQualifierLength();
            int qualifierOffset = cell.getQualifierOffset();
            if (isShadowCell()) {
                qualifierLength = qualifierLengthFromShadowCellQualifier(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                if (startsWith(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength(), SHADOW_CELL_PREFIX)) {
                    qualifierOffset = qualifierOffset + SHADOW_CELL_PREFIX.length;
                }
            }

            hasher.putBytes(cell.getQualifierArray(),qualifierOffset , qualifierLength);
            hasher.putLong(cell.getTimestamp());
            return hasher.hash().asInt();
        }

        @Override
        public String toString() {
            ToStringHelper helper = MoreObjects.toStringHelper(this);
            helper.add("row", Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            helper.add("family", Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
            helper.add("is shadow cell?", isShadowCell);
            helper.add("qualifier",
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
            if (isShadowCell()) {
                int qualifierLength = qualifierLengthFromShadowCellQualifier(cell.getQualifierArray(),
                        cell.getQualifierOffset(),
                        cell.getQualifierLength());
                byte[] cellWithoutSc = removeShadowCellSuffixPrefix(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                helper.add("qualifier whithout shadow cell suffix", Bytes.toString(cellWithoutSc));
            }
            helper.add("ts", cell.getTimestamp());
            return helper.toString();
        }
    }

}
