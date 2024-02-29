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

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.tso.client.CellId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.apache.omid.transaction.CellUtils.SHADOW_CELL_PREFIX;
import static org.apache.omid.transaction.CellUtils.SHADOW_CELL_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "noHBase")
public class TestCellUtils {

    private final byte[] row = Bytes.toBytes("test-row");
    private final byte[] family = Bytes.toBytes("test-family");
    private final byte[] qualifier = Bytes.toBytes("test-qual");
    private final byte[] otherQualifier = Bytes.toBytes("other-test-qual");

    @DataProvider(name = "shadow-cell-suffixes")
    public Object[][] createShadowCellSuffixes() {
        return new Object[][]{
                {SHADOW_CELL_SUFFIX},
        };
    }

    @Test(dataProvider = "shadow-cell-suffixes", timeOut = 10_000)
    public void testShadowCellQualifiers(byte[] shadowCellSuffixToTest) throws IOException {

        final byte[] validShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(qualifier, shadowCellSuffixToTest);
        final byte[] sandwichValidShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(shadowCellSuffixToTest, validShadowCellQualifier);
        final byte[] doubleEndedValidShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(validShadowCellQualifier, shadowCellSuffixToTest);
        final byte[] interleavedValidShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(validShadowCellQualifier, org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes
                        .concat(validShadowCellQualifier, validShadowCellQualifier));
        final byte[] value = Bytes.toBytes("test-value");

        // Test the qualifier passed is a shadow cell
        // qualifier because it contains only one suffix
        // and is placed at the end of the qualifier:
        // qual_nameSUFFIX
        KeyValue kv = new KeyValue(row, family, validShadowCellQualifier, value);
        assertTrue(CellUtils.isShadowCell(kv), "Should include a valid shadowCell identifier");

        // We also accept this pattern in the qualifier:
        // SUFFIXqual_nameSUFFIX
        kv = new KeyValue(row, family, sandwichValidShadowCellQualifier, value);
        assertTrue(CellUtils.isShadowCell(kv), "Should include a valid shadowCell identifier");

        // We also accept this pattern in the qualifier:
        // qual_nameSUFFIXSUFFIX
        kv = new KeyValue(row, family, doubleEndedValidShadowCellQualifier, value);
        assertTrue(CellUtils.isShadowCell(kv), "Should include a valid shadowCell identifier");

        // We also accept this pattern in the qualifier:
        // qual_nameSUFFIXqual_nameSUFFIXqual_nameSUFFIX
        kv = new KeyValue(row, family, interleavedValidShadowCellQualifier, value);
        assertTrue(CellUtils.isShadowCell(kv), "Should include a valid shadowCell identifier");

        // Test the qualifier passed is not a shadow cell
        // qualifier if there's nothing else apart from the suffix
        kv = new KeyValue(row, family, shadowCellSuffixToTest, value);
        assertFalse(CellUtils.isShadowCell(kv), "Should not include a valid shadowCell identifier");

    }

    @Test(timeOut = 10_000)
    public void testCorrectMapingOfCellsToShadowCells() throws IOException {
        // Create the required data
        final byte[] validShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, SHADOW_CELL_SUFFIX);

        final byte[] qualifier2 = Bytes.toBytes("test-qual2");
        final byte[] validShadowCellQualifier2 =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier2, SHADOW_CELL_SUFFIX);

        final byte[] qualifier3 = Bytes.toBytes("test-qual3");

        Cell cell1 = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value")); // Default type is Put
        Cell dupCell1 = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value")); // Default type is Put
        Cell dupCell1WithAnotherValue = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("other-value"));
        Cell delCell1 = new KeyValue(row, family, qualifier, 1, Type.Delete, Bytes.toBytes("value"));
        Cell shadowCell1 = new KeyValue(row, family, validShadowCellQualifier, 1, Bytes.toBytes("sc-value"));

        Cell cell2 = new KeyValue(row, family, qualifier2, 1, Bytes.toBytes("value2"));
        Cell shadowCell2 = new KeyValue(row, family, validShadowCellQualifier2, 1, Bytes.toBytes("sc-value2"));

        Cell cell3 = new KeyValue(row, family, qualifier3, 1, Bytes.toBytes("value3"));

        // Check a list of cells with duplicate values
        List<Cell> badListWithDups = new ArrayList<>();
        badListWithDups.add(cell1);
        badListWithDups.add(dupCell1WithAnotherValue);

        // Check dup shadow cell with same MVCC is ignored
        SortedMap<Cell, Optional<Cell>> cellsToShadowCells = CellUtils.mapCellsToShadowCells(badListWithDups);
        assertEquals(cellsToShadowCells.size(), 1, "There should be only 1 key-value maps");
        assertTrue(cellsToShadowCells.containsKey(cell1));
        KeyValue firstKey = (KeyValue) cellsToShadowCells.firstKey();
        KeyValue lastKey = (KeyValue) cellsToShadowCells.lastKey();
        assertTrue(firstKey.equals(lastKey));
        assertTrue(0 == Bytes.compareTo(firstKey.getValueArray(), firstKey.getValueOffset(), firstKey.getValueLength(),
                                        cell1.getValueArray(), cell1.getValueOffset(), cell1.getValueLength()),
                   "Should be equal");

        // Modify dup shadow cell to have a greater MVCC and check that is replaced
        ((KeyValue)dupCell1WithAnotherValue).setSequenceId(1);
        cellsToShadowCells = CellUtils.mapCellsToShadowCells(badListWithDups);
        assertEquals(cellsToShadowCells.size(), 1, "There should be only 1 key-value maps");
        assertTrue(cellsToShadowCells.containsKey(dupCell1WithAnotherValue));
        firstKey = (KeyValue) cellsToShadowCells.firstKey();
        lastKey = (KeyValue) cellsToShadowCells.lastKey();
        assertTrue(firstKey.equals(lastKey));
        assertTrue(0 == Bytes.compareTo(firstKey.getValueArray(), firstKey.getValueOffset(),
                                        firstKey.getValueLength(), dupCell1WithAnotherValue.getValueArray(),
                                        dupCell1WithAnotherValue.getValueOffset(), dupCell1WithAnotherValue.getValueLength()),
                   "Should be equal");
        // Check a list of cells with duplicate values
        List<Cell> cellListWithDups = new ArrayList<>();
        cellListWithDups.add(cell1);
        cellListWithDups.add(shadowCell1);
        cellListWithDups.add(dupCell1); // Dup cell
        cellListWithDups.add(delCell1); // Another Dup cell but with different type
        cellListWithDups.add(cell2);
        cellListWithDups.add(cell3);
        cellListWithDups.add(shadowCell2);

        cellsToShadowCells = CellUtils.mapCellsToShadowCells(cellListWithDups);
        assertEquals(cellsToShadowCells.size(), 3, "There should be only 3 key-value maps");
        assertTrue(cellsToShadowCells.get(cell1).get().equals(shadowCell1));
        assertTrue(cellsToShadowCells.get(dupCell1).get().equals(shadowCell1));
        assertFalse(cellsToShadowCells.containsKey(delCell1)); // TODO This is strange and needs to be solved.
        // The current algo avoids to put the delete cell
        // as key after the put cell with same value was added
        assertTrue(cellsToShadowCells.get(cell2).get().equals(shadowCell2));
        assertTrue(cellsToShadowCells.get(cell3).equals(Optional.absent()));

    }

    @Test(timeOut = 10_000)
    public void testShadowCellSuffixConcatenationToQualifier() {

        Cell cell = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value"));
        byte[] suffixedQualifier = CellUtils.addShadowCellSuffixPrefix(cell.getQualifierArray(),
                                                                 cell.getQualifierOffset(),
                                                                 cell.getQualifierLength());
        byte[] expectedQualifier = org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, SHADOW_CELL_SUFFIX);
        assertEquals(suffixedQualifier, expectedQualifier);

    }

    @Test(dataProvider = "shadow-cell-suffixes", timeOut = 10_000)
    public void testShadowCellSuffixRemovalFromQualifier(byte[] shadowCellSuffixToTest) throws IOException {

        // Test removal from a correclty suffixed qualifier
        byte[] suffixedQualifier = org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, shadowCellSuffixToTest);
        Cell cell = new KeyValue(row, family, suffixedQualifier, 1, Bytes.toBytes("value"));
        byte[] resultedQualifier = CellUtils.removeShadowCellSuffixPrefix(cell.getQualifierArray(),
                                                                    cell.getQualifierOffset(),
                                                                    cell.getQualifierLength());
        byte[] expectedQualifier = qualifier;
        assertEquals(resultedQualifier, expectedQualifier);

        // Test removal from a badly suffixed qualifier
        byte[] badlySuffixedQualifier = org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, Bytes.toBytes("BAD"));
        Cell badCell = new KeyValue(row, family, badlySuffixedQualifier, 1, Bytes.toBytes("value"));
        try {
            CellUtils.removeShadowCellSuffixPrefix(badCell.getQualifierArray(),
                                             badCell.getQualifierOffset(),
                                             badCell.getQualifierLength());
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test(timeOut = 10_000)
    public void testMatchingQualifiers() {
        Cell cell = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value"));
        assertTrue(CellUtils.matchingQualifier(cell, qualifier, 0, qualifier.length));
        assertFalse(CellUtils.matchingQualifier(cell, otherQualifier, 0, otherQualifier.length));
    }

    @Test(dataProvider = "shadow-cell-suffixes", timeOut = 10_000)
    public void testQualifierLengthFromShadowCellQualifier(byte[] shadowCellSuffixToTest) {
        // Test suffixed qualifier
        byte[] suffixedQualifier = org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, shadowCellSuffixToTest);
        int originalQualifierLength =
                CellUtils.qualifierLengthFromShadowCellQualifier(suffixedQualifier, 0, suffixedQualifier.length);
        assertEquals(originalQualifierLength, qualifier.length);

        // Test passing qualifier without shadow cell suffix
        originalQualifierLength =
                CellUtils.qualifierLengthFromShadowCellQualifier(qualifier, 0, qualifier.length);
        assertEquals(originalQualifierLength, qualifier.length);
    }


    @Test(timeOut = 10_000)
    public void testmapCellsToShadowCellsCellOrder() {
        // Create the required data
        final byte[] validShadowCellQualifier =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier, SHADOW_CELL_SUFFIX);

        final byte[] qualifier2 = Bytes.toBytes("test-qual2");
        final byte[] validShadowCellQualifier2 =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier2, SHADOW_CELL_SUFFIX);

        final byte[] qualifier3 = Bytes.toBytes("test-qual3");
        final byte[] validShadowCellQualifier3 =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier3, SHADOW_CELL_SUFFIX);

        final byte[] qualifier4 = Bytes.toBytes("test-qual4");
        final byte[] qualifier5 = Bytes.toBytes("test-qual5");
        final byte[] validShadowCellQualifier5 =
                org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.concat(SHADOW_CELL_PREFIX, qualifier5, SHADOW_CELL_SUFFIX);


        Cell cell1 = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value")); // Default type is Put
        Cell shadowCell1 = new KeyValue(row, family, validShadowCellQualifier, 1, Bytes.toBytes("sc-value"));

        Cell cell2 = new KeyValue(row, family, qualifier2, 1, Bytes.toBytes("value2"));
        Cell shadowCell2 = new KeyValue(row, family, validShadowCellQualifier2, 1, Bytes.toBytes("sc-value2"));

        Cell cell3 = new KeyValue(row, family, qualifier3, 1, Bytes.toBytes("value3"));
        Cell shadowCell3 = new KeyValue(row, family, validShadowCellQualifier3, 1, Bytes.toBytes("sc-value2"));

        Cell cell4 = new KeyValue(row, family, qualifier4, 1, Bytes.toBytes("value4"));

        Cell shadowCell5 = new KeyValue(row, family, validShadowCellQualifier5, 1, Bytes.toBytes("sc-value2"));

        List<Cell> scanList = new ArrayList<>();
        scanList.add(shadowCell5);
        scanList.add(cell3);
        scanList.add(cell1);
        scanList.add(shadowCell1);
        scanList.add(shadowCell2);
        scanList.add(cell4);
        scanList.add(cell2);
        scanList.add(shadowCell3);
        scanList.add(shadowCell5);

        SortedMap<Cell, Optional<Cell>> cellsToShadowCells = CellUtils.mapCellsToShadowCells(scanList);
        assertEquals(cellsToShadowCells.get(cell1).get(), shadowCell1);
        assertEquals(cellsToShadowCells.get(cell2).get(), shadowCell2);
        assertEquals(cellsToShadowCells.get(cell3).get(), shadowCell3);
        assertFalse(cellsToShadowCells.get(cell4).isPresent());
    }

}
