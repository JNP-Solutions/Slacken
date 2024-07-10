/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers;
import it.unimi.dsi.fastutil.longs.LongArrays;

import java.util.Random;

public class SortTest {

    public static long[][] makeData(int items, int itemSize) {
        Random rand = new Random(1234);
        long[][] r = new long[itemSize][items];
        for (int i = 0; i < itemSize; i++) {
            for (int j = 0; j < items; j++) {
                r[i][j] = rand.nextLong();
            }
        }
        return r;
    }

    public static long[][] makeData2(int items, int itemSize) {
        Random rand = new Random(1234);
        long[][] r = new long[itemSize][items];
        for (int i = 0; i < itemSize; i++) {
            for (int j = 0; j < items; j++) {
                if (i == 0) {
                    r[i][j] = 0;
                } else {
                    r[i][j] = j < items / 2 ? 0 : rand.nextLong();
                }
            }
        }
        return r;
    }

    final static long mask = (0xffffL << 48) | (0xffffffffL);
    public static long[][] makeData3(int items, int itemSize) {
        Random rand = new Random(1234);
        long[][] r = new long[itemSize][items];
        for (int i = 0; i < itemSize; i++) {
            for (int j = 0; j < items; j++) {
                r[i][j] = rand.nextLong() & mask;
            }
        }
        return r;
    }

    /**
     * Performance test program for lexicographic sort of arrays of arrays using Fastutil.
     *
     * Tested with these parameters: 10000000 2 5
     * The time for sorting converges to about 1500 ms for RADIXSORT_NO_REC = 1024,
     * and to 600 ms for RADIXSORT_NO_REC = 16.
     * @param args
     */
    public static void main(String[] args) {
//        int numItems = Integer.parseInt(args[0]);
        int itemWidth = Integer.parseInt(args[1]);
        int numTimes = Integer.parseInt(args[2]);

        int[] sizes = new int[] { 10000000, 100000000, 500000000 };

        for (int numItems: sizes) {
            System.out.println(String.format("Testing with data of size %d x %d", numItems,
                    itemWidth));

            for (int run = 0; run < numTimes; run++) {
                long[][] data = makeData(numItems, itemWidth);
                long start = System.currentTimeMillis();
                LongArrays.radixSort(data);
                long stop = System.currentTimeMillis();
                System.out.printf("Time elapsed (data 1): %d ms", (stop - start));

                data = null;
                data = makeData2(numItems, itemWidth);
                start = System.currentTimeMillis();
                LongArrays.radixSort(data);
                stop = System.currentTimeMillis();
                System.out.printf("Time elapsed (data 2): %d ms", (stop - start));
                data = null;

                data = makeData3(numItems, itemWidth);
                start = System.currentTimeMillis();
                LongArrays.radixSort(data);
                stop = System.currentTimeMillis();
                System.out.printf("Time elapsed (data 3): %d ms", (stop - start));
                data = null;
            }
        }
    }
}