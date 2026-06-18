package org.apache.paimon.service.example;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

public class PaimonMinmaxTestGenerator {

    private static final String DB_NAME    = "tests";
    private static final String TABLE_NAME = "paimon_minmax_test";

    // DECIMAL(20, 0): precision > 18, so Paimon stores it as variable-width signed
    // two's-complement bytes, exercising the sign-extension path in BinaryRow::getDecimal.
    private static final int DEC_PRECISION = 20;
    private static final int DEC_SCALE     = 0;

    public static void main(String[] args) throws Exception {
        // Default output path; override via first CLI arg if needed.
        String rootPath = args.length > 0 ? args[0] : "/tmp/warehouse";
        generate(rootPath);
    }

    public static void generate(String rootPath) throws Exception {
        // ── 1. Build schema ──────────────────────────────────────────────────
        // The first five columns reproduce the original dataset (so the existing
        // queries keep their results). The remaining columns each target a
        // correctness guard in the C++ min/max pruner:
        //   big_val  BIGINT          → BinaryRow::getLong must read Int64 (values > 2^31).
        //   dec_val  DECIMAL(20, 0)  → BinaryRow::getDecimal must sign-extend negatives.
        //   nul_val  INT (nullable)  → a file with non-zero _NULL_COUNTS must not be pruned.
        //   ts6      TIMESTAMP(6)    → scale > 3 stats must be skipped (no exception).
        //   bin_val  BYTES           → unsupported stats type must be skipped (no exception).
        Schema schema = Schema.newBuilder()
                .column("id",      DataTypes.INT().notNull())
                .column("int_val", DataTypes.INT().notNull())
                .column("str_val", DataTypes.STRING().notNull())
                .column("ts3",     DataTypes.TIMESTAMP(3).notNull())
                .column("ts1",     DataTypes.TIMESTAMP(1).notNull())
                .column("big_val", DataTypes.BIGINT().notNull())
                .column("dec_val", DataTypes.DECIMAL(DEC_PRECISION, DEC_SCALE).notNull())
                .column("nul_val", DataTypes.INT())
                .column("ts6",     DataTypes.TIMESTAMP(6).notNull())
                .column("bin_val", DataTypes.BYTES().notNull())
                // "fields-*" = dense stats: record min/max for every column.
                // This populates _VALUE_STATS_COLS in the manifest files so
                // that readers can skip data files whose [min, max] range does
                // not overlap with the query predicate.
                .option("metadata.stats-store", "fields-*")
                .build();

        // ── 2. Create catalog & table ────────────────────────────────────────
        Catalog catalog = createCatalog(rootPath);
        try {
            catalog.createDatabase(DB_NAME, /*ignoreIfExists=*/ true);
        } catch (Catalog.DatabaseAlreadyExistException ignored) {}

        Identifier tableId = Identifier.create(DB_NAME, TABLE_NAME);
        try {
            catalog.createTable(tableId, schema, /*ignoreIfExists=*/ false);
        } catch (Catalog.TableAlreadyExistException ignored) {
            System.out.println("Table already exists, reusing: " + tableId);
        }

        Table table = catalog.getTable(tableId);

        // ── 3. Write 3 independent batches ───────────────────────────────────
        // Each batch uses its own BatchWriteBuilder → newWrite → prepareCommit
        // → commit cycle, which guarantees a separate data file per batch.

        // Batch 1: int_val [10, 30], ts [2024-01-01 00:00 .. 12:00],
        //          big_val ~5e9, dec_val ~-9e18, nul_val all non-null.
        writeBatch(table, rootPath,
                new int[]    {  1,   2,   3 },
                new int[]    { 10,  20,  30 },
                new String[] { "a", "b", "c" },
                new LocalDateTime[] {
                        LocalDateTime.of(2024, 1, 1,  0,  0,  0),
                        LocalDateTime.of(2024, 1, 1,  6,  0,  0),
                        LocalDateTime.of(2024, 1, 1, 12,  0,  0),
                },
                new long[]   { 5000000001L, 5000000002L, 5000000003L },
                new BigDecimal[] {
                        new BigDecimal("-9000000000000000003"),
                        new BigDecimal("-9000000000000000002"),
                        new BigDecimal("-9000000000000000001"),
                },
                new Integer[] { 100, 200, 300 },
                new LocalDateTime[] {
                        LocalDateTime.of(2024, 1, 1,  0,  0,  0,   1000),
                        LocalDateTime.of(2024, 1, 1,  6,  0,  0,   2000),
                        LocalDateTime.of(2024, 1, 1, 12,  0,  0,   3000),
                },
                new String[] { "aaa1", "aaa2", "aaa3" });

        // Batch 2: int_val [110, 130], ts [2024-06-15 08:00 .. 20:00],
        //          big_val ~6e9, dec_val ~-5e18, nul_val has one NULL (id=4).
        writeBatch(table, rootPath,
                new int[]    {   4,    5,    6 },
                new int[]    { 110,  120,  130 },
                new String[] { "d", "e", "f" },
                new LocalDateTime[] {
                        LocalDateTime.of(2024, 6, 15,  8,  0,  0),
                        LocalDateTime.of(2024, 6, 15, 14, 30,  0),
                        LocalDateTime.of(2024, 6, 15, 20,  0,  0),
                },
                new long[]   { 6000000001L, 6000000002L, 6000000003L },
                new BigDecimal[] {
                        new BigDecimal("-5000000000000000003"),
                        new BigDecimal("-5000000000000000002"),
                        new BigDecimal("-5000000000000000001"),
                },
                new Integer[] { null, 500, 600 },
                new LocalDateTime[] {
                        LocalDateTime.of(2024, 6, 15,  8,  0,  0,   4000),
                        LocalDateTime.of(2024, 6, 15, 14, 30,  0,   5000),
                        LocalDateTime.of(2024, 6, 15, 20,  0,  0,   6000),
                },
                new String[] { "bbb1", "bbb2", "bbb3" });

        // Batch 3: int_val [210, 230], ts [2025-01-01 00:00 .. 23:59:59],
        //          big_val ~7e9, dec_val ~-1e18, nul_val all non-null.
        writeBatch(table, rootPath,
                new int[]    {   7,    8,    9 },
                new int[]    { 210,  220,  230 },
                new String[] { "g", "h", "i" },
                new LocalDateTime[] {
                        LocalDateTime.of(2025, 1, 1,  0,  0,  0),
                        LocalDateTime.of(2025, 1, 1, 12,  0,  0),
                        LocalDateTime.of(2025, 1, 1, 23, 59, 59),
                },
                new long[]   { 7000000001L, 7000000002L, 7000000003L },
                new BigDecimal[] {
                        new BigDecimal("-1000000000000000003"),
                        new BigDecimal("-1000000000000000002"),
                        new BigDecimal("-1000000000000000001"),
                },
                new Integer[] { 700, 800, 900 },
                new LocalDateTime[] {
                        LocalDateTime.of(2025, 1, 1,  0,  0,  0,   7000),
                        LocalDateTime.of(2025, 1, 1, 12,  0,  0,   8000),
                        LocalDateTime.of(2025, 1, 1, 23, 59, 59,   9000),
                },
                new String[] { "ccc1", "ccc2", "ccc3" });

        System.out.println("Done. Paimon table written to: " + rootPath
                + "/" + DB_NAME + "/" + TABLE_NAME);
    }

    /**
     * Writes one batch of rows and commits them as a single snapshot.
     * Each call produces exactly one new data file.
     */
    private static void writeBatch(
            Table table, String ioTmpPath,
            int[] ids, int[] intVals, String[] strVals, LocalDateTime[] timestamps,
            long[] bigVals, BigDecimal[] decVals, Integer[] nulVals,
            LocalDateTime[] ts6Vals, String[] binVals) throws Exception {

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        TableWriteImpl writer = (TableWriteImpl) writeBuilder.newWrite()
                .withIOManager(new IOManagerImpl(ioTmpPath));
        try {
            for (int i = 0; i < ids.length; i++) {
                // GenericRow field order must match schema:
                // id, int_val, str_val, ts3, ts1, big_val, dec_val, nul_val, ts6, bin_val
                GenericRow row = new GenericRow(10);
                row.setField(0, ids[i]);
                row.setField(1, intVals[i]);
                row.setField(2, BinaryString.fromString(strVals[i]));
                row.setField(3, Timestamp.fromLocalDateTime(timestamps[i]));
                row.setField(4, Timestamp.fromLocalDateTime(timestamps[i]));
                row.setField(5, bigVals[i]);
                row.setField(6, Decimal.fromBigDecimal(decVals[i], DEC_PRECISION, DEC_SCALE));
                row.setField(7, nulVals[i]); // may be null
                row.setField(8, Timestamp.fromLocalDateTime(ts6Vals[i]));
                row.setField(9, binVals[i].getBytes(StandardCharsets.UTF_8));
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();

            BatchTableCommit commit = writeBuilder.newCommit();
            try {
                commit.commit(messages);
                System.out.printf("  committed batch: id=[%d..%d], int_val=[%d..%d]%n",
                        ids[0], ids[ids.length - 1],
                        intVals[0], intVals[intVals.length - 1]);
            } finally {
                commit.close();
            }
        } finally {
            writer.close();
        }
    }

    private static Catalog createCatalog(String rootPath) {
        CatalogContext context = CatalogContext.create(new Path(rootPath));
        return CatalogFactory.createCatalog(context);
    }
}
