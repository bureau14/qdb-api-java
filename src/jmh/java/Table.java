package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;

@State(Scope.Thread)
public class Table {
    public enum ValueType {
        INT64,
        DOUBLE,
        TIMESTAMP,
        BLOB_1,
        BLOB_256,
        BLOB_1024
    };


    @Param({"1", "10", "25", "100"})
    public int colCount;

    @Param({"INT64", "DOUBLE", "TIMESTAMP", "BLOB_1", "BLOB_256", "BLOB_1024"})
    public ValueType valueType;

    QdbColumnDefinition[] cols;
    QdbTimeSeriesRow[] rows;

    @Setup(Level.Trial)
    public void setup() {
        int complexity = 1;

        switch(this.valueType) {
        case DOUBLE:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.DOUBLE,
                                                     colCount);
            break;

        case INT64:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.INT64,
                                                     colCount);
            break;

        case TIMESTAMP:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.TIMESTAMP,
                                                     colCount);
            break;

        case BLOB_1:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.BLOB,
                                                     colCount);
            break;

        case BLOB_256:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.BLOB,
                                                     colCount);
            complexity = 256;
            break;

        case BLOB_1024:
            this.cols = Helpers.generateTableColumns(QdbTimeSeriesValue.Type.BLOB,
                                                     colCount);
            complexity = 256;
            break;
        }

        int rowCount = 1000000 / colCount;
        System.out.println("Generating " + rowCount + " rows and keeping structure in memory..");
        this.rows = Helpers.generateTableRows(this.cols, complexity, rowCount);
    }
}
