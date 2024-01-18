package com.flink.examples.datasetapi;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Date;

public class PostgresSourceSink {
    public static void main(String[] args) throws Exception {
        // Getting the Execution environment
        final ExecutionEnvironment eEnv = ExecutionEnvironment.getExecutionEnvironment();

        final DataSet<Row> orderRecords
                = eEnv.createInput(
                getJDBC(
                        "select order_id, order_date from orders",
                        new RowTypeInfo(
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.DATE_TYPE_INFO)
                )
        );

        final DataSet<Row> itemsRecords
                = eEnv.createInput(
                getJDBC(
                        "select item_id, unit_price from items",
                        new RowTypeInfo(
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.DOUBLE_TYPE_INFO)
                )
        );

        final DataSet<Row> orderItemsRecords
                = eEnv.createInput(
                getJDBC(
                        "select order_id, item_id, quantity from order_items",
                        new RowTypeInfo(
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.LONG_TYPE_INFO,
                                BasicTypeInfo.LONG_TYPE_INFO
                        )
                )
        );

        orderRecords.first(5).print();
        System.out.println();
        itemsRecords.first(5).print();
        System.out.println();
        orderItemsRecords.first(5).print();
        System.out.println();

        final DataSet<Tuple3<Long, Long, Double>> orderItemsJoinedRecords =
                orderItemsRecords
                        .join(itemsRecords)
                        .where(1)
                        .equalTo(0)
                        .with(new JoinFunction<Row, Row, Tuple3<Long, Long, Double>>() {
                            @Override
                            public Tuple3<Long, Long, Double> join(Row first, Row second) throws Exception {
                                return new Tuple3<>(
                                        (Long) first.getField(0),
                                        (Long) first.getField(1),
                                        (Long) first.getField(2) * (Double) second.getField(1)
                                );
                            }
                        });

        orderItemsJoinedRecords.first(5).print();
        System.out.println();

        final DataSet<Tuple3<Long, Long, Double>> sumGroupingBy
                = orderItemsJoinedRecords.groupBy(0).sum(2);

        sumGroupingBy.first(5).print();
        System.out.println();

        /*final DataSet<Row> summaryResult = sumGroupingBy.map(new MapFunction<Tuple3<Long, Long, Double>, Row>() {
            @Override
            public Row map(Tuple3<Long, Long, Double> value) throws Exception {
                return Row.of(value.f0, value.f1);
            }
        });

        summaryResult.print();
        System.out.println();*/
    }

    private static JDBCInputFormat getJDBC(String query, RowTypeInfo rowInfo) {
        return JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.postgresql.Driver")
                .setDBUrl("jdbc:postgresql://localhost:5432/test_db")
                .setUsername("postgres")
                .setPassword("postgres")
                .setQuery(query)
                .setRowTypeInfo(rowInfo)
                .finish();
    }
}
