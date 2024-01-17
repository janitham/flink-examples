package com.flink.examples.datasetapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class Transformation {
    public static void main(String[] args) {
        try {
            // Getting the Execution environment
            final ExecutionEnvironment eEnv = ExecutionEnvironment.getExecutionEnvironment();

            //ID,Customer,Product,Date,Quantity,Rate,Tags
            final DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> dataset =
                    eEnv.readCsvFile("src/main/resources/sales_orders.csv")
                            .ignoreFirstLine()
                            .parseQuotedStrings('\"')
                            .types(Integer.class, String.class, String.class, String.class,
                                    Integer.class, Double.class, String.class);
            // Top 5
            dataset.first(5).print();
            System.out.println();

            // Order Value Computing
            final MapFunction<
                    Tuple7<Integer, String, String, String, Integer, Double, String>,
                    Tuple8<Integer, String, String, String, Integer, Double, String, Double>> computeTotalFn
                    = new MapFunction<
                    Tuple7<Integer, String, String, String, Integer, Double, String>,
                    Tuple8<Integer, String, String, String, Integer, Double, String, Double>
                    >() {
                @Override
                public Tuple8<Integer, String, String, String, Integer, Double, String, Double> map(
                        Tuple7<Integer, String, String, String, Integer, Double, String> order) throws Exception {
                    return new Tuple8<>(
                            order.f0, order.f1, order.f2, order.f3, order.f4, order.f5, order.f6, (order.f4 * order.f5));
                }
            };

            DataSet<Tuple8<Integer, String, String, String, Integer, Double, String, Double>> computedValue
                    = dataset.map(computeTotalFn);

            // Top 5
            computedValue.first(5).print();
            System.out.println();

            // Customer Tags Computing
            final FlatMapFunction<
                    Tuple7<Integer, String, String, String, Integer, Double, String>,
                    Tuple2<Integer, String>> computeTags
                    = new FlatMapFunction<
                    Tuple7<Integer, String, String, String, Integer, Double, String>, Tuple2<Integer, String>
                    >() {
                @Override
                public void flatMap(Tuple7<Integer, String, String, String, Integer, Double, String> order,
                                    Collector<Tuple2<Integer, String>> collector) {
                    final Integer customer = order.f0;
                    final String tags = order.f6;
                    Arrays.stream(tags.split(":"))
                            .map(tag -> new Tuple2<>(customer, tag))
                            .forEach(collector::collect);
                }
            };

            final DataSet<Tuple2<Integer, String>> filteredTags = dataset.flatMap(computeTags);
            filteredTags.first(5).print();
            System.out.println();

            // Filter orders by date
            final FilterFunction<Tuple7<Integer, String, String, String, Integer, Double, String>> filterFunction
                    = new FilterFunction<Tuple7<Integer, String, String, String, Integer, Double, String>>() {
                @Override
                public boolean filter(Tuple7<Integer, String, String, String, Integer, Double, String> order) {
                    final LocalDate orderDate = LocalDate.parse(order.f3, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
                    final LocalDate firstDate = LocalDate.of(2019, 11, 1);
                    final LocalDate lastDate = LocalDate.of(2019, 11, 11);
                    return orderDate.isAfter(firstDate) && orderDate.isBefore(lastDate);
                }
            };

            final DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> filteredData
                    = dataset.filter(filterFunction);
            filteredData.first(5).print();

        } catch (Exception e) {

        }
    }
}
