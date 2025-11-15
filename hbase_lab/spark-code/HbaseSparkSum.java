import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;  // <<< IMPORT MANQUANT

import java.io.Serializable;

public class HbaseSparkSum implements Serializable {

    public static class PriceExtractor implements Function<Tuple2<ImmutableBytesWritable, Result>, Double>, Serializable {

        @Override
        public Double call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {

            Result r = tuple._2;

            byte[] priceBytes = r.getValue("cf".getBytes(), "price".getBytes());
            if (priceBytes == null) return 0.0;

            String price = new String(priceBytes)
                    .replace("$", "")
                    .replace(",", "")
                    .trim();

            try {
                return Double.parseDouble(price);
            } catch (Exception e) {
                return 0.0;
            }
        }
    }

    public void computeTotalSales() {

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, "products");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseSum")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class
                );

        JavaRDD<Double> pricesRDD = hBaseRDD.map(new PriceExtractor());

        double total = pricesRDD.reduce((a, b) -> a + b);

        System.out.println("\n==============================");
        System.out.println("TOTAL DES PRIX = " + total);
        System.out.println("==============================\n");

        jsc.close();
    }

    public static void main(String[] args) {
        new HbaseSparkSum().computeTotalSales();
    }
}

