import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseSparkFilter {

    public static void main(String[] args) {

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, "products");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseFilter")
                .setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class
                );

        JavaRDD<Double> prices = hBaseRDD.map(tuple -> {
            byte[] v = tuple._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"));
            if (v == null) return 0.0;
            try {
                return Double.parseDouble(Bytes.toString(v).replace(",", ""));
            } catch (Exception e) {
                return 0.0;
            }
        });

        JavaRDD<Double> filtered = prices.filter(price -> price > 500);

        long count = filtered.count();
        System.out.println("==============");
        System.out.println("NOMBRE DE PRIX > 500 = " + count);
        System.out.println("==============");

        System.out.println("====== TOP 20 PRIX > 500 ======");
        filtered
                .sortBy(p -> p, false, 1)
                .take(20)
                .forEach(System.out::println);

        jsc.close();
    }
}

