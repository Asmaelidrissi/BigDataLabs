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
import scala.Tuple2;

public class HbaseSparkTop10 {

    public static void main(String[] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, "products");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTop10")
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
            Result r = tuple._2;
            byte[] value = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"));
            if (value == null) return 0.0;
            try {
                return Double.parseDouble(Bytes.toString(value));
            } catch (Exception e) {
                return 0.0;
            }
        });

        // Top 10
        java.util.List<Double> top10 = prices.top(10);

        System.out.println("======= TOP 10 PRIX =======");
        for (Double d : top10) {
            System.out.println(d);
        }

        jsc.close();
    }
}

