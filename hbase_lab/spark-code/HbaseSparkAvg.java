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

public class HbaseSparkAvg {

    public static void main(String[] args) {

        Configuration config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, "products");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseAvg")
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
            byte[] val = tuple._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"));
            if (val == null) return 0.0;
            try { return Double.parseDouble(Bytes.toString(val)); }
            catch (Exception e) { return 0.0; }
        });

        long count = prices.count();
        double sum = prices.reduce((a, b) -> a + b);

        double avg = sum / count;

        System.out.println("======= MOYENNE DES PRIX =======");
        System.out.println(avg);

        jsc.close();
    }
}

