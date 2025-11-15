import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseSparkProcess {

    public void createHbaseTable() {

        // Configuration HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-master");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase");
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Configuration Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[2]"); // TP: exécution locale

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Lecture HBase -> RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                config,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        long count = hBaseRDD.count();
        System.out.println("Nombre d'enregistrements dans HBase = " + count);

        jsc.close();
    }

    public static void main(String[] args) {
        new HbaseSparkProcess().createHbaseTable();
    }
}

