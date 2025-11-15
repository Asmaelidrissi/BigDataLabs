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

        // Créer la configuration HBase
        Configuration config = HBaseConfiguration.create();

        // Créer la configuration Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[4]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Indiquer la table HBase à lire
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Créer un RDD depuis la table HBase
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class
                );

        // Afficher le nombre d'enregistrements
        System.out.println("Nombre d'enregistrements : " + hBaseRDD.count());
    }

    public static void main(String[] args) {
        HbaseSparkProcess admin = new HbaseSparkProcess();
        admin.createHbaseTable();
    }
}
