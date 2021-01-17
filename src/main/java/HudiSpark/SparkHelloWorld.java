package HudiSpark;

import org.apache.commons.io.FileUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkHelloWorld {

    public static  final String basePath = "/home/brijesh/Downloads/";
    private static List<Album> INITIAL_ALBUM_DATA = null;

    static {
        try {
            INITIAL_ALBUM_DATA = new ArrayList<>(Arrays.asList(
                    new Album(800, "6 String Theory", Arrays.asList("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01")),
                    new Album(801, "Hail to the Thief", Arrays.asList("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
                    new Album(801, "Hail to the Thief", Arrays.asList("2+2=5", "Backdrifts", "Go to sleep"), dateToLong("2019-12-03"))
            ));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("hudi-datalake")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .getOrCreate();

        String tableName = "Album";
        String tableKey = "albumId";
        clearDirectory(tableName);
        upsert(spark.createDataset(INITIAL_ALBUM_DATA, Encoders.bean(Album.class)), tableName, tableKey, "updateDate");
        snapshotQuery(spark, tableName);
    }

    private static void upsert (Dataset dataset, String tableName, String key, String combineKey) {
        dataset.write()
                .format("hudi")
                .option(DataSourceWriteOptions.TABLE_NAME_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), key)
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), combineKey)
                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())
                .option("hoodie.upsert.shuffle.parallelism", "2")
                .mode(SaveMode.Append)
                .save(basePath.concat(tableName).concat("/"));
    }

    private static Long dateToLong(String str) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd").parse(str).getTime();
    }

    private static void snapshotQuery(SparkSession spark, String tableName) {
        spark.read().format("hudi").load(basePath.concat(tableName).concat("/*")).show();
    }

    private static void clearDirectory(String folder) throws IOException {
        File file = new File(basePath.concat(folder));
        FileUtils.deleteDirectory(file);
    }

}
