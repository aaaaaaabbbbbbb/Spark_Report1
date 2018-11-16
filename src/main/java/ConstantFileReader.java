import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * ファイル関連の定数を定義する
 *
 * @author TOSHI.I
 */
public class ConstantFileReader {

    /** 読み込むファイルの内部定義値：タイトル **/
    public static final int INTERNALVAL_FILE_TITLEBASICS = 0;
    /** 読み込むファイルの内部定義値：キャスト **/
    public static final int INTERNALVAL_FILE_NAMEBASICS = 1;
    /** 読み込むファイルの内部定義値：プリンシパル **/
    public static final int INTERNALVAL_FILE_TITLEPRINCIPALS = 2;
    /** 読み込むファイルの内部定義値：レーティング **/
    public static final int INTERNALVAL_FILE_TITLERATING = 3;

    /** プリンシパルのデータ構造：プリンシパル **/
    private static final StructType ST_TITLEPRINCIPALS = new StructType(new StructField[] {
            new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
            new StructField("ordering", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("nconst", DataTypes.StringType, false, Metadata.empty()),
            new StructField("category", DataTypes.StringType, false, Metadata.empty()),
            new StructField("job", DataTypes.StringType, false, Metadata.empty()),
            new StructField("characters", DataTypes.StringType, false, Metadata.empty()),
    });

    /** レーティングのデータ構造：レーティング **/
    private static final StructType ST_TITLERATING = new StructType(new StructField[] {
            new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
            new StructField("agerageRating", DataTypes.FloatType, false, Metadata.empty()),
            new StructField("numVotes", DataTypes.IntegerType, false, Metadata.empty())
    });

    /** キャストのデータ構造：キャスト **/
    private static final StructType ST_NAMEBASICS = new StructType(new StructField[] {
            new StructField("nconst", DataTypes.StringType, false, Metadata.empty()),
            new StructField("primaryName", DataTypes.StringType, false, Metadata.empty())
    });

    /** キャストのデータ構造：タイトル **/
    private static final StructType ST_TITLEBASICS = new StructType(new StructField[] {
            new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
            new StructField("titleType", DataTypes.StringType, false, Metadata.empty()),
            new StructField("primaryTitle", DataTypes.StringType, false, Metadata.empty())
    });


    /** ファイルパス：メインのテーブル**/
    private static final String FILEPATH_TITLEBASIC = "hdfs:/IMDB_data/title.basics.tsv";
    /** ファイルパス：メインのテーブル**/
    private static final String FILEPATH_TITLEPRINCIPALS = "hdfs:/IMDB_data/title.principals.tsv";
    /** ファイルパス：キャストのテーブル**/
    private static final String FILEPATH_NAMEBASICS = "hdfs:/IMDB_data/name.basics.tsv";
    /** ファイルパス：レーティングのテーブル**/
    private static final String FILEPATH_TITLERATING = "hdfs:/IMDB_data/title.ratings.tsv";

    /** インデックス：内部値とファイルパスとデータ構造のテーブル ：内部値 **/
    private static final int INDEX_INTERNALPATHSTTABLE_INTERNAL = 0;
    /** インデックス：内部値とファイルパスとデータ構造のテーブル ：データ構造 **/
    private static final int INDEX_INTERNALPATHSTTABLE_STRUCTFIELD = 1;
    /** インデックス：内部値とファイルパスとデータ構造のテーブル ：ファイルパス **/
    private static final int INDEX_INTERNALPATHSTTABLE__FILEPATH = 2;

    /** 内部値とファイルパスとデータ構造のテーブル **/
    private static Object[][] TABLE_INTERNALPATHST = {
            {INTERNALVAL_FILE_TITLEBASICS, ST_TITLEBASICS, FILEPATH_TITLEBASIC},
            {INTERNALVAL_FILE_NAMEBASICS, ST_NAMEBASICS, FILEPATH_NAMEBASICS},
            {INTERNALVAL_FILE_TITLEPRINCIPALS, ST_TITLEPRINCIPALS, FILEPATH_TITLEPRINCIPALS},
            {INTERNALVAL_FILE_TITLERATING, ST_TITLERATING, FILEPATH_TITLERATING},
    };

    /**
     * データのファイルを読み込む
     *
     * @param _internal 読み込むファイルの内部値
     * @param _spark SparkSession
     * @return 読み込んだデータ
     */
    public static  Dataset<Row> readFile(int _internal, SparkSession _spark){
        Object[] fileInfo  = getTableValue(_internal);
        if(fileInfo == null){
            return null;
        }
        //tsvファイルを読み込む
        Dataset<Row> data = _spark.read().option("delimiter", "\t").option("header", "true").schema((StructType)fileInfo[INDEX_INTERNALPATHSTTABLE_STRUCTFIELD]).csv((String)fileInfo[INDEX_INTERNALPATHSTTABLE__FILEPATH]);
        return data;
    }

    /**
     * 指定内部値に対応するデータ構造とファイルパスとを習得する
     *
     * @param _internal 指定内部値
     * @return 指定内部値、データ構造、ファイルパスの配列
     */
    private static  Object[] getTableValue(int _internal){
        for(int i = 0; i < TABLE_INTERNALPATHST.length; i++){
            if(_internal == ((Integer)TABLE_INTERNALPATHST[i][INDEX_INTERNALPATHSTTABLE_INTERNAL]).intValue()){
                return TABLE_INTERNALPATHST[i];
            }
        }
        return null;
    }
}
