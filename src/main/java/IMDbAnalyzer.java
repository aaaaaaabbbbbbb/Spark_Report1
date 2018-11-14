import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;

import static org.apache.spark.sql.functions.*;

/**
 * IMDBファイルに指定の分析を行い、結果を表示する。
 *
 * @author TOSHI.I
 */
public class IMDbAnalyzer {

    /** 解析対象：全部 **/
    private static final String PARAMVAL_TARGET_ALLTITLE = "a";
    /** 解析対象：シリーズ **/
    private static final String PARAMVAL_TARGET_SERIES = "f";
    /** 解析対象：エピソード **/
    private static final String PARAMVAL_TARGET_EPISODE = "e";
    /** 解析対象：映画 **/
    private static final String PARAMVAL_TARGET_MOVIE = "m";
    /** 解析対象：ショート **/
    private static final String PARAMVAL_TARGET_SHORT = "s";

    /** 分析内容：キャスト **/
    private static final String PARAMVAL_ANALYSE_CASTAPPEARANCE = "c";
    /** 分析内容：プロデューサ **/
    private static final String PARAMVAL_ANALYSE_PRODUCERAPPEARANCE = "p";
    /** 分析内容：プロデューサ **/
    private static final String PARAMVAL_ANALYSE_DIRECTORAPPEARANCE = "d";
    /** 分析内容：単語頻度分析 **/
    private static final String PARAMVAL_ANALYSE_WORDFREQUENCY = "w";
    /** 分析内容：評価 **/
    private static final String PARAMVAL_ANALYSE_RATING = "r";

    /** 表示順：少ない/低い順 **/
    private static final String PARAMVAL_ANALYSE_ASCENDANT = "a";

    /** ファイルパス：メインのテーブル**/
    private static final String FILEPATH_TITLEBASIC = "hdfs:/IMDB_data/title.basics.tsv";
    /** ファイルパス：メインのテーブル**/
    private static final String FILEPATH_TITLEPRINCIPALS = "hdfs:/IMDB_data/title.principals.tsv";
    /** ファイルパス：キャストのテーブル**/
    private static final String FILEPATH_NAMEBASICS = "hdfs:/IMDB_data/name.basics.tsv";
    /** ファイルパス：レーティングのテーブル**/
    private static final String FILEPATH_TITLERATING = "hdfs:/IMDB_data/title.ratings.tsv";
    /** ファイルパス：エピソードのテーブル**/
    private static final String FILEPATH_TITLEEPISODE = "hdfs:/IMDB_data/title.episode.tsv";

    /** スパークセッション **/
    private static SparkSession spark;

    /**
     * <p>
     * 引数は
     * 1つ目が対象のデータ(a：全データ、f：シリーズ、e：エピソード、m：映画、s：ショート)
     * 2つ目が分析内容(w：単語出現頻度、c：キャスト出演数、p：プロデューサ担当数、d：ディレクタ担当数、r：レーティング)
     * 3つ目は結果表示の個数。無指定の場合、デフォルトを使用する。
     * 4つ目が表示内容(d：多い順(デフォルト)、a：小さい順)
     * <p>
     * 演習の各設問の結果は以下の指定で得る。
     * "a w [30] [d]":(1)全データの単語の頻度
     * "f c [10] [d]":(2)多くのシリーズに出演している俳優
     * "e c [10] [d]":(3)多くのエピソードに出演している俳優
     * "m c [10] [d]":(4)多くの映画に出演している俳優
     * <p>
     * ex: spark-submit IMDbAnalyzer-0.1.jar a w
     */
    public static void main(String[] args) {
        //SparkSession
        spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        if (args == null || args.length < 2) {
            //パラメータ不正
            System.err.println("Error: Invalid number of parameters.");
            return;
        }

        //要素の表示数を習得する
        int shownum = getShowNum(args);

        //結果表示の順番。trueで昇順
        boolean isAscendant = getResultOrder(args);

        //ターゲットデータの取得
        Dataset<Row> target = readTargetData(args[0]);
        if (target == null) {
            System.err.println("Error: Invalid parameter.");
            return;
        }

        //解析の実行
        Dataset<Row> analysed = analyseData(args[1], target);
        if (analysed == null) {
            System.err.println("Error: Invalid parameter.");
            return;
        }

        //結果を表示
        showResult(analysed, shownum, isAscendant);

        //SparkSessionをクローズ
        spark.close();
    }

    /**
     * 指定されたターゲットのデータを読み込む
     *
     * @param _targetSpecified ターゲットに指定された文字列
     * @return ターゲットデータ
     */
    private static Dataset<Row> readTargetData(String _targetSpecified) {

        //ターゲットデータ
        Dataset<Row> target = null;

        //TitleBasicsを読み込む
        Dataset<Row> title = readTitleBasics();

        //対象データ読み込み
        if (_targetSpecified.equals(PARAMVAL_TARGET_ALLTITLE)) {
            //すべて使う
            target = title;
        } else if (_targetSpecified.equals(PARAMVAL_TARGET_SERIES)) {
            //シリーズを抜き出す
            target = title.filter(col("titleType").eqNullSafe("tvSeries"));
        } else if (_targetSpecified.equals(PARAMVAL_TARGET_EPISODE)) {
            //TVエピソードを抜き出す
            target = title.filter(col("titleType").eqNullSafe("tvEpisode"));
        } else if (_targetSpecified.equals(PARAMVAL_TARGET_MOVIE)) {
            //映画を抜き出す
            target = title.filter(col("titleType").eqNullSafe("movie"));
        } else if (_targetSpecified.equals(PARAMVAL_TARGET_SHORT)) {
            //ショートを抜き出す
            target = title.filter(col("titleType").eqNullSafe("short"));
        } else {
            //パラメータ不正
            return null;
        }

        return target;
    }

    /**
     * 指定された解析を行う
     *
     * @param _analyzationSpecified 解析処理に指定された文字列文字列
     * @param _target               解析対象のデータ
     * @return 解析実行の結果として得たデータ
     */
    private static Dataset<Row> analyseData(String _analyzationSpecified, Dataset<Row> _target) {

        //ターゲットデータ
        Dataset<Row> analysed = null;

        //解析対象
        if (_analyzationSpecified.equals(PARAMVAL_ANALYSE_WORDFREQUENCY)) {
            //単語頻度分析
            //単語分解
            Dataset<Row> breaked = breakIntoWordsData(_target, "primaryTitle");
            //頻度解析
            analysed = analyseFrequencyInArray(breaked, "words");
        } else if (_analyzationSpecified.equals(PARAMVAL_ANALYSE_RATING)) {
            //評価取得
            //レーティングを読み込む
            Dataset<Row> rating = readTitleRating();
            //ターゲットとマージする
            Dataset<Row> castprintitle = rating.join(_target, rating.col("tconst").equalTo(_target.col("tconst")), "inner").drop(_target.col("tconst")).drop(_target.col("titleType"));
            //評価を表示時の並べ替え対象データのカラム名に変更する
            analysed = castprintitle.withColumnRenamed("agerageRating", "data").drop(castprintitle.col("tconst"));
        } else {
            //作品数分析
            String category = null;
            if (_analyzationSpecified.equals(PARAMVAL_ANALYSE_CASTAPPEARANCE)) {
                category = "actor";
            } else if (_analyzationSpecified.equals(PARAMVAL_ANALYSE_PRODUCERAPPEARANCE)) {
                category = "producer";
            } else if (_analyzationSpecified.equals(PARAMVAL_ANALYSE_DIRECTORAPPEARANCE)) {
                category = "director";
            } else {
                //パラメータ不正
                return null;
            }

            //キャストを読み込む
            Dataset<Row> castdata = readNameBasics();
            //プリンシパルを読み込み、対象のカテゴリーのみ残す。
            Dataset<Row> principal = readTitlePrincipals().filter(new Column("category").eqNullSafe(category));
            //キャストとプリンシパルをマージする
            Dataset<Row> castprintitle = castdata.join(principal, principal.col("nconst").equalTo(castdata.col("nconst")), "inner").drop(castdata.col("nconst"));
            //ターゲットとマージする
            Dataset<Row> casttarget = _target.join(castprintitle, _target.col("tconst").equalTo(castprintitle.col("tconst")), "inner").drop(castprintitle.col("tconst"));
            //必要な行だけ抜き出す
            Dataset<Row> data = casttarget.select("nconst", "primaryName");
            //頻度解析
            Dataset<Row> temp1 = analyseFrequencyString(data, "nconst");
            //表示用データを作る
            Dataset<Row> temp2 = data.dropDuplicates("nconst");
            analysed = temp1.join(temp2, temp1.col("nconst").equalTo(temp2.col("nconst")), "inner").select("data", "primaryName");
        }

        return analysed;
    }

    /**
     * プリンシパスを読み込む
     *
     * @return 読み込んだデータ
     */
    private static Dataset<Row> readTitlePrincipals() {
        //tsvファイルの構造
        StructType schema = new StructType(new StructField[] {
                new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("ordering", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("nconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty()),
                new StructField("job", DataTypes.StringType, false, Metadata.empty()),
                new StructField("characters", DataTypes.StringType, false, Metadata.empty()),
        });

        //tsvファイルを読み込む
        Dataset<Row> data = spark.read().schema(schema).format("csv").option("sep", "\t").option("header", "true").load(FILEPATH_TITLEPRINCIPALS);

        return data;
    }

    /**
     * レーティングを読み込む
     *
     * @return 読み込んだデータ
     */
    private static Dataset<Row> readTitleRating() {
        //tsvファイルの構造
        StructType schema = new StructType(new StructField[] {
                new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("agerageRating", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("numVotes", DataTypes.IntegerType, false, Metadata.empty())
        });

        //tsvファイルを読み込む
        Dataset<Row> data = spark.read().schema(schema).format("csv").option("sep", "\t").option("header", "true").load(FILEPATH_TITLERATING);
        return data;
    }

    /**
     * キャストを読み込む
     *
     * @return 読み込んだデータ
     */
    private static Dataset<Row> readNameBasics() {
        //tsvファイルの構造
        StructType schema = new StructType(new StructField[] {
                new StructField("nconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("primaryName", DataTypes.StringType, false, Metadata.empty())
        });

        //tsvファイルを読み込む
        Dataset<Row> data = spark.read().schema(schema).format("csv").option("sep", "\t").option("header", "true").load(FILEPATH_NAMEBASICS);
        return data;
    }

    /**
     * エピソードを読み込む
     *
     * @return 読み込んだデータ
     */
    private static Dataset<Row> readTitleEpisode() {
        //tsvファイルの構造
        StructType schema = new StructType(new StructField[] {
                new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("parentTconst", DataTypes.StringType, false, Metadata.empty())
        });

        //tsvファイルを読み込む
        Dataset<Row> data = spark.read().option("delimiter", "\t").option("header", "true").schema(schema).csv(FILEPATH_TITLEEPISODE);
        return data;
    }

    /**
     * タイトルを読み込む
     *
     * @return 読み込んだデータ
     */
    private static Dataset<Row> readTitleBasics() {

        //tsvファイルの構造
        StructType schema = new StructType(new StructField[] {
                new StructField("tconst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("titleType", DataTypes.StringType, false, Metadata.empty()),
                new StructField("primaryTitle", DataTypes.StringType, false, Metadata.empty())
        });

        //tsvファイルを読み込む
        Dataset<Row> data = spark.read().option("delimiter", "\t").option("header", "true").schema(schema).csv(FILEPATH_TITLEBASIC);
        return data;
    }

    /**
     * 出現頻度を調べる
     * 出現頻度はカラム"data"に格納される
     *
     * @param _data 読み込んだデータ
     * @param _key  解析対象のカラム名
     * @return 重複をマージして出現頻度をカラム"data"に格納したもの
     */
    private static Dataset<Row> analyseFrequencyInArray(Dataset<Row> _data, String _key) {

        Dataset<Row> wordsData2 = _data.withColumn(_key, explode(col(_key)))
                .groupBy(_key)
                .agg(count("*").as("data"));

        return wordsData2.select(col("data"), col(_key));
    }

    /**
     * 出現頻度を調べる
     * 出現頻度はカラム"data"に格納される
     *
     * @param _data 読み込んだデータ
     * @param _key  解析対象のカラム名
     * @return 重複をマージして出現頻度をカラム"data"に格納したもの
     */
    private static Dataset<Row> analyseFrequencyString(Dataset<Row> _data, String _key) {

        Dataset<Row> wordsData2 = _data.groupBy(_key).count();

        return wordsData2.withColumnRenamed("count", "data");
    }

    /**
     * 単語分割をする
     * 分割データはカラム"words"に格納される
     *
     * @param _data 分割するデータ
     * @param _key  分割対象のカラム名
     * @return 単語分割しカラム"words"に格納したもの
     */
    private static Dataset<Row> breakIntoWordsData(Dataset<Row> _data, String _key) {
        //単語分割
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Tokenizer tokenizer = new Tokenizer().setInputCol(_key).setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(_data).select("words");

        return wordsData;
    }

    /**
     * 結果を表示する。
     *
     * @param _data        表示するデータ
     * @param _shownum     表示するデータ数
     * @param _isAscendant 表示順。{@code true} 昇順。{@code false} 降順
     */
    private static void showResult(Dataset<Row> _data, int _shownum, boolean _isAscendant) {

        if (_isAscendant) {
            _data.sort(asc("data")).show(_shownum);
        } else {
            _data.sort(desc("data")).show(_shownum);
        }
    }

    /**
     * 結果として表示する要素の数を習得する
     * 指定がない場合は演習で指定された数を表示する
     *
     * @param args コマンドラインの引数
     * @return 表示する要素数
     */
    private static int getShowNum(String[] args) {
        int shownum = 0;
        if (args.length >= 3) {
            try {
                shownum = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                //何もしない
            }
        }
        if (shownum <= 0) {
            //デフォルトは10
            shownum = 10;
            if (args[1].equals(PARAMVAL_ANALYSE_WORDFREQUENCY)) {
                //ただし1つ目の演習だけは30なので、単語解析は30とする
                shownum = 30;
            }
        }
        return shownum;
    }

    /**
     * 結果の表示順番を習得する。
     *
     * @param args コマンドラインの引数
     * @return {@code false} 降順。{@code true} 昇順。
     */
    private static boolean getResultOrder(String[] args) {
        if (args.length < 4) {
            //デフォルトは降順
            return false;
        } else if (args[3].equals(PARAMVAL_ANALYSE_ASCENDANT)) {
            //昇順が指定された
            return true;
        } else {
            //デフォルトは降順
            return false;
        }
    }
}
