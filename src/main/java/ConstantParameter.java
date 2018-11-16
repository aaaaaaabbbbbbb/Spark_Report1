/**
 * パラメータ関連の定数を定義する。
 *
 * TOSHI,I
 */
public class ConstantParameter {

    /** 指定値：解析対象：全部 **/
    private static final String PARAMVAL_TARGET_ALLTITLE = "a";
    /** 指定値：解析対象：シリーズ **/
    private static final String PARAMVAL_TARGET_SERIES = "f";
    /** 指定値：解析対象：エピソード **/
    private static final String PARAMVAL_TARGET_EPISODE = "e";
    /** 指定値：解析対象：映画 **/
    private static final String PARAMVAL_TARGET_MOVIE = "m";
    /** 指定値：解析対象：ショート **/
    private static final String PARAMVAL_TARGET_SHORT = "s";

    /** 指定値：分析内容：キャスト **/
    private static final String PARAMVAL_ANALYSE_CASTAPPEARANCE = "c";
    /** 指定値：分析内容：プロデューサ **/
    private static final String PARAMVAL_ANALYSE_PRODUCERAPPEARANCE = "p";
    /** 指定値：分析内容：監督 **/
    private static final String PARAMVAL_ANALYSE_DIRECTORAPPEARANCE = "d";
    /** 指定値：分析内容：単語頻度分析 **/
    private static final String PARAMVAL_ANALYSE_WORDFREQUENCY = "w";
    /** 指定値：分析内容：評価 **/
    private static final String PARAMVAL_ANALYSE_RATING = "r";

    /** 指定値：表示順：少ない/低い順 **/
    private static final String PARAMVAL_ANALYSE_ASCENDANT = "a";

    /** 内部値：指定エラー **/
    public static final int INTERNALVAL_INVALID = -1;
    /** 内部値：解析対象：全部 **/
    public static final int INTERNALVAL_TARGET_ALLTITLE = 0;
    /** 内部値：解析対象：シリーズ **/
    public static final int INTERNALVAL_TARGET_SERIES = 1;
    /** 内部値：解析対象：エピソード **/
    public static final int INTERNALVAL_TARGET_EPISODE = 2;
    /** 内部値：解析対象：映画 **/
    public static final int INTERNALVAL_TARGET_MOVIE = 3;
    /** 内部値：解析対象：ショート **/
    public static final int INTERNALVAL_TARGET_SHORT = 4;

    /** 内部値：分析内容：キャスト **/
    public static final int INTERNALVAL_ANALYSE_CASTAPPEARANCE = 0;
    /** 内部値：分析内容：プロデューサ **/
    public static final int INTERNALVAL_ANALYSE_PRODUCERAPPEARANCE = 1;
    /** 内部値：分析内容：監督**/
    public static final int INTERNALVAL_ANALYSE_DIRECTORAPPEARANCE = 2;
    /** 内部値：分析内容：単語頻度分析 **/
    public static final int INTERNALVAL_ANALYSE_WORDFREQUENCY = 3;
    /** 内部値：分析内容：評価 **/
    public static final int INTERNALVAL_ANALYSE_RATING = 4;

    /** データの値：カテゴリ：アクター **/
    public static final String DATAVAL_CATEGORY_ACTOR = "actor";
    /** データの値：カテゴリ：プロデューサ **/
    public static final String DATAVAL_CATEGORY_PRODUCER = "producer";
    /** データの値：カテゴリ：監督 **/
    public static final String DATAVAL_CATEGORY_DIRECTOR = "director";

    /** パラメータと内部値のテーブルのインデックス：パラメータ **/
    private final static int INDEX_INTERNALPARAMTABLE_PARAMETER = 0;
    /** パラメータと内部値のテーブルのインデックス：内部値 **/
    private final static int INDEX_INTERNALPARAMTABLE_INTERNAL = 1;

    /** 解析対象の指定文字列と内部値の対応 **/
    public final static Object[][] TABLE_INTERNALPARAMTABLE_TARGET = {
            {PARAMVAL_TARGET_ALLTITLE, INTERNALVAL_TARGET_ALLTITLE},
            {PARAMVAL_TARGET_SERIES, INTERNALVAL_TARGET_SERIES},
            {PARAMVAL_TARGET_EPISODE, INTERNALVAL_TARGET_EPISODE},
            {PARAMVAL_TARGET_MOVIE, INTERNALVAL_TARGET_MOVIE},
            {PARAMVAL_TARGET_SHORT, INTERNALVAL_TARGET_SHORT},
    };

    /** 分析内容の指定文字列と内部値の対応 **/
    public final static Object[][] TABLE_INTERNALPARAMTABLE_ANALYSE = {
            {PARAMVAL_ANALYSE_CASTAPPEARANCE, INTERNALVAL_ANALYSE_CASTAPPEARANCE},
            {PARAMVAL_ANALYSE_PRODUCERAPPEARANCE, INTERNALVAL_ANALYSE_PRODUCERAPPEARANCE},
            {PARAMVAL_ANALYSE_DIRECTORAPPEARANCE, INTERNALVAL_ANALYSE_DIRECTORAPPEARANCE},
            {PARAMVAL_ANALYSE_WORDFREQUENCY, INTERNALVAL_ANALYSE_WORDFREQUENCY},
            {PARAMVAL_ANALYSE_RATING, INTERNALVAL_ANALYSE_RATING},
    };

    /** 内部値と値の対応のテーブルのインデックス：パラメータ **/
    private final static int INDEX_PARAMETERTABLE_INTERNAL_= 0;
    /** 内部値と値の対応のテーブルのインデックス：データの値 **/
    private final static int INDEX_PARAMETERTABLE_VALUE = 1;

    /** 分析対象の内部値とカテゴリ名の文字列の対応 **/
    public final static Object[][] TABLE_PARAMETERTABLE_PARAM_ANALYSE = {
            {INTERNALVAL_ANALYSE_CASTAPPEARANCE, DATAVAL_CATEGORY_ACTOR},
            {INTERNALVAL_ANALYSE_PRODUCERAPPEARANCE, DATAVAL_CATEGORY_PRODUCER},
            {INTERNALVAL_ANALYSE_DIRECTORAPPEARANCE, DATAVAL_CATEGORY_DIRECTOR},
    };

    /** ファイルパス：メインのテーブル**/
    public static final String FILEPATH_TITLEBASIC = "hdfs:/IMDB_data/title.basics.tsv";
    /** ファイルパス：メインのテーブル**/
    public static final String FILEPATH_TITLEPRINCIPALS = "hdfs:/IMDB_data/title.principals.tsv";
    /** ファイルパス：キャストのテーブル**/
    public static final String FILEPATH_NAMEBASICS = "hdfs:/IMDB_data/name.basics.tsv";
    /** ファイルパス：レーティングのテーブル**/
    public static final String FILEPATH_TITLERATING = "hdfs:/IMDB_data/title.ratings.tsv";

    /**
     * 指定されたパラメータから内部定義値を取得する
     *
     * @param _param 指定されたパラメータ
     * @param _table パラメータと内部定義値のテーブル
     * @return 内部定義値
     */
    public static int getInternalVal(String _param, Object[][] _table){
        for(int i = 0; i < _table.length; i++){
            if(((String) _table[i][INDEX_INTERNALPARAMTABLE_PARAMETER]).equals(_param)){
                return ((Integer)_table[i][INDEX_INTERNALPARAMTABLE_INTERNAL]).intValue();
            }
        }
        return INTERNALVAL_INVALID;
    }


    /**
     * 指定された内部値からデータの値を取得する
     *
     * @param _internal 内部値
     * @param _table 内部値とデータの値定義値のテーブル
     * @return データの値
     */
    public static String getDataVal(int _internal, Object[][] _table){
        for(int i = 0; i < _table.length; i++){
            if(((Integer) _table[i][INDEX_PARAMETERTABLE_INTERNAL_]).intValue() == _internal){
                return (String)_table[i][INDEX_PARAMETERTABLE_VALUE];
            }
        }
        return null;
    }


    /**
     * 結果として表示する要素の数を習得する
     * 指定がない場合は演習で指定された数を表示する
     *
     * @param args コマンドラインの引数
     * @return 表示する要素数
     */
    static int getShowNum(String[] args) {
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
    static boolean getResultOrder(String[] args) {
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
