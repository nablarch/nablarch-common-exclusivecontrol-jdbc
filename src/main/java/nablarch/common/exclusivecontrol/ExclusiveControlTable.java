package nablarch.common.exclusivecontrol;

/**
 * 排他制御用テーブルのスキーマ情報とSQL文を保持するクラス。
 * @author Kiyohito Itoh
 */
public class ExclusiveControlTable {
    
    /** バージョン番号カラム名 */
    private String versionColumnName;
    
    /** バージョン番号を取得するSQL文(バージョン番号の更新チェックなし) */
    private String selectSql;
    
    /** バージョン番号を取得するSQL文(バージョン番号の更新チェックあり) */
    private String selectAndCheckSql;
    
    /** バージョン番号を追加するSQL文 */
    private String insertSql;
    
    /** バージョン番号を更新するSQL文(バージョン番号の更新チェックなし) */
    private String updateSql;
    
    /** バージョン番号を更新するSQL文(バージョン番号の更新チェックあり) */
    private String updateAndCheckSql;
    
    /** バージョン番号を削除するSQL文 */
    private String deleteSql;

    /**
     * コンストラクタ。
     * @param versionColumnName バージョン番号カラム名
     * @param selectSql バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)
     * @param selectAndCheckSql バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)
     * @param insertSql バージョン番号を追加するSQL文
     * @param updateSql バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)
     * @param updateAndCheckSql バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)
     * @param deleteSql バージョン番号を削除するSQL文
     */
    public ExclusiveControlTable(String versionColumnName,
                                  String selectSql,
                                  String selectAndCheckSql,
                                  String insertSql,
                                  String updateSql,
                                  String updateAndCheckSql,
                                  String deleteSql) {
        this.versionColumnName = versionColumnName;
        this.selectSql = selectSql;
        this.selectAndCheckSql = selectAndCheckSql;
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.updateAndCheckSql = updateAndCheckSql;
        this.deleteSql = deleteSql;
    }
    
    /**
     * バージョン番号カラム名を取得する。
     * @return バージョン番号カラム名
     */
    public String getVersionColumnName() {
        return versionColumnName;
    }

    /**
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)を取得する。
     * @return バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)
     */
    public String getSelectSql() {
        return selectSql;
    }

    /**
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)を取得する。
     * @return バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)
     */
    public String getSelectAndCheckSql() {
        return selectAndCheckSql;
    }

    /**
     * バージョン番号を追加するSQL文を取得する。
     * @return バージョン番号を追加するSQL文
     */
    public String getInsertSql() {
        return insertSql;
    }

    /**
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)を取得する。
     * @return バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)
     */
    public String getUpdateSql() {
        return updateSql;
    }

    /**
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)を取得する。
     * @return バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)
     */
    public String getUpdateAndCheckSql() {
        return updateAndCheckSql;
    }

    /**
     * バージョン番号を削除するSQL文を取得する。
     * @return バージョン番号を削除するSQL文
     */
    public String getDeleteSql() {
        return deleteSql;
    }
}
