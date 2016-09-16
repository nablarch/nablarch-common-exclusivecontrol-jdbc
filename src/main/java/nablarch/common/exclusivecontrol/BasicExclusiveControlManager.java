package nablarch.common.exclusivecontrol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.message.Message;
import nablarch.core.message.MessageLevel;
import nablarch.core.message.MessageUtil;
import nablarch.core.util.Builder;
import nablarch.core.util.StringUtil;

/**
 * {@link ExclusiveControlManager}の基本実装クラス。
 * @author Kiyohito Itoh
 */
public class BasicExclusiveControlManager implements ExclusiveControlManager {

    /** バージョン番号の初期値 */
    private static final long INITIAL_VERSION = 1L;

    /** SQL文のキャッシュ */
    private static Map<String, ExclusiveControlTable> exclusiveControlTableSchemaAndSqlHolderCache
            = new ConcurrentHashMap<String, ExclusiveControlTable>();
    
    /** 楽観ロックエラーメッセージID */
    private String optimisticLockErrorMessageId;
    
    /**
     * 楽観ロックエラーメッセージIDを設定する。
     * @param optimisticLockErrorMessageId 楽観ロックエラーメッセージID
     */
    public void setOptimisticLockErrorMessageId(String optimisticLockErrorMessageId) {
        this.optimisticLockErrorMessageId = optimisticLockErrorMessageId;
    }

    /** {@inheritDoc} */
    public Version getVersion(ExclusiveControlContext context) {
        
        ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(context);
        String sql = exclusiveControlTableHolder.getSelectSql();
        Map<String, Object> condition = context.getCondition();
        
        AppDbConnection conn = DbConnectionContext.getConnection();
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, condition);
        SqlResultSet resultSet = stmt.retrieve(condition);
        
        if (resultSet.isEmpty()) {
            return null;
        }
        
        String version = resultSet.get(0).getString(context.getVersionColumnName());
        return new Version(context, version);
    }
    
    /** {@inheritDoc} */
    public void checkVersions(List<Version> versions) {
        
        List<Version> errorVersions = new ArrayList<Version>();
        
        for (Version version : versions) {
            
            ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(version);
            String sql = exclusiveControlTableHolder.getSelectAndCheckSql();
            Map<String, Object> condition = version.getPrimaryKeyCondition();
            putVersionNo(condition, exclusiveControlTableHolder, version);

            AppDbConnection conn = DbConnectionContext.getConnection();
            ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, condition);
            SqlResultSet resultSet = stmt.retrieve(condition);
            
            if (resultSet.isEmpty()) {
                errorVersions.add(version);
            }
        }
        
        if (!errorVersions.isEmpty()) {
            throw new OptimisticLockException(errorVersions, getOptimisticLockErrorMessage());
        }
    }
    
    /**
     * 楽観的ロックエラー発生時のメッセージを取得する。
     * @return 楽観的ロックエラー発生時のメッセージ。メッセージIDが設定されていない場合はnull
     */
    protected Message getOptimisticLockErrorMessage() {
        return StringUtil.hasValue(optimisticLockErrorMessageId)
                ? MessageUtil.createMessage(MessageLevel.ERROR, optimisticLockErrorMessageId) : null;
    }
    
    /** {@inheritDoc} */
    public void updateVersionsWithCheck(List<Version> versions) {
        
        List<Version> errorVersions = new ArrayList<Version>();
        
        for (Version version : versions) {
            
            ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(version);
            String sql = exclusiveControlTableHolder.getUpdateAndCheckSql();
            Map<String, Object> data = version.getPrimaryKeyCondition();
            putVersionNo(data, exclusiveControlTableHolder, version);
            
            AppDbConnection conn = DbConnectionContext.getConnection();
            ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, data);
            int count = stmt.executeUpdateByMap(data);
            
            if (count == 0) {
                errorVersions.add(version);
            }
        }
        
        if (!errorVersions.isEmpty()) {
            throw new OptimisticLockException(errorVersions, getOptimisticLockErrorMessage());
        }
    }

    /** {@inheritDoc} */
    public void updateVersion(ExclusiveControlContext context) {
        
        ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(context);
        String sql = exclusiveControlTableHolder.getUpdateSql();
        
        AppDbConnection conn = DbConnectionContext.getConnection();
        Map<String, Object> data = context.getCondition();
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, data);
        
        int count = stmt.executeUpdateByMap(data);
        if (count != 1) {
            throw new IllegalArgumentException(
                    String.format("version was not found. sql = [%s], data = [%s]", sql, data));
        }
    }
    
    /**
     * 初期バージョン番号を取得する。
     * <p/>
     * このメソッドは、バージョン番号追加時に使用される。
     * デフォルト実装では、"1"を返す。
     * @return 初期バージョン番号
     */
    protected Long getInitialVersion() {
        return INITIAL_VERSION;
    }
    
    /** {@inheritDoc} */
    public void addVersion(ExclusiveControlContext context) {
        
        ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(context);
        String sql = exclusiveControlTableHolder.getInsertSql();
        
        AppDbConnection conn = DbConnectionContext.getConnection();
        Map<String, Object> data = new HashMap<String, Object>(context.getCondition());
        data.put(ExclusiveControlUtil.convertToVariableName(exclusiveControlTableHolder.getVersionColumnName()), getInitialVersion());
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, data);
        stmt.executeUpdateByMap(data);
    }
    
    /** {@inheritDoc} */
    public void removeVersion(ExclusiveControlContext context) {
        
        ExclusiveControlTable exclusiveControlTableHolder = getExclusiveControlTableHolder(context);
        String sql = exclusiveControlTableHolder.getDeleteSql();
        
        AppDbConnection conn = DbConnectionContext.getConnection();
        Map<String, Object> condition = context.getCondition();
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatement(sql, condition);
        int count = stmt.executeUpdateByMap(condition);
        if (count != 1) {
            throw new IllegalArgumentException(
                    String.format("version was not found. sql = [%s], condition = [%s]", sql, condition));
        }
    }
    
    /**
     * 排他制御用テーブルに対応した{@link ExclusiveControlTable}を取得する。
     * <p/>
     * {@link #getExclusiveControlTableHolder(String, String, String...)}に処理を委譲する。
     * @param context 排他制御コンテキスト
     * @return 排他制御用テーブルに対応した{@link ExclusiveControlTable}
     */
    protected ExclusiveControlTable getExclusiveControlTableHolder(ExclusiveControlContext context) {
        Enum<?>[] pkEnums = context.getPrimaryKeyColumnNames();
        String[] primaryKeyColumnNames = new String[pkEnums.length];
        for (int i = 0; i < pkEnums.length; i++) {
            primaryKeyColumnNames[i] = pkEnums[i].name();
        }
        return getExclusiveControlTableHolder(
                context.getTableName(), context.getVersionColumnName(), primaryKeyColumnNames);
    }
    
    /**
     * 排他制御用テーブルに対応した{@link ExclusiveControlTable}を取得する。
     * <p/>
     * {@link #getExclusiveControlTableHolder(String, String, String...)}に処理を委譲する。
     * @param version バージョン番号
     * @return 排他制御用テーブルに対応した{@link ExclusiveControlTable}
     */
    protected ExclusiveControlTable getExclusiveControlTableHolder(Version version) {
        Set<String> pkSet = version.getPrimaryKeyCondition().keySet();
        return getExclusiveControlTableHolder(
                version.getTableName(), version.getVersionColumnName(), pkSet.toArray(new String[pkSet.size()]));
    }
    
    /**
     * 排他制御用テーブルに対応した{@link ExclusiveControlTable}を取得する。
     * <p/>
     * 一度生成した{@link ExclusiveControlTable}は、メモリ上にキャッシュしている。
     * このため、キャッシュに存在する場合は、キャッシュしているものを返し、
     * キャッシュに存在しない場合は、{@link ExclusiveControlTable}を生成し、キャッシュに追加したものを返す。
     * {@link ExclusiveControlTable}の生成では、排他制御用テーブルのスキーマ情報からSQL文を作成する。
     * @param tableName 排他制御用テーブルのテーブル名
     * @param versionColumnName バージョン番号カラム名
     * @param primaryKeyColumnNames 主キーのカラム名
     * @return 排他制御用テーブルに対応した{@link ExclusiveControlTable}
     * @see #createExclusiveControlTableSchemaAndSqlHolder(String, String, String...)
     */
    protected ExclusiveControlTable getExclusiveControlTableHolder(String tableName, String versionColumnName, String... primaryKeyColumnNames) {
        if (exclusiveControlTableSchemaAndSqlHolderCache.containsKey(tableName)) {
            return exclusiveControlTableSchemaAndSqlHolderCache.get(tableName);
        }
        synchronized (exclusiveControlTableSchemaAndSqlHolderCache) {
            if (exclusiveControlTableSchemaAndSqlHolderCache.containsKey(tableName)) {
                return exclusiveControlTableSchemaAndSqlHolderCache.get(tableName);
            }
            exclusiveControlTableSchemaAndSqlHolderCache.put(
                tableName, createExclusiveControlTableSchemaAndSqlHolder(tableName, versionColumnName, primaryKeyColumnNames));
            return exclusiveControlTableSchemaAndSqlHolderCache.get(tableName);
        }
    }
    
    /**
     * 排他制御用テーブルのスキーマ情報から{@link ExclusiveControlTable}を生成する。
     * <p/>
     * 下記のメソッドから各SQL文のテンプレートを取得し、プレースホルダを置換することでSQL文を作成する。
     * SQL文を変更したい場合は、下記メソッドをオーバライドして対応する。
     * <ul>
     * <li>{@link #getSelectSqlTemplate()}</li>
     * <li>{@link #getSelectAndCheckSqlTemplate()}</li>
     * <li>{@link #getInsertSqlTemplate()}</li>
     * <li>{@link #getUpdateSqlTemplate()}</li>
     * <li>{@link #getUpdateAndCheckSqlTemplate()}</li>
     * <li>{@link #getDeleteSqlTemplate()}</li>
     * </ul>
     * デフォルト実装で作成されるSQL文は下記のとおり。
     * <pre>
     * 排他制御用テーブルのスキーマ情報を下記に示す。
     * 
     * 排他制御用テーブルのテーブル名: USER_TBL
     * バージョン番号カラム名    : VERSION
     * 主キーのカラム名          : USER_ID, PK2, PK3
     * 
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)
     * 
     *     "SELECT VERSION FROM USER_TBL WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3"
     *     
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)
     * 
     *     "SELECT VERSION FROM USER_TBL WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3 AND VERSION = :version"
     *     
     * バージョン番号を追加するSQL文
     * 
     *     "INSERT INTO USER_TBL (USER_ID, PK2, PK3) VALUES (:user_id, :pk2, :pk3)"
     *     
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)
     * 
     *     "UPDATE USER_TBL SET VERSION = (VERSION + 1) WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3"
     *     
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)
     * 
     *     "UPDATE USER_TBL SET VERSION = (VERSION + 1) WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3 AND VERSION = :version"
     *     
     * バージョン番号を削除するSQL文
     * 
     *     "DELETE FROM USER_TBL WHERE WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3"
     *     
     * </pre>
     * @param tableName 排他制御用テーブルのテーブル名
     * @param versionColumnName バージョン番号カラム名
     * @param primaryKeyColumnNames 主キーのカラム名
     * @return {@link ExclusiveControlTable}
     */
    protected ExclusiveControlTable createExclusiveControlTableSchemaAndSqlHolder(String tableName, String versionColumnName, String... primaryKeyColumnNames) {
        
        String primaryKeysCondition = getPrimaryKeysCondition(primaryKeyColumnNames);
        String versionCondition = versionColumnName + " = :" + ExclusiveControlUtil.convertToVariableName(versionColumnName);
        
        // SELECT
        String selectSql = getSelectSqlTemplate().replace("$VERSION$", versionColumnName)
                                                 .replace("$TABLE_NAME$", tableName)
                                                 .replace("$PRIMARY_KEYS_CONDITION$", primaryKeysCondition);
        
        String selectAndCheckSql = getSelectAndCheckSqlTemplate().replace("$VERSION$", versionColumnName)
                                                                 .replace("$TABLE_NAME$", tableName)
                                                                 .replace("$PRIMARY_KEYS_CONDITION$", primaryKeysCondition)
                                                                 .replace("$VERSION_CONDITION$", versionCondition);
        
        // INSERT
        String insertSql = getInsertSqlTemplate().replace("$TABLE_NAME$", tableName)
                                                 .replace("$COLUMNS_AND_VALUES$",
                                                          getInsertColumnsAndValues(primaryKeyColumnNames, versionColumnName));
        
        // UPDATE
        String updateSql = getUpdateSqlTemplate().replace("$VERSION$", versionColumnName)
                                                 .replace("$TABLE_NAME$", tableName)
                                                 .replace("$PRIMARY_KEYS_CONDITION$", primaryKeysCondition);
        
        String updateAndCheckSql = getUpdateAndCheckSqlTemplate().replace("$VERSION$", versionColumnName)
                                                                 .replace("$TABLE_NAME$", tableName)
                                                                 .replace("$PRIMARY_KEYS_CONDITION$", primaryKeysCondition)
                                                                 .replace("$VERSION_CONDITION$", versionCondition);
        
        // DELETE
        String deleteSql = getDeleteSqlTemplate().replace("$TABLE_NAME$", tableName)
                                                 .replace("$PRIMARY_KEYS_CONDITION$", primaryKeysCondition);
        
        return new ExclusiveControlTable(versionColumnName, selectSql, selectAndCheckSql, insertSql, updateSql, updateAndCheckSql, deleteSql);
    }
    
    /**
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $VERSION$: バージョン番号カラム名
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $PRIMARY_KEYS_CONDITION$: 主キーの条件(例: "PK1 = :pk1 AND PK2 = :pk2")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "SELECT $VERSION$ FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "SELECT VERSION FROM EXCLUSIVE_USER WHERE USER_ID = :user_id"
     * 
     * </pre>
     * @return バージョン番号を取得するSQL文(バージョン番号の更新チェックなし)のテンプレート
     */
    protected String getSelectSqlTemplate() {
        return "SELECT $VERSION$ FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$";
    }

    /**
     * バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $VERSION$: バージョン番号カラム名
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $PRIMARY_KEYS_CONDITION$: 主キーの条件(例: "PK1 = :pk1 AND PK2 = :pk2")
     * $VERSION_CONDITION$: バージョン番号の条件(例: "VERSION = :version")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "SELECT $VERSION$ FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$ AND $VERSION_CONDITION$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "SELECT VERSION FROM EXCLUSIVE_USER WHERE USER_ID = :user_id AND VERSION = :version"
     * 
     * </pre>
     * @return バージョン番号を取得するSQL文(バージョン番号の更新チェックあり)のテンプレート
     */
    protected String getSelectAndCheckSqlTemplate() {
        return "SELECT $VERSION$ FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$ AND $VERSION_CONDITION$";
    }
    
    /**
     * バージョン番号を追加するSQL文のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $COLUMNS_AND_VALUES$: INSERT文のカラム名と値(例: "(PK1, PK2, VERSION) VALUES (:pk1, :pk2, :version)")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "INSERT INTO $TABLE_NAME$ $COLUMNS_AND_VALUES$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "INSERT INTO EXCLUSIVE_USER (USER_ID, VERSION) VALUES (:user_id, :version)"
     * 
     * </pre>
     * @return バージョン番号を追加するSQL文のテンプレート
     */
    protected String getInsertSqlTemplate() {
        return "INSERT INTO $TABLE_NAME$ $COLUMNS_AND_VALUES$";
    }

    /**
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $VERSION$: バージョン番号カラム名
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $PRIMARY_KEYS_CONDITION$: 主キーの条件(例: "PK1 = :pk1 AND PK2 = :pk2")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "UPDATE $TABLE_NAME$ SET $VERSION$ = ($VERSION$ + 1) WHERE $PRIMARY_KEYS_CONDITION$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "UPDATE EXCLUSIVE_USER SET VERSION = (VERSION + 1) WHERE USER_ID = :user_id"
     * 
     * </pre>
     * @return バージョン番号を更新するSQL文(バージョン番号の更新チェックなし)のテンプレート
     */
    protected String getUpdateSqlTemplate() {
        return "UPDATE $TABLE_NAME$ SET $VERSION$ = ($VERSION$ + 1) WHERE $PRIMARY_KEYS_CONDITION$";
    }

    /**
     * バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $VERSION$: バージョン番号カラム名
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $PRIMARY_KEYS_CONDITION$: 主キーの条件(例: "PK1 = :pk1 AND PK2 = :pk2")
     * $VERSION_CONDITION$: バージョン番号の条件(例: "VERSION = :version")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "UPDATE $TABLE_NAME$ SET $VERSION$ = ($VERSION$ + 1) WHERE $PRIMARY_KEYS_CONDITION$ AND $VERSION_CONDITION$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "UPDATE EXCLUSIVE_USER SET VERSION = (VERSION + 1) WHERE USER_ID = :user_id AND VERSION = :version"
     * 
     * </pre>
     * @return バージョン番号を更新するSQL文(バージョン番号の更新チェックあり)のテンプレート
     */
    protected String getUpdateAndCheckSqlTemplate() {
        return "UPDATE $TABLE_NAME$ SET $VERSION$ = ($VERSION$ + 1) WHERE $PRIMARY_KEYS_CONDITION$ AND $VERSION_CONDITION$";
    }
    
    /**
     * バージョン番号を削除するSQL文のテンプレートを取得する。
     * <pre>
     * 下記のプレースホルダを使用してテンプレートを作成する。
     * 
     * $TABLE_NAME$: 排他制御用テーブルのテーブル名
     * $PRIMARY_KEYS_CONDITION$: 主キーの条件(例: "PK1 = :pk1 AND PK2 = :pk2")
     * 
     * デフォルト実装では、下記のテンプレートを返す。
     * 
     * "DELETE FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$"
     * 
     * 変換例を下記に示す。
     * 
     * テーブル定義
     * 
     *     CREATE TABLE EXCLUSIVE_USER (
     *         USER_ID CHAR(6) NOT NULL,
     *         VERSION NUMBER(10) NOT NULL,
     *         PRIMARY KEY(USER_ID)
     *     )
     * 
     * テンプレートから作成されるSQL文
     * 
     *     "DELETE FROM EXCLUSIVE_USER WHERE USER_ID = :user_id"
     * 
     * </pre>
     * @return バージョン番号を削除するSQL文のテンプレート
     */
    protected String getDeleteSqlTemplate() {
        return "DELETE FROM $TABLE_NAME$ WHERE $PRIMARY_KEYS_CONDITION$";
    }
    
    /**
     * INSERT文のカラムと値を取得する。
     * @param primaryKeyColumnNames 主キーカラム名
     * @param versionColumnName バージョン番号カラム名
     * @return INSERT文のカラムと値
     */
    protected String getInsertColumnsAndValues(String[] primaryKeyColumnNames, String versionColumnName) {
        
        StringBuilder columns = new StringBuilder();
        columns.append(Builder.join(primaryKeyColumnNames, ", ")).append(", ").append(versionColumnName);
        
        StringBuilder values = new StringBuilder();
        for (String columnName : primaryKeyColumnNames) {
            if (values.length() != 0) {
                values.append(", ");
            }
            values.append(":" + ExclusiveControlUtil.convertToVariableName(columnName));
        }
        values.append(", ")
              .append(":" + ExclusiveControlUtil.convertToVariableName(versionColumnName));

        return String.format("(%s) VALUES (%s)", columns, values);
    }
    
    /**
     * 主キー条件を取得する。
     * @param columnNames カラム名
     * @return 主キー条件
     */
    protected String getPrimaryKeysCondition(String[] columnNames) {
        StringBuilder sb = new StringBuilder();
        for (String columnName : columnNames) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append(columnName).append(" = :").append(ExclusiveControlUtil.convertToVariableName(columnName));
        }
        return sb.toString();
    }

    /**
     * バージョン番号をデータオブジェクトに追加する。
     *
     * @param data データオブジェクト
     * @param exclusiveControlTableHolder 排他制御テーブルの情報
     * @param version バージョン情報
     */
    private static void putVersionNo(
            final Map<String, Object> data,
            final ExclusiveControlTable exclusiveControlTableHolder,
            final Version version) {
        data.put(ExclusiveControlUtil.convertToVariableName(exclusiveControlTableHolder.getVersionColumnName()),
                Long.valueOf(version.getVersion()));
    }

}
