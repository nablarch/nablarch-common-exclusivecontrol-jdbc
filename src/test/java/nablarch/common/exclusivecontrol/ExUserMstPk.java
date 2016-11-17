package nablarch.common.exclusivecontrol;

/**
 * 排他制御のテスト用の主キークラス。
 * @author Kiyohito Itoh
 */
public class ExUserMstPk extends ExclusiveControlContext {

    public enum PK { USER_ID, PK2, PK3 };
    
    public ExUserMstPk(String userId, String pk2, String pk3) {
        setTableName("EXCLUSIVE_USER_MST");
        setVersionColumnName("VERSION");
        setPrimaryKeyColumnNames(PK.values());
        appendCondition(PK.USER_ID, userId);
        appendCondition(PK.PK2, pk2);
        appendCondition(PK.PK3, pk3);
    }
}
