package nablarch.common.exclusivecontrol;

/**
 * 排他制御のテスト用の主キークラス。
 * @author Kiyohito Itoh
 */
public class ExDummyMstPk extends ExclusiveControlContext {
    
    public enum PK { PK1 };
    
    public ExDummyMstPk(String pk1) {
        setTableName("EXCLUSIVE_DUMMY_MST");
        setVersionColumnName("VERSION");
        setPrimaryKeyColumnNames(PK.values());
        appendCondition(PK.PK1, pk1);
    }
}
