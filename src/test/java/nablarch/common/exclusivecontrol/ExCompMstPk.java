package nablarch.common.exclusivecontrol;

/**
 * 排他制御のテスト用の主キークラス。
 * @author Kiyohito Itoh
 */
public class ExCompMstPk extends ExclusiveControlContext {

    public enum PK { COMP_ID };
    
    public ExCompMstPk(String compId) {
        setTableName("EXCLUSIVE_COMP_MST");
        setVersionColumnName("VERSION");
        setPrimaryKeyColumnNames(PK.values());
        appendCondition(PK.COMP_ID, compId);
    }
}
