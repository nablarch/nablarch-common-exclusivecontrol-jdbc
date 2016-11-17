package nablarch.common.exclusivecontrol;

public class UserMstPk extends ExclusiveControlContext {

    public enum PK { USER_ID, PK2, PK3 };
    
    public UserMstPk(String userId, String pk2, String pk3) {
        setTableName("USER_MST");
        setVersionColumnName("VERSION");
        setPrimaryKeyColumnNames(PK.values());
        appendCondition(PK.USER_ID, userId);
        appendCondition(PK.PK2, pk2);
        appendCondition(PK.PK3, pk3);
    }
}
