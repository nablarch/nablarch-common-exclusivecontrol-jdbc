package nablarch.common.exclusivecontrol;

import java.sql.SQLException;

import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;

/**
 * 排他制御用のテストサポート。
 * @author Kiyohito Itoh
 */
public class ExclusiveControlTestSupport {


    protected static final String[][] MESSAGES = {
        { "MSG00025", "ja", "処理対象データは他のユーザによって更新されました。はじめから操作をやり直してください。", "en", "Target data to operate was updated by other user. Please try operation from begining." },
        { "CUST0001", "ja", "カスタムメッセージ", "en", "custom message" }
    };


    @BeforeClass
    public static void setUpClass() throws SQLException {
        VariousDbTestHelper.createTable(ExclusiveUserMst.class);
        VariousDbTestHelper.createTable(UserMst.class);
        VariousDbTestHelper.createTable(ExclusiveCompMst.class);
        VariousDbTestHelper.createTable(ExclusiveDummyMst.class);
    }
}
