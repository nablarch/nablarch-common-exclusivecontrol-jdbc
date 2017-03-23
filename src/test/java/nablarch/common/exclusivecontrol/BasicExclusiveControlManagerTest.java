package nablarch.common.exclusivecontrol;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import nablarch.core.ThreadContext;
import nablarch.core.db.statement.exception.DuplicateStatementException;
import nablarch.core.db.support.DbAccessSupport;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.message.MockStringResourceHolder;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link BasicExclusiveControlManager}テスト。
 * @author Kiyohito Itoh
 */
@RunWith(DatabaseTestRunner.class)
public class BasicExclusiveControlManagerTest extends ExclusiveControlTestSupport {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("nablarch/common/exclusivecontrol/exclusivecontrol.xml");

    private static SimpleDbTransactionManager transactionManager;

    @BeforeClass
    public static void setUpClass() {
        VariousDbTestHelper.createTable(ExclusiveUserMst.class);
        VariousDbTestHelper.createTable(ExclusiveCompMst.class);
        VariousDbTestHelper.createTable(ExclusiveDummyMst.class);
        VariousDbTestHelper.createTable(UserMst.class);
    }

    @AfterClass
    public static void tearDownClass() {
        VariousDbTestHelper.dropTable(ExclusiveUserMst.class);
        VariousDbTestHelper.dropTable(ExclusiveCompMst.class);
        VariousDbTestHelper.dropTable(ExclusiveDummyMst.class);
        VariousDbTestHelper.dropTable(UserMst.class);
    }

    @Before
    public void setUp() {
        ThreadContext.setLanguage(Locale.JAPAN);
        transactionManager = repositoryResource.getComponent("dbManager-default");
        transactionManager.beginTransaction();
        repositoryResource.getComponentByType(MockStringResourceHolder.class).setMessages(MESSAGES);

        VariousDbTestHelper.delete(ExclusiveUserMst.class);
        VariousDbTestHelper.delete(ExclusiveCompMst.class);
        VariousDbTestHelper.delete(ExclusiveDummyMst.class);
        VariousDbTestHelper.delete(UserMst.class);
    }

    @After
    public void tearDown() {
        transactionManager.endTransaction();
    }

    /**
     * バージョン番号のCRUDをテストする。
     */
    @Test
    public void testVersionCrud() {

        ExclusiveControlManager manager = new BasicExclusiveControlManager();

        /****************************************************************
        バージョン番号を追加した場合
        ****************************************************************/

        List<ExclusiveUserMst> result = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(result.size(), is(0));

        ExclusiveControlContext context = new ExUserMstPk("uid001", "pk2001", "pk3001");
        manager.addVersion(context);
        transactionManager.commitTransaction();

        result = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(result.size(), is(1));
        assertThat(result.get(0).userId, is("uid001"));
        assertThat(result.get(0).pk2, is("pk2001"));
        assertThat(result.get(0).pk3, is("pk3001"));
        assertThat(result.get(0).version, is(1L));

        // 追加後に条件が変更されていないことをテスト。
        assertThat(context.getCondition().size(), is(3));
        assertThat(context.getCondition().get("user_id").toString(), is("uid001"));
        assertThat(context.getCondition().get("pk2").toString(), is("pk2001"));
        assertThat(context.getCondition().get("pk3").toString(), is("pk3001"));

        /****************************************************************
        バージョン番号を取得した場合
        ****************************************************************/

        Version version = manager.getVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));

        assertThat(version.getTableName(), is("EXCLUSIVE_USER_MST"));
        assertThat(version.getPrimaryKeyCondition().size(), is(3));
        assertThat(version.getPrimaryKeyCondition().get("user_id").toString(), is("uid001"));
        assertThat(version.getPrimaryKeyCondition().get("pk2").toString(), is("pk2001"));
        assertThat(version.getPrimaryKeyCondition().get("pk3").toString(), is("pk3001"));
        assertThat(version.getVersion(), is("1"));
        assertThat(version.toString(), containsString("tableName = [EXCLUSIVE_USER_MST], version = [1], primaryKeyCondition = ["));
        assertThat(version.toString(), containsString("user_id=uid001"));
        assertThat(version.toString(), containsString("pk2=pk2001"));
        assertThat(version.toString(), containsString("pk3=pk3001"));

        /****************************************************************
        バージョン番号を更新した場合
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        result = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(result.size(), is(1));
        assertThat(result.get(0).userId, is("uid001"));
        assertThat(result.get(0).pk2, is("pk2001"));
        assertThat(result.get(0).pk3, is("pk3001"));
        assertThat(result.get(0).version, is(2L));

        /****************************************************************
        バージョン番号を削除した場合
        ****************************************************************/

        manager.removeVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        result = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(result.size(), is(0));

        /****************************************************************
        同じ主キーで追加した場合
        ****************************************************************/

        manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002")); // 1st
        transactionManager.commitTransaction();

        result = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(result, is(notNullValue()));
        assertThat(result.get(0).userId, is("uid002"));
        assertThat(result.get(0).pk2, is("pk2002"));
        assertThat(result.get(0).pk3, is("pk3002"));
        assertThat(result.get(0).version, is(1L));

        try {
            manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002")); // 2nd
            fail();
        } catch (DuplicateStatementException e) {
            // success
        } finally {
            transactionManager.rollbackTransaction();
        }

        /****************************************************************
        存在しない主キーで取得した場合
        ****************************************************************/

        assertNull(manager.getVersion(new ExUserMstPk("xxxxxx", "yyyyyy", "zzzzzz")));

        /****************************************************************
        存在しない主キーで更新した場合
        ****************************************************************/

        try {
            manager.updateVersion(new ExUserMstPk("xxxxxx", "yyyyyy", "zzzzzz"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("version was not found."));
            assertThat(e.getMessage(), containsString("sql = [UPDATE EXCLUSIVE_USER_MST SET VERSION = (VERSION + 1) WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3]"));
            assertThat(e.getMessage(), containsString("user_id=xxxxxx"));
            assertThat(e.getMessage(), containsString("pk2=yyyyyy"));
            assertThat(e.getMessage(), containsString("pk3=zzzzzz"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        /****************************************************************
        存在しない主キーで削除した場合
        ****************************************************************/

        try {
            manager.removeVersion(new ExUserMstPk("xxxxxx", "yyyyyy", "zzzzzz"));
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("version was not found."));
            assertThat(e.getMessage(), containsString("sql = [DELETE FROM EXCLUSIVE_USER_MST WHERE USER_ID = :user_id AND PK2 = :pk2 AND PK3 = :pk3]"));
            assertThat(e.getMessage(), containsString("user_id=xxxxxx"));
            assertThat(e.getMessage(), containsString("pk2=yyyyyy"));
            assertThat(e.getMessage(), containsString("pk3=zzzzzz"));
        } finally {
            transactionManager.rollbackTransaction();
        }
    }

    /**
     * バージョン番号の更新チェックをテストする。
     */
    @Test
    public void testVersionUpdatingCheck() {

        ExclusiveControlManager manager = new BasicExclusiveControlManager();

        /****************************************************************
        バージョン番号が更新されていない場合(1件)
        ****************************************************************/
        VariousDbTestHelper.delete(ExclusiveUserMst.class);
        List<ExclusiveUserMst> exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(0));

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);;
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));

        manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1")));

        /****************************************************************
        バージョン番号が更新されている場合(1件)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));

        try {
            manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getMessages().size(), is(0));
            assertThat(e.getErrorVersions().size(), is(1));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3001"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        /****************************************************************
        バージョン番号が更新されていない場合(複数件)
        ****************************************************************/

        VariousDbTestHelper.delete(ExclusiveUserMst.class);

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.addVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.addVersion(new ExUserMstPk("uid004", "pk2004", "pk3004"));
        manager.addVersion(new ExUserMstPk("uid005", "pk2005", "pk3005"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(1L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(1L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(1L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(1L));

        manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                            new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                            new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                            new Version(new ExUserMstPk("uid004", "pk2004", "pk3004"), "1"),
                                            new Version(new ExUserMstPk("uid005", "pk2005", "pk3005"), "1")));

        /****************************************************************
        バージョン番号が更新されている場合(複数件)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.updateVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.updateVersion(new ExUserMstPk("uid005", "pk2005", "pk3005"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(1L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(2L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(1L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(2L));

        try {
            manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                                new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                                new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                                new Version(new ExUserMstPk("uid004", "pk2004", "pk3004"), "1"),
                                                new Version(new ExUserMstPk("uid005", "pk2005", "pk3005"), "1")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getMessages().size(), is(0));
            assertThat(e.getErrorVersions().size(), is(3));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3001"));
            assertThat(e.getErrorVersions().get(1).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(1).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("user_id").toString(), is("uid003"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("pk2").toString(), is("pk2003"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("pk3").toString(), is("pk3003"));
            assertThat(e.getErrorVersions().get(2).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(2).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("user_id").toString(), is("uid005"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("pk2").toString(), is("pk2005"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("pk3").toString(), is("pk3005"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        /****************************************************************
        以降は、楽観ロックエラーメッセージIDを指定した場合
        ****************************************************************/
        ((BasicExclusiveControlManager) manager).setOptimisticLockErrorMessageId("MSG00025");

        /****************************************************************
        バージョン番号が更新されていない場合(複数テーブル)
        ****************************************************************/

        VariousDbTestHelper.delete(ExclusiveUserMst.class);
        VariousDbTestHelper.delete(ExclusiveCompMst.class);

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.addVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.addVersion(new ExCompMstPk("com001"));
        manager.addVersion(new ExCompMstPk("com002"));
        manager.addVersion(new ExCompMstPk("com003"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(1L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(1L));

        List<ExclusiveCompMst> exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(1L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(1L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(1L));

        manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                            new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                            new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                            new Version(new ExCompMstPk("com001"), "1"),
                                            new Version(new ExCompMstPk("com002"), "1"),
                                            new Version(new ExCompMstPk("com003"), "1")));

        /****************************************************************
        バージョン番号が更新されている場合(複数テーブル)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.updateVersion(new ExCompMstPk("com003"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(2L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(1L));

        exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(1L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(1L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(2L));

        try {
            manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                                new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                                new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                                new Version(new ExCompMstPk("com001"), "1"),
                                                new Version(new ExCompMstPk("com002"), "1"),
                                                new Version(new ExCompMstPk("com003"), "1")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getMessages().size(), is(1));
            assertThat(e.getMessages().get(0).formatMessage(),
                    is("処理対象データは他のユーザによって更新されました。はじめから操作をやり直してください。"));
            assertThat(e.getErrorVersions().size(), is(2));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid002"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2002"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3002"));
            assertThat(e.getErrorVersions().get(1).getTableName(), is("EXCLUSIVE_COMP_MST"));
            assertThat(e.getErrorVersions().get(1).getVersion(), is("1"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("comp_id").toString(), is("com003"));
        } finally {
            transactionManager.rollbackTransaction();
        }
    }

    /**
     * バージョン番号の更新チェックを伴う更新をテストする。
     */
    @Test
    public void testVersionUpdatingCheckAndUpdate() {

        ExclusiveControlManager manager = new BasicExclusiveControlManager();

        /****************************************************************
        バージョン番号が更新されていない場合(1件)
        ****************************************************************/
        VariousDbTestHelper.delete(ExclusiveUserMst.class);
        List<ExclusiveUserMst> exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(0));

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));

        manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1")));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));

        /****************************************************************
        バージョン番号が更新されている場合(1件)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(3L));

        try {
            manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "2")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getErrorVersions().size(), is(1));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3001"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(3L));

        /****************************************************************
        バージョン番号が更新されていない場合(複数件)
        ****************************************************************/

        VariousDbTestHelper.delete(ExclusiveUserMst.class);

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.addVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.addVersion(new ExUserMstPk("uid004", "pk2004", "pk3004"));
        manager.addVersion(new ExUserMstPk("uid005", "pk2005", "pk3005"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(1L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(1L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(1L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(1L));

        manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                                      new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                                      new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                                      new Version(new ExUserMstPk("uid004", "pk2004", "pk3004"), "1"),
                                                      new Version(new ExUserMstPk("uid005", "pk2005", "pk3005"), "1")));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(2L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(2L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(2L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(2L));

        /****************************************************************
        バージョン番号が更新されている場合(複数件)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.updateVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.updateVersion(new ExUserMstPk("uid005", "pk2005", "pk3005"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(3L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(2L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(3L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(2L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(3L));
        try {
            manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "2"),
                                                          new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "2"),
                                                          new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "2"),
                                                          new Version(new ExUserMstPk("uid004", "pk2004", "pk3004"), "2"),
                                                          new Version(new ExUserMstPk("uid005", "pk2005", "pk3005"), "2")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getErrorVersions().size(), is(3));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2001"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3001"));
            assertThat(e.getErrorVersions().get(1).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(1).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("user_id").toString(), is("uid003"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("pk2").toString(), is("pk2003"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("pk3").toString(), is("pk3003"));
            assertThat(e.getErrorVersions().get(2).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(2).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("user_id").toString(), is("uid005"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("pk2").toString(), is("pk2005"));
            assertThat(e.getErrorVersions().get(2).getPrimaryKeyCondition().get("pk3").toString(), is("pk3005"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(5));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(3L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(2L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(3L));
        assertThat(exclusiveUserMstList.get(3).userId, is("uid004"));
        assertThat(exclusiveUserMstList.get(3).version, is(2L));
        assertThat(exclusiveUserMstList.get(4).userId, is("uid005"));
        assertThat(exclusiveUserMstList.get(4).version, is(3L));

        /****************************************************************
        バージョン番号が更新されていない場合(複数テーブル)
        ****************************************************************/

        VariousDbTestHelper.delete(ExclusiveUserMst.class);
        VariousDbTestHelper.delete(ExclusiveCompMst.class);

        manager.addVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        manager.addVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.addVersion(new ExUserMstPk("uid003", "pk2003", "pk3003"));
        manager.addVersion(new ExCompMstPk("com001"));
        manager.addVersion(new ExCompMstPk("com002"));
        manager.addVersion(new ExCompMstPk("com003"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(1L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(1L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(1L));

        List<ExclusiveCompMst> exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(1L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(1L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(1L));

        manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "1"),
                                                      new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "1"),
                                                      new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "1"),
                                                      new Version(new ExCompMstPk("com001"), "1"),
                                                      new Version(new ExCompMstPk("com002"), "1"),
                                                      new Version(new ExCompMstPk("com003"), "1")));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(2L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(2L));

        exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(2L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(2L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(2L));

        /****************************************************************
        バージョン番号が更新されている場合(複数テーブル)
        ****************************************************************/

        manager.updateVersion(new ExUserMstPk("uid002", "pk2002", "pk3002"));
        manager.updateVersion(new ExCompMstPk("com003"));
        transactionManager.commitTransaction();

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(3L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(2L));

        exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(2L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(2L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(3L));

        try {
            manager.checkVersions(Arrays.asList(new Version(new ExUserMstPk("uid001", "pk2001", "pk3001"), "2"),
                                                new Version(new ExUserMstPk("uid002", "pk2002", "pk3002"), "2"),
                                                new Version(new ExUserMstPk("uid003", "pk2003", "pk3003"), "2"),
                                                new Version(new ExCompMstPk("com001"), "2"),
                                                new Version(new ExCompMstPk("com002"), "2"),
                                                new Version(new ExCompMstPk("com003"), "2")));
            fail();
        } catch (OptimisticLockException e) {
            assertThat(e.getErrorVersions().size(), is(2));
            assertThat(e.getErrorVersions().get(0).getTableName(), is("EXCLUSIVE_USER_MST"));
            assertThat(e.getErrorVersions().get(0).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("user_id").toString(), is("uid002"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk2").toString(), is("pk2002"));
            assertThat(e.getErrorVersions().get(0).getPrimaryKeyCondition().get("pk3").toString(), is("pk3002"));
            assertThat(e.getErrorVersions().get(1).getTableName(), is("EXCLUSIVE_COMP_MST"));
            assertThat(e.getErrorVersions().get(1).getVersion(), is("2"));
            assertThat(e.getErrorVersions().get(1).getPrimaryKeyCondition().get("comp_id").toString(), is("com003"));
        } finally {
            transactionManager.rollbackTransaction();
        }

        exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class, "userId");
        assertThat(exclusiveUserMstList.size(), is(3));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
        assertThat(exclusiveUserMstList.get(1).userId, is("uid002"));
        assertThat(exclusiveUserMstList.get(1).version, is(3L));
        assertThat(exclusiveUserMstList.get(2).userId, is("uid003"));
        assertThat(exclusiveUserMstList.get(2).version, is(2L));

        exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class, "compId");
        assertThat(exclusiveCompMstList.size(), is(3));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(2L));
        assertThat(exclusiveCompMstList.get(1).compId, is("com002"));
        assertThat(exclusiveCompMstList.get(1).version, is(2L));
        assertThat(exclusiveCompMstList.get(2).compId, is("com003"));
        assertThat(exclusiveCompMstList.get(2).version, is(3L));
    }

    /**
     * メソッド呼び出し順に依存せずに使用できることをテストする。
     */
    @Test
    public void testInvalidMethodInvoking() throws SQLException {

        ExclusiveControlManager manager = new BasicExclusiveControlManager();

        /****************************************************************
        バージョン番号を取得する前にバージョン番号のチェックメソッドが呼ばれた場合
        ****************************************************************/

        VariousDbTestHelper.setUpTable(new ExclusiveDummyMst("dum001", 1L));

        manager.checkVersions(Arrays.asList(new Version(new ExDummyMstPk("dum001"), "1")));

        List<ExclusiveDummyMst> exclusiveDummyMstList = VariousDbTestHelper.findAll(ExclusiveDummyMst.class);
        assertThat(exclusiveDummyMstList.size(), is(1));
        assertThat(exclusiveDummyMstList.get(0).pk1, is("dum001"));
        assertThat(exclusiveDummyMstList.get(0).version, is(1L));

        /****************************************************************
        バージョン番号を取得する前にバージョン番号のチェック付き更新メソッドが呼ばれた場合
        ****************************************************************/

        VariousDbTestHelper.setUpTable(new ExclusiveCompMst("com001", 1L));

        manager.updateVersionsWithCheck(Arrays.asList(new Version(new ExCompMstPk("com001"), "1")));
        transactionManager.commitTransaction();

        List<ExclusiveCompMst> exclusiveCompMstList = VariousDbTestHelper.findAll(ExclusiveCompMst.class);
        assertThat(exclusiveCompMstList.size(), is(1));
        assertThat(exclusiveCompMstList.get(0).compId, is("com001"));
        assertThat(exclusiveCompMstList.get(0).version, is(2L));

        /****************************************************************
        バージョン番号を取得する前にバージョン番号の更新メソッド(悲観的ロック)が呼ばれた場合
        不正な呼び出し順でないので更新できる。
        ****************************************************************/

        VariousDbTestHelper.setUpTable(new ExclusiveUserMst("uid001", "pk2001", "pk3001", 1L));

        manager.updateVersion(new ExUserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        List<ExclusiveUserMst> exclusiveUserMstList = VariousDbTestHelper.findAll(ExclusiveUserMst.class);
        assertThat(exclusiveUserMstList.size(), is(1));
        assertThat(exclusiveUserMstList.get(0).userId, is("uid001"));
        assertThat(exclusiveUserMstList.get(0).pk2, is("pk2001"));
        assertThat(exclusiveUserMstList.get(0).pk3, is("pk3001"));
        assertThat(exclusiveUserMstList.get(0).version, is(2L));
    }

    /**
     * 排他制御用テーブルと業務テーブルを同一とした場合の
     * バージョン番号の更新チェックを伴う更新をテストする。
     */
    @Test
    public void testUsingBusinessTable() throws SQLException {

        ExclusiveControlManager manager = new BasicExclusiveControlManager();

        Map<String, Object> data;
        int updateCount;


        /****************************************************************
        楽観的ロックを使用して業務データを更新できること。
        ****************************************************************/
        VariousDbTestHelper.setUpTable(new UserMst("uid001", "pk2001", "pk3001", "test_user_001", 1L));

        List<UserMst> userMstList = VariousDbTestHelper.findAll(UserMst.class);
        assertThat(userMstList.size(), is(1));
        assertThat(userMstList.get(0).userId, is("uid001"));
        assertThat(userMstList.get(0).name, is("test_user_001"));
        assertThat(userMstList.get(0).version, is(1L));

        // バージョン番号の準備
        Version version = manager.getVersion(new UserMstPk("uid001", "pk2001", "pk3001"));
        transactionManager.commitTransaction();

        // バージョン番号と業務データの更新
        manager.updateVersionsWithCheck(Arrays.asList(version));

        data = new HashMap<String, Object>();
        data.put("name", "test_user_001_changed");
        data.put("user_id", "uid001");
        data.put("pk2", "pk2001");
        data.put("pk3", "pk3001");
        updateCount = new DbAccessSupport(BasicExclusiveControlManagerTest.class)
                                .getParameterizedSqlStatement("UPDATE_USER_NAME", data)
                                .executeUpdateByMap(data);
        assertThat(updateCount, is(1));

        transactionManager.commitTransaction();

        userMstList = VariousDbTestHelper.findAll(UserMst.class);
        assertThat(userMstList.size(), is(1));
        assertThat(userMstList.get(0).userId, is("uid001"));
        assertThat(userMstList.get(0).name, is("test_user_001_changed"));
        assertThat(userMstList.get(0).version, is(2L));

        /****************************************************************
        悲観的ロックを使用して業務データを更新できること。
        ****************************************************************/

        VariousDbTestHelper.setUpTable(new UserMst("uid001", "pk2001", "pk3001", "test_user_001", 12L));

        userMstList = VariousDbTestHelper.findAll(UserMst.class);
        assertThat(userMstList.size(), is(1));
        assertThat(userMstList.get(0).userId, is("uid001"));
        assertThat(userMstList.get(0).name, is("test_user_001"));
        assertThat(userMstList.get(0).version, is(12L));

        // バージョン番号と業務データの更新
        manager.updateVersion(new UserMstPk("uid001", "pk2001", "pk3001"));

        data = new HashMap<String, Object>();
        data.put("name", "test_user_001_changed");
        data.put("user_id", "uid001");
        data.put("pk2", "pk2001");
        data.put("pk3", "pk3001");
        updateCount = new DbAccessSupport(BasicExclusiveControlManagerTest.class)
                        .getParameterizedSqlStatement("UPDATE_USER_NAME", data)
                        .executeUpdateByMap(data);
        assertThat(updateCount, is(1));

        transactionManager.commitTransaction();

        userMstList = VariousDbTestHelper.findAll(UserMst.class);
        assertThat(userMstList.size(), is(1));
        assertThat(userMstList.get(0).userId, is("uid001"));
        assertThat(userMstList.get(0).name, is("test_user_001_changed"));
        assertThat(userMstList.get(0).version, is(13L));
    }
}
