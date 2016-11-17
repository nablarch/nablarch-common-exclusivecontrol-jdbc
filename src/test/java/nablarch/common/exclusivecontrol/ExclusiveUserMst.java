package nablarch.common.exclusivecontrol;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 排他ユーザマスタ
 */
@Entity
@Table(name = "EXCLUSIVE_USER_MST")
public class ExclusiveUserMst {
    
    public ExclusiveUserMst() {
    };
    
    public ExclusiveUserMst(String userId, String pk2, String pk3, Long version) {
        this.userId = userId;
        this.pk2 = pk2;
        this.pk3 = pk3;
        this.version = version;
    }

    @Id
    @Column(name = "USER_ID", length = 6, nullable = false)
    public String userId;
    
    @Id
    @Column(name = "PK2", length = 6, nullable = false)
    public String pk2;
    
    @Id
    @Column(name = "PK3", length = 6, nullable = false)
    public String pk3;
    
    @Column(name = "VERSION", length = 10, nullable = false)
    public Long version;
}
