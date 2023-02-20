package nablarch.common.exclusivecontrol;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * ユーザマスタ
 */
@Entity
@Table(name = "USER_MST")
public class UserMst {
    
    public UserMst() {
    };
    
    public UserMst(String userId, String pk2, String pk3, String name, Long version) {
        this.userId = userId;
        this.pk2 = pk2;
        this.pk3 = pk3;
        this.name = name;
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
    
    @Column(name = "NAME", length = 40)
    public String name;
    
    @Column(name = "VERSION", length = 10, nullable = false)
    public Long version;
}
