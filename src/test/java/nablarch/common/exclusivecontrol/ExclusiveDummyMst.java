package nablarch.common.exclusivecontrol;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 排他ダミーマスタ
 */
@Entity
@Table(name = "EXCLUSIVE_DUMMY_MST")
public class ExclusiveDummyMst {
    
    public ExclusiveDummyMst() {
    };
    
    public ExclusiveDummyMst(String pk1, Long version) {
        this.pk1 = pk1;
        this.version = version;
    }

    @Id
    @Column(name = "PK1", length = 6, nullable = false)
    public String pk1;
    
    @Column(name = "VERSION", length = 10, nullable = false)
    public Long version;
}
