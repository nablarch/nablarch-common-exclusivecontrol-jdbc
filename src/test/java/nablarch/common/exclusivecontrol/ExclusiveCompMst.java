package nablarch.common.exclusivecontrol;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 排他カードマスタ3
 */
@Entity
@Table(name = "EXCLUSIVE_COMP_MST")
public class ExclusiveCompMst {
    
    public ExclusiveCompMst() {
    };
    
    public ExclusiveCompMst(String compId, Long version) {
        this.compId = compId;
        this.version = version;
    }

    @Id
    @Column(name = "COMP_ID", length = 6, nullable = false)
    public String compId;
    
    @Column(name = "VERSION", length = 10, nullable = false)
    public Long version;
}
