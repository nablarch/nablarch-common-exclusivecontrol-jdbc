package nablarch.common.exclusivecontrol;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
