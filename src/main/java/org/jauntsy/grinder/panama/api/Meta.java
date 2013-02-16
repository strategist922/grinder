package org.jauntsy.grinder.panama.api;

import java.util.List;

/**
 * User: ebishop
 * Date: 12/12/12
 * Time: 4:30 PM
 */
public class Meta {
    private String table;
    private List id;
    private String rev;
    private long updatedAt;
    private String type;
    private int flags;
    private long expiration;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List getId() {
        return id;
    }

    public void setId(List id) {
        this.id = id;
    }

    public String getRev() {
        return rev;
    }

    public void setRev(String rev) {
        this.rev = rev;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
}
