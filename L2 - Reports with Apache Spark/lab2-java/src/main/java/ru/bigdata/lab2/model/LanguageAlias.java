package ru.bigdata.lab2.model;

import java.io.Serializable;
import java.util.Objects;

public class LanguageAlias implements Serializable {

    private String canonicalName;
    private String alias;

    public LanguageAlias() {
    }

    public LanguageAlias(String canonicalName, String alias) {
        this.canonicalName = canonicalName;
        this.alias = alias;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    public void setCanonicalName(String canonicalName) {
        this.canonicalName = canonicalName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LanguageAlias that)) {
            return false;
        }
        return Objects.equals(canonicalName, that.canonicalName) && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalName, alias);
    }
}
