package org.streamreasoning.rsp4j.yasper.querying.operators.r2r;

import org.apache.commons.rdf.api.RDFTerm;
import org.streamreasoning.rsp4j.api.RDFUtils;

import java.util.Objects;

public class VarImplJGraphT implements VarOrTerm {

    private final String name;

    public VarImplJGraphT(String name) {
        this.name = "<"+name+">";
    }


    @Override
    public String getIRIString() {
        String tmp = this.name.replace("<","");
        tmp = tmp.replace(">","");
        return tmp;
    }

    @Override
    public String ntriplesString() {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean bind(Binding b, RDFTerm t) {
        return b.add(this, t);
    }

    @Override
    public boolean isVariable() {
        return true;
    }

    @Override
    public boolean isTerm() {
        return false;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VarImplJGraphT var = (VarImplJGraphT) o;
        return Objects.equals(name, var.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
