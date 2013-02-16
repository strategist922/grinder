package org.jauntsy.grinder.karma;

import java.util.Collection;

/**
 * User: ebishop
 * Date: 1/3/13
 * Time: 2:51 PM
 */
public abstract class Emitter {
//    public abstract void emit(List key, List values);
    public abstract void emit(Object... values);

    public void emitAll(Collection values) {
        emit(values.toArray());
    }

}
