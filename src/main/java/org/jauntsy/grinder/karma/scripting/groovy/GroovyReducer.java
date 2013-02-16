package org.jauntsy.grinder.karma.scripting.groovy;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.karma.mapred.SimpleTuple;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import org.codehaus.groovy.control.CompilerConfiguration;

import java.util.List;
import java.util.Map;

/**
 * User: ebishop
 * Date: 1/10/13
 * Time: 11:36 AM
 */
public class GroovyReducer extends Reducer {

    private final String reduceScript;

    private transient Closure reduceClosure;
    private transient int pamCount;

    @Override
    public void prepare(Map stormConfig, TopologyContext context) {
        this.reduceClosure = compileClosure();
        this.pamCount = reduceClosure.getParameterTypes().length;
    }

    private Closure compileClosure() {
        CompilerConfiguration compiler = new CompilerConfiguration();
        GroovyShell shell = new GroovyShell();
        registerTupleTypes(shell);
        return (Closure)shell.evaluate(reduceScript);
    }

    // *** TODO: this should throw errors if the wrong field name is requested
    private void registerTupleTypes(GroovyShell shell) {
        shell.evaluate(TupleImpl.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
        shell.evaluate(TupleImpl.class.getName() + ".metaClass.getAt = { idx -> return getValue(idx) }");
        shell.evaluate(SimpleTuple.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
        shell.evaluate(SimpleTuple.class.getName() + ".metaClass.getAt = { idx -> return getValue(idx) }");
    }

    GroovyReducer(String reduceScript) {
        this.reduceScript = reduceScript;
        compileClosure();
    }

    @Override
    public List reduce(Tuple key, Tuple a, Tuple b) {
        try {
            switch(pamCount) {
                case 2:
                    return (List)reduceClosure.call(a, b);
                default:
                    return (List)reduceClosure.call(key, a, b);
            }
        } catch(RuntimeException ex) {
            throw new RuntimeException("Error while processing script: " + reduceScript + ", with a: " + a + ", b: " + b);
        }
    }

}
