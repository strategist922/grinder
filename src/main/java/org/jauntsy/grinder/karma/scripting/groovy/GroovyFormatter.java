package org.jauntsy.grinder.karma.scripting.groovy;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import org.jauntsy.grinder.karma.mapred.Formatter;
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
public class GroovyFormatter extends Formatter {

    private final String mergeScript;

    private transient Closure mergeClosure;
    private transient int pamCount;

    @Override
    public void prepare(Map stormConfig, TopologyContext context) {
        Closure evaluate = compileClosure();
        this.mergeClosure = evaluate;
        this.pamCount = mergeClosure.getParameterTypes().length;
    }

    private Closure compileClosure() {
        CompilerConfiguration compiler = new CompilerConfiguration();
        GroovyShell shell = new GroovyShell();
        registerTupleTypes(shell);
        return (Closure) shell.evaluate(mergeScript);
    }

    // *** TODO: this should throw errors if the wrong field name is requested
    private void registerTupleTypes(GroovyShell shell) {
        shell.evaluate(TupleImpl.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
        shell.evaluate(SimpleTuple.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
    }

    GroovyFormatter(String mergeScript) {
        this.mergeScript = mergeScript;
        compileClosure();
    }

    @Override
    public List merge(Tuple d) {
        return (List)mergeClosure.call(d);
    }

}
