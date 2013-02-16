package org.jauntsy.grinder.karma.scripting.groovy;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import org.jauntsy.grinder.karma.Emitter;
import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.mapred.SimpleTuple;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import org.codehaus.groovy.control.CompilerConfiguration;

import java.util.Map;

/**
 * User: ebishop
 * Date: 1/10/13
 * Time: 11:36 AM
 */
public class GroovyMapper extends Mapper {

    private final String mapScript;

    private transient Closure mapClosure;
    private transient Delegate delegate;
    private transient Emitter emitter;
    private transient int numParameters;

    GroovyMapper(String mapScript) {
        this.mapScript = mapScript;
        compleClosure();
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext context) {
        Closure evaluate = compleClosure();
        this.mapClosure = evaluate;
        this.delegate = new Delegate();
        this.numParameters = evaluate.getParameterTypes().length;
        if (numParameters < 0 || numParameters > 1)
            throw new IllegalArgumentException("Closure must accept 0 or 1 parameters");
    }

    private Closure compleClosure() {
        CompilerConfiguration compiler = new CompilerConfiguration();
        GroovyShell shell = new GroovyShell(compiler);
        registerTupleTypes(shell);
        return (Closure) shell.evaluate(mapScript);
    }

    private void registerTupleTypes(GroovyShell shell) {
//        shell.evaluate(TupleImpl.class.getName() + ".metaClass.getProperty = { name -> getValueByField(name) }");
//        shell.evaluate(SimpleTuple.class.getName() + ".metaClass.getProperty = { name -> getValueByField(name) }");
        shell.evaluate(TupleImpl.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
        shell.evaluate(SimpleTuple.class.getName() + ".metaClass.getProperty = { name -> if (contains(name)) getValueByField(name); else throw new NoSuchFieldError(name) }");
    }

    /*
    TODO: use a delegate to e is not passed
     */
    @Override
    public void map(Tuple value, Emitter e) {
        try {
            this.emitter = e;
            this.mapClosure.setDelegate(delegate);
            switch(numParameters) {
                case 0:
                    this.mapClosure.call();
                    break;
                case 1:
                    this.mapClosure.call(value);
                    break;
                default:
                    throw new RuntimeException("Invalid number of parameters");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error while mapping tuple: fields: " + value.getFields() + ", value: " + value.getValues() + ", script: " + mapScript, ex);
        }
    }

    public class Delegate {
        public void emit(Object... values) {
            emitter.emit(values);
        }
    }

}
