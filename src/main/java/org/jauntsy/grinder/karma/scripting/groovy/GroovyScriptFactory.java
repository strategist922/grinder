package org.jauntsy.grinder.karma.scripting.groovy;

import org.jauntsy.grinder.karma.mapred.*;
import org.jauntsy.grinder.karma.scripting.ScriptFactory;

/**
 * User: ebishop
 * Date: 1/16/13
 * Time: 11:49 AM
 */
public class GroovyScriptFactory implements ScriptFactory {
    @Override
    public Mapper buildMapper(String script) {
        return new GroovyMapper(script);
    }

    @Override
    public Reducer buildReducer(String script) {
        return new GroovyReducer(script);
    }

    @Override
    public Formatter buildFormatter(String script) {
        return new GroovyFormatter(script);
    }
}
