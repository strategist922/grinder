package org.jauntsy.grinder.karma.scripting;

import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.mapred.Formatter;
import org.jauntsy.grinder.karma.mapred.Reducer;

/**
 * User: ebishop
 * Date: 1/16/13
 * Time: 11:47 AM
 */
public interface ScriptFactory {
    public Mapper buildMapper(String script);
    public Reducer buildReducer(String script);
    public Formatter buildFormatter(String script);
}
