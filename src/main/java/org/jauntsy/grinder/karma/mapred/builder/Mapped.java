package org.jauntsy.grinder.karma.mapred.builder;

import org.jauntsy.grinder.karma.mapred.KarmaBuilderContext;
import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.mapred.Reducer;
import org.jauntsy.grinder.karma.mapred.Schema;
import org.jauntsy.grinder.karma.operations.map.SelectAll;

import java.util.ArrayList;
import java.util.List;

import static org.jauntsy.nice.Nice.addall;

/**
 * User: ebishop
 * Date: 1/11/13
 * Time: 11:02 AM
 */
public class Mapped {

    private final KarmaBuilderContext context;
    private final Sources sources;

    public Mapped(KarmaBuilderContext context, String srcName, String srcComponentId, List<String> idFields, List<String> valueFields, Mapper mapper, Integer parallelismHint) {
        this.context = context;
        this.sources = new Sources(srcName, srcComponentId, idFields, valueFields, mapper, parallelismHint);
    }

    public Mapped map(String values, String schema, String mapper) {
        return map(values, schema, context.buildMapper(mapper));
    }

    public Mapped map(String values, String schema, Mapper mapper) {
        Schema valueScheme = Schema.parseSignature(values);
        Schema tableScheme = Schema.parse(schema);
//        Schema src = context.parseInputSchema(srcStream, query);
        return map(new Schema(tableScheme.getName(), tableScheme.getIdFields(), valueScheme.getValueFields()), mapper);
    }

    public Mapped map(String projection, String schema) {
        return map(projection, schema, new SelectAll());
    }

    private Mapped map(Schema src, Mapper mapper) {
        return map(src.getName(), src.getIdFields(), src.getValueFields(), mapper);
    }

    public Mapped map(String srcComponentId, List<String> docIdFields, List<String> docValueFields, Mapper mapper) {
        return map(null, srcComponentId, docIdFields, docValueFields, mapper, null);
    }

    public Mapped map(String name, String srcComponentId, List<String> docIdFields, List<String> docValueFields, Mapper mapper, Integer parallelismHint) {
         sources.add(
                 name,
                 srcComponentId,
                 docIdFields,
                 docValueFields,
                 mapper,
                 parallelismHint
         );
        return this;
    }

    public Mapped parallelismHint(int parallelismHint) {
        sources.getLastSource().parallelismHint(parallelismHint);
        return this;
    }

    public Mapped name(String name) {
        sources.getLastSource().setName(name);
        return this;
    }

    public Reduced red(String values, String into, String using) {
        return red(values, into, context.buildReducer(using));
    }

    public Reduced red(String values, String into, Reducer using) {
        Schema valuesSchema = Schema.parseSignature(values);
        Schema tableScheme = Schema.parse(into);
        return context.replace(null, new Reduced(
                context,
                sources,
                addall(new ArrayList<String>(), tableScheme.getIdFields(), valuesSchema.getValueFields()),
                using,
                tableScheme.getName(),
                valuesSchema.getValueFields()
        ));
    }



}
