package org.jauntsy.grinder.karma.mapred.builder;

import org.jauntsy.grinder.karma.mapred.Mapper;
import org.jauntsy.grinder.karma.mapred.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: ebishop
 * Date: 1/16/13
 * Time: 2:56 PM
 */
public class Sources implements Iterable<ViewBuilder.Source> {

    private final List<ViewBuilder.Source> list;

    public Sources(String name, String srcComponentId, List<String> docIdFields, List<String> docValueFields, Mapper mapper, Integer parallelismHint) {
        this.list = new ArrayList<ViewBuilder.Source>();
        add(name, srcComponentId, docIdFields, docValueFields, mapper, parallelismHint);
    }

    public Sources(String query, Mapper mapper, Integer parallelismHint) {
        this.list = new ArrayList<ViewBuilder.Source>();
        Schema schema = Schema.parse(query);
        add(null, schema.getName(), schema.getIdFields(), schema.getValueFields(), mapper, parallelismHint);
    }

    public ViewBuilder.Source add(String name, String srcComponentId, List<String> docIdFields, List<String> docValueFields, Mapper mapper, Integer parallelismHint) {
        ViewBuilder.Source source = new ViewBuilder.Source(
                name != null ? name : String.valueOf(list.size()),
                srcComponentId,
                docIdFields,
                docValueFields,
                mapper,
                parallelismHint
        );
        list.add(source);
        return source;
    }

    public ViewBuilder.Source getLastSource() {
        return list.get(list.size() - 1);
    }

    @Override
    public Iterator<ViewBuilder.Source> iterator() {
        return list.iterator();
    }

}
