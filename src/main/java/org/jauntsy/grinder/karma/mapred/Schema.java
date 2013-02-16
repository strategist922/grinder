package org.jauntsy.grinder.karma.mapred;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * User: ebishop
 * Date: 1/11/13
 * Time: 1:30 PM
 */
public class Schema {

    String name;
    boolean nameIsSet = false;

    List<String> idFields;
    boolean idFieldsAreSet = false;

    List<String> valueFields;
    boolean valueFieldsAreSet = false;

    Schema() {

    }

    public Schema(String name, List<String> idFields, List<String> valueFields) {
        this.name = name;
        this.nameIsSet = true;
        this.idFields = idFields;
        this.idFieldsAreSet = true;
        this.valueFields = valueFields;
        this.valueFieldsAreSet = true;
    }
//    List<Schema> subTables; TODO: add sub-tables

    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    private void toString(StringBuilder sb) {
        sb.append(name);
        if (idFieldsAreSet) {
            sb.append("(");
            boolean first = true;
            for (String id : idFields) {
                if (!first) sb.append(" "); else first = false;
                sb.append(id);
            }
            sb.append(")");
        }
        if (valueFieldsAreSet) {
            sb.append(" {");
            boolean first = true;
            for (String col : valueFields) {
                if (!first) sb.append(" "); else first = false;
                sb.append(col);
            }
            sb.append("}");
        }
    }

    /*
    eg. Schema.parseSignature("(id) { name age }")
     */
    public static Schema parseSignature(String script) {
        Schema schema = new Schema();
        List<String> tokens = Tokenizer.tokenize(script);
        parseSignature(schema, tokens);
        return schema;
    }

    public static Schema parse(String script) {
        Schema schema = new Schema();
        List<String> tokens = Tokenizer.tokenize(script);
        parse(schema, tokens);
        return schema;
    }

    public static Schema parseSignature(String name, String signature) {
        Schema ret = new Schema();
        ret.name = name;
        ret.nameIsSet = true;
        parseSignature(ret, Tokenizer.tokenize(signature));
        return ret;
    }

    private static void parse(Schema schema, List<String> tokens) {
        String name = tokens.remove(0);
        if (!Character.isJavaIdentifierStart(name.charAt(0)))
            throw new IllegalArgumentException("Bad character '" + name.charAt(0) + "'; expected identifier start character.");
        schema.name = name;
        schema.nameIsSet = true;
        parseSignature(schema, tokens);

    }

    private static void parseSignature(Schema schema, List<String> tokens) {
        int mode = 0;
        String leftParen = tokens.remove(0);
        if ("(".equals(leftParen)) {
            mode = 1;
            schema.idFieldsAreSet = true;
        } else if ("{".equals(leftParen)) {
            mode = 2;
            schema.valueFieldsAreSet = true;
        } else {
            throw new IllegalArgumentException("Left paren '(' or brace '{' expected, got: " + leftParen);
        }
        List<String> ids = new ArrayList<String>();
        List<String> columns = new ArrayList<String>();
        while (true) {
            String next = tokens.remove(0);
            if (mode == 1) {
                if (")".equals(next)) {
//                    if (ids.size() < 1)
//                        throw new IllegalArgumentException("At least one id field is required");
                    schema.idFields = ids;
                    if (tokens.size() == 0)
                        break; // All good, no value fields
                    String lefBrace = tokens.remove(0);
                    if (!"{".equals(lefBrace))
                        throw new IllegalArgumentException("Left brace '{' expected.");
                    schema.valueFieldsAreSet = true;
                    mode = 2;
                    continue;
                } else if (Character.isJavaIdentifierStart(next.charAt(0))) {
                    ids.add(next);
                } else {
                    throw new IllegalArgumentException("id field name or right paren ')' expected, got: " + next);
                }
            } else if (mode == 2) {
                if ("}".equals(next)) {
                    if (columns.size() < 1)
                        throw new IllegalArgumentException("At least one column is required"); // ** hmm, dont think so
                    schema.valueFields = columns;
                    break;
                } else if (Character.isJavaIdentifierStart(next.charAt(0))) {
                    columns.add(next);
                } else {
                    throw new IllegalArgumentException("column name or right brace '}' expected");
                }
            } else {
                throw new IllegalStateException("unknown mode: " + mode);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(Tokenizer.tokenize("This is a test"));
        System.out.println(Tokenizer.tokenize("user[i] { name,age addresses[id] {city state}}"));
        System.out.println(Schema.parse("org(id) { name }"));
        System.out.println(Schema.parse("sortedNames (count name) { name count,,}"));
    }

    public String getName() {
        return name;
    }

    public List<String> getIdFields() {
        return idFields;
    }

    public List<String> getValueFields() {
        return valueFields;
    }

    public boolean isIdFieldsAreSet() {
        return idFieldsAreSet;
    }

    public List<String> getAllFields() {
        List<String> ret = new ArrayList<String>();
        if (idFieldsAreSet)
            ret.addAll(idFields);
        if (valueFieldsAreSet)
            ret.addAll(valueFields);
        return ret;
    }

    private static class Tokenizer {
        public static List<String> tokenize(String text) {
            List<String> tokens = new LinkedList<String>();
            tokenize(tokens, text.toCharArray());
            return tokens;
        }

        private static void tokenize(List<String> tokens, char[] chars) {
            StringBuilder sb = null;
            int mode = 0;
            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];
                if (mode == 0) {
                    if (Character.isWhitespace(c) || ',' == c) {
                        // ignore
                    } else {
                        if (Character.isJavaIdentifierStart(c)) {
                            sb = new StringBuilder();
                            sb.append(c);
                            mode = 1;
                        } else {
                            tokens.add(String.valueOf(c));
                        }
                    }
                } else if (mode == 1) {
                    if (Character.isJavaIdentifierPart(c)) {
                        sb.append(c);
                    } else {
                        tokens.add(sb.toString());
                        sb = null;
                        mode = 0;
                        i--;
                    }
                }
            }
            if (sb != null && sb.length() > 0)
                tokens.add(sb.toString());
        }

    }

}
