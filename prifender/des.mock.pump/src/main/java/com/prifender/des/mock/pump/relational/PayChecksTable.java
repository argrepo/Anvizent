package com.prifender.des.mock.pump.relational;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;

public final class PayChecksTable extends Table {

    public static final Table INSTANCE = new PayChecksTable();
    
    private PayChecksTable() {}

    @Override
    public String collection() {
        return "PayChecks";
    }

    @Override
    public int collectionSizeMultiple()
    {
        return 10;
    }

    @Override
    public List<String> attributes() {
        final List<String> attributes = new ArrayList<String>();

        attributes.add("id");
        attributes.add("employee_id");
        attributes.add("payment_date");
        attributes.add("amount");

        return Collections.unmodifiableList(attributes);
    }

    @Override
    public void copyDataIntoPreparedStatement( final JsonObject json, final PreparedStatement preparedStatement ) throws SQLException
    {
        preparedStatement.setInt(1, json.get("id").getAsInt());
        preparedStatement.setInt(2, json.get("employee_id").getAsInt());
        preparedStatement.setString(3, json.get("payment_date").getAsString());
        preparedStatement.setBigDecimal(4, json.get("amount").getAsBigDecimal());
    }

    @Override
    public String createTableDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE ").append(tableNamespace).append('.').append(collection()).append(" ( ");
        buf.append("id INTEGER NOT NULL, ");
        buf.append("employee_id INTEGER NOT NULL, ");
        buf.append("payment_date date, ");
        buf.append("amount DECIMAL, ");
        buf.append("PRIMARY KEY ( id ) ");
        buf.append(");");

        return buf.toString();
    }

    @Override
    public String foreignKeysDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("ALTER TABLE ").append(tableNamespace).append('.').append(collection()).append(' ');
        buf.append("ADD FOREIGN KEY ( employee_id ) REFERENCES ").append(tableNamespace).append(".Employees( id );" );
        
        return buf.toString();
    }

}
