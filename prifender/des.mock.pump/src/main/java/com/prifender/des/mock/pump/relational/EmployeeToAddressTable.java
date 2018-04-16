package com.prifender.des.mock.pump.relational;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;

public final class EmployeeToAddressTable extends Table {

    public static final Table INSTANCE = new EmployeeToAddressTable();
    
    private EmployeeToAddressTable() {}

    @Override
    public String collection() {
        return "EmployeeToAddress";
    }

    @Override
    public int collectionSizeMultiple()
    {
        return 2;
    }

    @Override
    public List<String> attributes() {
        final List<String> attributes = new ArrayList<String>();

        attributes.add("employee_id");
        attributes.add("address_id");

        return Collections.unmodifiableList(attributes);
    }

    @Override
    public void copyDataIntoPreparedStatement( final JsonObject json, final PreparedStatement preparedStatement ) throws SQLException
    {
        preparedStatement.setInt(1, json.get("employee_id").getAsInt());
        preparedStatement.setInt(2, json.get("address_id").getAsInt());
    }

    @Override
    public String createTableDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE ").append(tableNamespace).append('.').append(collection()).append(" ( ");
        buf.append("employee_id INTEGER NOT NULL, ");
        buf.append("address_id INTEGER NOT NULL, ");
        buf.append("PRIMARY KEY ( employee_id, address_id )");
        buf.append(");");

        return buf.toString();
    }

    @Override
    public String foreignKeysDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("ALTER TABLE ").append(tableNamespace).append('.').append(collection()).append(' ');
        buf.append("ADD FOREIGN KEY ( employee_id ) REFERENCES ").append(tableNamespace).append(".Employees( id ); " );
        
        buf.append("ALTER TABLE ").append(tableNamespace).append('.').append(collection()).append(' ');
        buf.append("ADD FOREIGN KEY ( address_id ) REFERENCES ").append(tableNamespace).append(".Addresses( id );" );
        
        return buf.toString();
    }

}
