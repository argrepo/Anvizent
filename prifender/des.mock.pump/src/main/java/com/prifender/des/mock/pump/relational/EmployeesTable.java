package com.prifender.des.mock.pump.relational;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;

public final class EmployeesTable extends Table {

    public static final Table INSTANCE = new EmployeesTable();
    
    private EmployeesTable() {}

    @Override
    public String collection() {
        return "Employees";
    }

    @Override
    public List<String> attributes() {
        final List<String> attributes = new ArrayList<String>();

        attributes.add("id");
        attributes.add("first_name");
        attributes.add("last_name");
        attributes.add("ssn");
        attributes.add("email");
        attributes.add("hire_date");
        attributes.add("dob");
        attributes.add("nationality");
        attributes.add("salary");
        attributes.add("commission_pct");
        attributes.add("manager_id");

        return Collections.unmodifiableList(attributes);
    }

    @Override
    public void copyDataIntoPreparedStatement(final JsonObject json, final PreparedStatement preparedStatement)
            throws SQLException {
        preparedStatement.setInt(1, json.get("id").getAsInt());
        preparedStatement.setString(2, json.get("first_name").getAsString());
        preparedStatement.setString(3, json.get("last_name").getAsString());
        preparedStatement.setString(4, json.get("ssn").getAsString());
        preparedStatement.setString(5, json.get("email").getAsString());
        preparedStatement.setString(6, json.get("hire_date").getAsString());
        preparedStatement.setString(7, json.get("dob").getAsString());
        preparedStatement.setString(8, json.get("nationality").getAsString());
        preparedStatement.setBigDecimal(9, json.get("salary").getAsBigDecimal());
        preparedStatement.setBigDecimal(10, json.get("commission_pct").getAsBigDecimal());
        preparedStatement.setInt(11, json.get("manager_id").getAsInt());
    }

    @Override
    public String createTableDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE ").append(tableNamespace).append('.').append(collection()).append(" ( ");
        buf.append("id INTEGER NOT NULL, ");
        buf.append("first_name VARCHAR(60), ");
        buf.append("last_name VARCHAR(60), ");
        buf.append("ssn VARCHAR(15), ");
        buf.append("email VARCHAR(50), ");
        buf.append("hire_date DATE, ");
        buf.append("dob DATE, ");
        buf.append("nationality VARCHAR(15), ");
        buf.append("salary DECIMAL, ");
        buf.append("commission_pct DECIMAL, ");
        buf.append("manager_id INTEGER, ");
        buf.append("PRIMARY KEY ( id ) ");
        buf.append(");");

        return buf.toString();
    }

    @Override
    public String foreignKeysDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("ALTER TABLE ").append(tableNamespace).append('.').append(collection()).append(' ');
        buf.append("ADD FOREIGN KEY ( manager_id ) REFERENCES ").append(tableNamespace).append(".Employees( id );" );

        return buf.toString();
    }

}
