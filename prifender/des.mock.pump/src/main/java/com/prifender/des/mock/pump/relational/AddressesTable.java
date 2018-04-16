package com.prifender.des.mock.pump.relational;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;

public final class AddressesTable extends Table {
    
    public static final Table INSTANCE = new AddressesTable();
    
    private AddressesTable() {}

    @Override
    public String collection() {
        return "Addresses";
    }

    @Override
    public int collectionSizeMultiple()
    {
        return 2;
    }

    @Override
    public List<String> attributes() {
        final List<String> attributes = new ArrayList<String>();

        attributes.add("id");
        attributes.add("street");
        attributes.add("city");
        attributes.add("region");
        attributes.add("postal_code");
        attributes.add("country");

        return Collections.unmodifiableList(attributes);
    }

    @Override
    public void copyDataIntoPreparedStatement( final JsonObject json, final PreparedStatement preparedStatement ) throws SQLException
    {
        preparedStatement.setInt(1, json.get("id").getAsInt());
        preparedStatement.setString(2, json.get("street").getAsString());
        preparedStatement.setString(3, json.get("city").getAsString());
        preparedStatement.setString(4, json.get("region").getAsString());
        preparedStatement.setString(5, json.get("postal_code").getAsString());
        preparedStatement.setString(6, json.get("country").getAsString());
    }

    @Override
    public String createTableDdl(final String tableNamespace) {
        final StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE ").append(tableNamespace).append('.').append(collection()).append(" ( ");
        buf.append("id INTEGER NOT NULL, ");
        buf.append("street VARCHAR(150), ");
        buf.append("city VARCHAR(50), ");
        buf.append("region VARCHAR(2), ");
        buf.append("postal_code VARCHAR(15), ");
        buf.append("country VARCHAR(15), ");
        buf.append("PRIMARY KEY ( id ) ");
        buf.append(");");

        return buf.toString();
    }

}
