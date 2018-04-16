package com.prifender.des.mock.pump.relational;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.google.gson.JsonObject;

public abstract class Table
{
    public abstract String collection();
    
    public int collectionSizeMultiple()
    {
        return 1;
    }
    
    public abstract List<String> attributes();
    
    public abstract String createTableDdl( String tableNamespace );
    
    public String foreignKeysDdl( final String tableNamespace )
    {
        return null;
    }
    
    public abstract void copyDataIntoPreparedStatement( JsonObject json, PreparedStatement preparedStatement ) throws SQLException;

}
