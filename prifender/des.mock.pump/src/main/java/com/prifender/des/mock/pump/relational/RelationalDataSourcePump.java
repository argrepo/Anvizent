package com.prifender.des.mock.pump.relational;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.prifender.des.client.ApiClient;
import com.prifender.des.client.DefaultApi;
import com.prifender.des.client.model.ConnectionParam;
import com.prifender.des.client.model.DataSource;
import com.prifender.des.mock.pump.Config;
import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;

@Component
public final class RelationalDataSourcePump
{
    @Autowired
    private Config config;
    
    @Autowired
    protected RelationalDatabaseClient database;

    public void run() throws Exception
    {
        // Connect to Mock DES
        
        final DefaultApi mockDes = new DefaultApi( new ApiClient().setBasePath( this.config.mockDesUrl ) );
        
        // Add a data source
        
        final DataSource ds = mockDes.addDataSource
        (
            new DataSource()
                .type( "relational.database.mock" )
                .label( "relational" )
                .addConnectionParamsItem( new ConnectionParam().id( "user" ).value( "joe" ) )
                .addConnectionParamsItem( new ConnectionParam().id( "password" ).value( "somebody" ) )
                .addConnectionParamsItem( new ConnectionParam().id( "size" ).value( String.valueOf( this.config.mockDataSize ) ) )
        );
        
        new RelationalTablePump( this.config, this.database, mockDes, ds, EmployeesTable.INSTANCE ).pump();
        new RelationalTablePump( this.config, this.database, mockDes, ds, PhoneNumbersTable.INSTANCE ).pump();
        new RelationalTablePump( this.config, this.database, mockDes, ds, AddressesTable.INSTANCE ).pump();
        new RelationalTablePump( this.config, this.database, mockDes, ds, EmployeeToAddressTable.INSTANCE ).pump();
        
        if( ! this.config.mockDataMinimal )
        {
            new RelationalTablePump( this.config, this.database,  mockDes, ds, PayChecksTable.INSTANCE ).pump();
        }
        
        System.out.println( "Applying foreign keys..." );
        
        new RelationalTablePump( this.config, this.database, mockDes, ds, EmployeesTable.INSTANCE ).applyForeignKeys();
        new RelationalTablePump( this.config, this.database, mockDes, ds, PhoneNumbersTable.INSTANCE ).applyForeignKeys();
        new RelationalTablePump( this.config, this.database, mockDes, ds, AddressesTable.INSTANCE ).applyForeignKeys();
        new RelationalTablePump( this.config, this.database, mockDes, ds, EmployeeToAddressTable.INSTANCE ).applyForeignKeys();
        
        if( ! this.config.mockDataMinimal )
        {
            new RelationalTablePump( this.config, this.database, mockDes, ds, PayChecksTable.INSTANCE ).applyForeignKeys();
        }
    }

}
