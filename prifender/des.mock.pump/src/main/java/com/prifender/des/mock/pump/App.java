package com.prifender.des.mock.pump;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.prifender.des.mock.pump.perf.OptimalBatchSizeProbe;
import com.prifender.des.mock.pump.perf.OptimalInsertStrategyProbe;
import com.prifender.des.mock.pump.relational.RelationalDataSourcePump;
import com.prifender.des.mock.pump.relational.RelationalTablePumpProcess;
import com.prifender.des.mock.pump.relational.client.MySqlClient;
import com.prifender.des.mock.pump.relational.client.OracleClient;
import com.prifender.des.mock.pump.relational.client.PostgreSqlClient;
import com.prifender.des.mock.pump.relational.client.SqlServerClient;

@SpringBootApplication
@ComponentScan("com.prifender")
public class App implements CommandLineRunner{
    
    @Autowired
    private RelationalDataSourcePump relationalDataSourcePump;
    
    @Autowired
    private RelationalTablePumpProcess relationalTablePumpProcess;
    
    @Autowired
    private OptimalInsertStrategyProbe optimalInsertStrategyProbe;
    
    @Autowired
    private OptimalBatchSizeProbe optimalBatchSizeProbe;
    
	public static void main( final String[] args ) throws Exception
	{
	    final String databaseUrl = System.getProperty( "database.url" );
	    
	    if( databaseUrl == null )
	    {
	        throw new IllegalArgumentException( "Property database.url has not been specified" );
	    }
	    
	    final String profile;
	    
	    if( databaseUrl.startsWith( MySqlClient.JDBC_PREFIX ) )
	    {
	        profile = "MySql";
	    }
	    else if( databaseUrl.startsWith( OracleClient.JDBC_PREFIX ) )
        {
            profile = "Oracle";
        }
        else if( databaseUrl.startsWith( PostgreSqlClient.JDBC_PREFIX ) )
        {
            profile = "PostgreSql";
        }
        else if( databaseUrl.startsWith( SqlServerClient.JDBC_PREFIX ) )
        {
            profile = "SqlServer";
        }
        else
        {
            throw new IllegalArgumentException( "Unknown database type in " + databaseUrl );
        }
	    
	    System.setProperty( "spring.profiles.active", profile );

	    SpringApplication app = new SpringApplication(App.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        
        if( args.length == 0 )
        {
            this.relationalDataSourcePump.run();
        }
        else if( args.length == 1 && args[ 0 ].equals( "-probeOptimalInsertStrategy" ) )
        {
            this.optimalInsertStrategyProbe.run();
        }
        else if( args.length == 2 && args[ 0 ].equals( "-probeOptimalBatchSize" ) )
        {
            this.optimalBatchSizeProbe.run( args[ 1 ] );
        }
        else if( args.length == 3 && args[ 0 ].equals( "-t" ) )
        {
            this.relationalTablePumpProcess.run( args[ 1 ], args[ 2 ] );
        }
        else
        {
            System.out.println( "Invalid program parameters" );
        }
    }
	
}
