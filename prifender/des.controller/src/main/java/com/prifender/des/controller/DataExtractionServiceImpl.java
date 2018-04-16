package com.prifender.des.controller;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionService;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataSourceAdapter.StartResult;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionChunk;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionJob.StateEnum;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Type;
import com.prifender.des.util.JSONPersistanceUtil;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component( "dataExtractionService" )
public final class DataExtractionServiceImpl extends DataExtractionService
{
	@Value( "${des.home}" )
	private String desHome;
    
	@Autowired
    private List<DataSourceAdapter> adapters;

    @Autowired
    WorkQueueMonitoring workQueueMonitoring;
    @Autowired
    private MessagingConnectionFactory messagingConnectionFactory;
    
    protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);

    private final Map<DataExtractionJob,DataExtractionThread> jobToThreadMap = new IdentityHashMap<>();
    
    @Override
    public List<DataSourceType> getSupportedDataSourceTypes()
    {
        final List<DataSourceType> types = new ArrayList<DataSourceType>( this.adapters.size() );

        this.adapters.forEach( adapter -> types.add( adapter.getDataSourceType() ) );
        
        return types;
    }
    
    private DataSourceAdapter getAdapter( final String type )
    {
        return this.adapters.stream()
                .filter( adapter -> adapter.getDataSourceType().getId().equals( type ) )
                .findFirst()
                .get();
    }

    @Override
    protected ConnectionStatus testConnection( final DataSource ds ) throws DataExtractionServiceException
    {
        return getAdapter( ds.getType() ).testConnection( ds );
    }

    @Override
    protected Metadata getMetadata( final DataSource ds ) throws DataExtractionServiceException
    {
        return getAdapter( ds.getType() ).getMetadata( ds );
    }
    
    @Override
    protected synchronized DataExtractionJob startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec ) throws DataExtractionServiceException
	{
    	final DataSourceAdapter adapter = getAdapter( ds.getType() );
        final StartResult result = adapter.startDataExtractionJob( ds, spec,this.messagingConnectionFactory);
        this.jobToThreadMap.put( result.job, result.thread);
        return result.job;
	}

     @Override
    protected synchronized void deleteDataExtractionJob( final DataExtractionJob job ) throws DataExtractionServiceException
    {
    	 final DataExtractionThread thread =   (DataExtractionThread) this.jobToThreadMap.remove( job );
         
         if( thread != null )
         {
             thread.cancel();
         }
    }
    public static boolean isValidTable( Metadata metadata,final String tableName )
    {
        for( final NamedType table : metadata.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                return true;
            }
        }
        
        return false;
    }
    
    public static boolean isValidColumn(Metadata metadata, final String tableName, final String columnName )
    {
        for( final NamedType table : metadata.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                final Type entryType = table.getType().getEntryType();
                
                for( final NamedType column : entryType.getAttributes() )
                {
                    if( column.getName().equals( columnName ) )
                    {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }

    private Path getJsonFilePath(){
    	return Paths.get(desHome+"/desDataSources.json");
    }
	
    @Override
    public synchronized DataSource getDataSource(String id) {
    	if(super.getDataSources().size() == 0){
    		getDataSources();
		 }
    	return super.getDataSource(id);
    }
    
	@Override
	public synchronized DataSource addDataSource(DataSource ds) throws DataExtractionServiceException {
		if(super.getDataSources().size() == 0){
		  getDataSources();
		}
		super.addDataSource(ds);
		return JSONPersistanceUtil.addDataSource(ds,getJsonFilePath());
	}

	@Override
	public synchronized void updateDataSource(String id, DataSource ds) throws DataExtractionServiceException {
		if(super.getDataSources().size() == 0){
			  getDataSources();
			}
		super.updateDataSource(id,ds);
        JSONPersistanceUtil.deleteDataSource(id,true,getJsonFilePath());
		JSONPersistanceUtil.addDataSource(ds,getJsonFilePath());
	}
	@Override
	public synchronized void deleteDataSource(String id) {
		if(super.getDataSources().size() == 0){
			   getDataSources();
			}
		
		
		super.deleteDataSource(id);
		JSONPersistanceUtil.deleteDataSource(id,true,getJsonFilePath());
	}
	 
	@Override
	public synchronized List<DataSource> getDataSources(){
		if(super.getDataSources().size() == 0){
		 return	JSONPersistanceUtil.getDataSources(super.getDataSources(),getJsonFilePath());
		}
       return super.getDataSources();
	} 

	@Override
	public synchronized List<DataExtractionJob> getDataExtractionJobs() {
		 List<DataExtractionJob> dataExtractionJobList = new ArrayList<DataExtractionJob>();
		 List<DataExtractionJob> dataExtractionJobs =  super.getDataExtractionJobs();
		 for(DataExtractionJob dataExtractionJob : dataExtractionJobs){
			 dataExtractionJobList.add(getDataExtractionJob(dataExtractionJob.getId()));
		 }
		 return dataExtractionJobList;
	}
	
	 @Override
		public synchronized DataExtractionJob getDataExtractionJob(String id) {
	    		DataExtractionJob finaldataExtractionJob = super.getDataExtractionJob(id);
	    		Queue<DataExtractionChunk> dataExtractionChunkQueue = workQueueMonitoring.getDataExtractionChunkQueue(finaldataExtractionJob);
	    		if(dataExtractionChunkQueue.size() > 0){
	    			int chunksCount = dataExtractionChunkQueue.size();
	        		int objectsExtracted = 0;
	        		String timeStarted = "";
	        		String timeCompleted = "";
	        		int failureMessageCount = 0;
	        		String lastFailureMessage="";
	        		int count = 1;
	        		List<DataExtractionChunk> dataExtractionChunks = new  ArrayList<DataExtractionChunk>();
	        		for(DataExtractionChunk dataExtractionChunk : dataExtractionChunkQueue){
	        			if(dataExtractionChunk.getJobId().equals(finaldataExtractionJob.getId())){
	        				   if(count == 1)
	        					timeStarted = dataExtractionChunk.getTimeStarted();
	        					if(count == chunksCount)
	        					timeCompleted = dataExtractionChunk.getTimeCompleted();
	        					dataExtractionChunks.add(dataExtractionChunk);
	        				    objectsExtracted += Integer.valueOf(dataExtractionChunk.getObjectsExtracted());
	        				    lastFailureMessage = dataExtractionChunk.getLastFailureMessage();
	        					if(lastFailureMessage != null && !lastFailureMessage.equals("")){
	        						failureMessageCount++;
	        					}
	        					count++;
	        			}
	        		}
	        		finaldataExtractionJob.setObjectsExtracted(objectsExtracted);
	        		if(failureMessageCount > 0 ) {
	        			 finaldataExtractionJob.setState(StateEnum.FAILED);
	        		} else{
	        			if(finaldataExtractionJob.getChunksCount() == chunksCount){
	            			finaldataExtractionJob.setState(StateEnum.SUCCEEDED);
	            		}else{
	            			 finaldataExtractionJob.setState(StateEnum.RUNNING);
	            		}
	        		}
	        		finaldataExtractionJob.setTimeStarted(timeStarted);
	        		finaldataExtractionJob.setTimeCompleted(timeCompleted);
	        		finaldataExtractionJob.setDataExtractionChunks(dataExtractionChunks);
	    		}
	    		return finaldataExtractionJob;
		} 
}
