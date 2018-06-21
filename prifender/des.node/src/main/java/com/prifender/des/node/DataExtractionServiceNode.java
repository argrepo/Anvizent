package com.prifender.des.node;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataExtractionTaskResults;
import com.prifender.des.model.DocExtractionResult;
import com.prifender.encryption.api.Encryption;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component
public class DataExtractionServiceNode implements Runnable
{

	private static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);

	@Value("${scheduling.pendingTasksQueue}")
	private String pendingTasksQueueName;

	private MessagingQueue pendingTasksQueue;

	@Value("${scheduling.taskStatusQueue}")
	private String taskStatusQueueName;

	private MessagingQueue taskStatusQueue;

	@Value("${scheduling.retries}")
	private int retries;

	@Autowired
	private Encryption encryption;

	@Autowired
	private MessagingConnectionFactory messagingConnectionFactory;

	@Override
	public void run()
	{
		try ( final MessagingConnection mc = this.messagingConnectionFactory.connect())
		{
			this.pendingTasksQueue = mc.queue(this.pendingTasksQueueName);

			this.taskStatusQueue = mc.queue(this.taskStatusQueueName);

			this.pendingTasksQueue.consume(new MessageConsumer()
			{

				public void consume(final byte[] message)
				{

					String msg = null;

					try
					{

						msg = new String(message, "UTF-8");

						processPendingTask(msg);

					}
					catch ( final UnsupportedEncodingException e )
					{

						e.printStackTrace();

						return;

					}
					catch ( Exception e )
					{
						try
						{

							retryToPendingTasksQueuOrTaskStatusQueue(msg, e.getMessage());

						}
						catch ( Exception e1 )
						{
							e1.printStackTrace();
						}
					}
				}
			});

		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
		finally
		{
			this.pendingTasksQueue = null;
			this.taskStatusQueue = null;
		}
	}

	/**
	 * @param msg
	 * @throws Exception
	 * @throws SQLException
	 */
	private void processPendingTask(String msg) throws Exception, SQLException
	{

		try
		{

			ObjectMapper mapper = new ObjectMapper();

			DataExtractionTask dataExtractionJobTask = mapper.readValue(msg, new TypeReference<DataExtractionTask>()
			{
			});

			if( dataExtractionJobTask != null )
			{
				processDocExtactionJob(dataExtractionJobTask, dataExtractionJobTask.getTypeId());
			}
		}
		catch ( Exception e )
		{
			throw new Exception(e.getMessage());
		}
	}

	private void processDocExtactionJob(DataExtractionTask dataExtractionJobTask, String typeId) throws Exception
	{
		try
		{
			String startTime = DATE_FORMAT.format(new Date());

			postTaskToTaskStatusQueue(

					new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())

							.jobId(dataExtractionJobTask.getJobId())

							.timeStarted(startTime)

							.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())

							.lastFailureMessage(dataExtractionJobTask.getLastFailureMessage())

							.objectsExtracted(0),

					taskStatusQueue

			);

			Map<String, String> contextParams = dataExtractionJobTask.getContextParameters();
			List<DocExtractionResult> resultList = new ArrayList<>();

			if( "GoogleDrive".equals(typeId) )
			{
				GDriveDocExtractionJobExecution docExtractionJobExecution = new GDriveDocExtractionJobExecution();
				resultList = docExtractionJobExecution.fetchAndExtractDocContent(contextParams);
			}
			else if( "Box".equals(typeId) )
			{
				BoxDocExtractionJobExecution docExtractionJobExecution = new BoxDocExtractionJobExecution();
				resultList = docExtractionJobExecution.fetchAndExtractDocContent(contextParams);
			}
			else if( "Dropbox".equals(typeId) )
			{
				DropBoxDocExtractionJobExecution docExtractionJobExecution = new DropBoxDocExtractionJobExecution();
				resultList = docExtractionJobExecution.fetchAndExtractDocContent(contextParams);
			}

			final MessagingConnection mc = this.messagingConnectionFactory.connect();
			MessagingQueue docContentQueue = mc.queue(dataExtractionJobTask.getJobId());

			for (DocExtractionResult docExtractionResult : resultList)
			{
				postResultTodocContentQueue(docExtractionResult, docContentQueue);
			}

			String endTime = DATE_FORMAT.format(new Date());

			postTaskToTaskStatusQueue(

					new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())

							.jobId(dataExtractionJobTask.getJobId())

							.timeStarted(startTime).timeCompleted(endTime)

							.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())

							.lastFailureMessage(dataExtractionJobTask.getLastFailureMessage())

							.objectsExtracted(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("SAMPLESIZE"))),

					taskStatusQueue);
		}
		catch ( Exception e )
		{
			throw new Exception(e.getMessage());
		}

	}

	/**
	 * @param dataExtractionTask
	 * @param queue
	 * @throws IOException
	 */
	private void postResultTodocContentQueue(DocExtractionResult docExtractionResult, final MessagingQueue queue) throws IOException
	{

		Gson gsonObj = new Gson();

		String jsonStr = gsonObj.toJson(docExtractionResult);

		queue.post(jsonStr);
	}

	/**
	 * @param dataExtractionTask
	 * @param queue
	 * @throws IOException
	 */
	private void retryTaskToPendingTaskQueue(final DataExtractionTask dataExtractionTask, final MessagingQueue queue) throws IOException
	{
		Gson gsonObj = new Gson();

		String jsonStr = gsonObj.toJson(dataExtractionTask);

		queue.post(jsonStr);
	}

	/**
	 * @param dataExtractionTaskResults
	 * @param queue
	 * @throws IOException
	 */
	private void postTaskToTaskStatusQueue(final DataExtractionTaskResults dataExtractionTaskResults, final MessagingQueue queue) throws IOException
	{

		Gson gsonObj = new Gson();

		String jsonStr = gsonObj.toJson(dataExtractionTaskResults);

		queue.post(jsonStr);
	}

	/**
	 * @param msg
	 * @param lastFailureMessage
	 * @throws Exception
	 */
	private void retryToPendingTasksQueuOrTaskStatusQueue(String msg, String lastFailureMessage) throws Exception
	{
		try
		{

			ObjectMapper mapper = new ObjectMapper();

			DataExtractionTask dataExtractionJobTask = mapper.readValue(msg, new TypeReference<DataExtractionTask>()
			{
			});

			dataExtractionJobTask.setContextParameters(decryptContextParams(dataExtractionJobTask.getContextParameters()));

			if( dataExtractionJobTask != null )
			{
				int numberOfFailedAttempts = dataExtractionJobTask.getNumberOfFailedAttempts();

				if( numberOfFailedAttempts == retries )
				{
					if( msg != null )
					{

						if( dataExtractionJobTask.getTypeId() != null )
						{
							postTaskToTaskStatusQueue(

									new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())

											.jobId(dataExtractionJobTask.getJobId())

											.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())

											.lastFailureMessage(lastFailureMessage)

											.objectsExtracted(0),

									taskStatusQueue

							);
						}
					}
				}
				else
				{

					dataExtractionJobTask.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts() + 1)

							.lastFailureMessage(lastFailureMessage)

							.setContextParameters(dataExtractionJobTask.getContextParameters());

					retryTaskToPendingTaskQueue(dataExtractionJobTask, pendingTasksQueue);

				}

			}
		}
		catch ( Exception e )
		{
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * @param encryptedContextParams
	 * @return
	 * @throws Exception
	 */
	private Map<String, String> decryptContextParams(final Map<String, String> encryptedContextParams) throws Exception
	{
		final Map<String, String> contextParmas = new HashMap<String, String>();

		final String decrypted = this.encryption.decrypt(encryptedContextParams.get("encryptedContextParams"));

		final JsonArray jsonArray = (JsonArray) new JsonParser().parse(new StringReader(decrypted));

		for (final JsonElement entry : jsonArray)
		{
			final JsonObject jsonObject = (JsonObject) entry;

			for (final Map.Entry<String, JsonElement> param : jsonObject.entrySet())
			{
				contextParmas.put(param.getKey(), param.getValue().getAsString());
			}
		}
		return contextParmas;
	}
}
