package com.prifender.des.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.encryption.api.Encryption;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingQueue;

public abstract class DataExtractionThread extends Thread
{
	private static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);

	protected final DataExtractionContext context;

	Encryption encryption;

	public DataExtractionThread(final DataExtractionContext context)
	{
		this.context = context;
	}

	public synchronized void cancel()
	{
		if( this.context != null )
		{
			this.cancel();
			System.out.println("Job " + this.context.job.getId() + " terminated");
		}
	}

	@Override
	public final void run()
	{
		final DataExtractionSpec spec = this.context.spec;
		final DataExtractionJob job = this.context.job;
		final String pendingTasksQueueName = this.context.pendingTasksQueueName;
		final String typeId = this.context.typeId;
		MessagingQueue pendingTasksQueue = this.context.pendingTasksQueue;
		encryption = this.context.encryption;

		List<DataExtractionTask> listDataExtractionTask = null;

		final String queueName;

		synchronized (job)
		{
			queueName = job.getOutputMessagingQueue();
		}

		try ( MessagingConnection mc = this.context.messaging.connect())
		{
			mc.queue(queueName);
		}
		catch ( final Exception e )
		{
			synchronized (job)
			{
				job.setState(DataExtractionJob.StateEnum.FAILED);
				job.setFailureMessage(e.getMessage());
			}

			return;
		}

		synchronized (job)
		{
			job.setState(DataExtractionJob.StateEnum.WAITING);
			job.setTimeStarted(DATE_FORMAT.format(new Date()));
		}

		try
		{
			int objectCount = this.context.adapter.getCountRows(this.context.ds, this.context.spec);

			if( !typeId.equals("CSV") && !typeId.equals("CSVNFS") && !typeId.equals("HDFSAvro") )
			{
				if( spec.getScope() == null )
				{
					spec.setScope(DataExtractionSpec.ScopeEnum.ALL);
				}
				else if( spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
				{
					if( spec.getSampleSize() == null )
					{
						throw new IllegalArgumentException("sampleSize value not found");
					}

					objectCount = objectCount < spec.getSampleSize() ? objectCount : spec.getSampleSize();
				}
			}
			synchronized (job)
			{
				job.setObjectCount(objectCount);
				job.setObjectsExtracted(0);
			}

			if( objectCount != 0 )
			{
				listDataExtractionTask = runDataExtractionJob();

				try ( MessagingConnection mc = this.context.messaging.connect())
				{

					pendingTasksQueue = mc.queue(pendingTasksQueueName);

					for (DataExtractionTask dataExtractionTask : listDataExtractionTask)
					{

						dataExtractionTask.setContextParameters(encryptContextParams(dataExtractionTask.getContextParameters()));

						postPendingTask(dataExtractionTask, pendingTasksQueue);

					}
				}
				catch ( Exception e )
				{
					synchronized (job)
					{
						job.setState(DataExtractionJob.StateEnum.FAILED);
						job.setFailureMessage(e.getMessage());
					}
					return;
				}
			}
			synchronized (job)
			{
				job.setTasksCount(listDataExtractionTask.size());
			}
			if( objectCount == 0 )
			{
				synchronized (job)
				{
					job.setObjectsExtracted(objectCount);
					job.setState(DataExtractionJob.StateEnum.SUCCEEDED);
					job.setTimeCompleted(DATE_FORMAT.format(new Date()));
				}
			}

		}
		catch ( final Exception e )
		{
			synchronized (job)
			{
				job.setState(DataExtractionJob.StateEnum.FAILED);
				job.setFailureMessage(e.getMessage());
			}
		}
	}

	protected abstract List<DataExtractionTask> runDataExtractionJob() throws Exception;

	private void postPendingTask(final DataExtractionTask dataExtractionJobTask, final MessagingQueue queue) throws IOException
	{
		Gson gsonObj = new Gson();

		String jsonStr = gsonObj.toJson(dataExtractionJobTask);

		queue.post(jsonStr);

	}

	private Map<String, String> encryptContextParams(final Map<String, String> contextParams) throws Exception
	{
		final Map<String, String> contextParmas = new HashMap<String, String>();

		StringWriter sw = new StringWriter();

		try ( final JsonWriter writer = new JsonWriter(sw))
		{
			writer.setIndent("    ");
			writer.setSerializeNulls(false);
			writer.beginArray();

			contextParams.entrySet().forEach(entry ->
			{
				try
				{
					writer.beginObject();
					writer.name(entry.getKey());
					writer.value(entry.getValue());
					writer.endObject();
				}

				catch ( Exception e )
				{
					throw new RuntimeException(e);
				}

			});

			writer.endArray();
		}

		contextParmas.put("encryptedContextParams", this.encryption.encrypt(sw.toString()));

		return contextParmas;
	}
}
