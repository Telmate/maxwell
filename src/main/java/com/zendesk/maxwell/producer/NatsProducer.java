package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.TopicInterpolator;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NatsProducer extends AbstractProducer {

	private final Logger LOGGER = LoggerFactory.getLogger(NatsProducer.class);
	private final Connection natsConnection;
	private final String natsSubjectTemplate;
	private final JetStream jsConnection;
	private long pubCount;
	private Position checkPoint;


	public NatsProducer(MaxwellContext context) {
		super(context);
		List<String> urls = Arrays.asList(context.getConfig().natsUrl.split(","));
		Options.Builder optionBuilder = new Options.Builder();
		urls.forEach(optionBuilder::server);

		if (context.getConfig().natsUser != null && context.getConfig().natsPassword != null) {
			optionBuilder.userInfo(context.getConfig().natsUser, context.getConfig().natsPassword);
		}
		Options option = optionBuilder.build();

		this.natsSubjectTemplate = context.getConfig().natsSubject;
		this.pubCount = 0;
		this.checkPoint = null;

		try {
			this.natsConnection = Nats.connect(option);
			// in JetStream mode, get jetStream connection for publishing
			if (context.getConfig().natsJetstream) {
				this.jsConnection = this.natsConnection.jetStream();
			} else {
				this.jsConnection = null;
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if (!r.shouldOutput(outputConfig)) {
			context.setPosition(r.getNextPosition());
			return;
		}

		String value = r.toJSON(outputConfig);
		String natsSubject = new TopicInterpolator(this.natsSubjectTemplate).generateFromRowMapAndCleanUpIllegalCharacters(r);

		long maxPayloadSize = natsConnection.getMaxPayload();
		byte[] messageBytes = value.getBytes(StandardCharsets.UTF_8);

		if (messageBytes.length > maxPayloadSize) {
			LOGGER.error("->  nats message size (" + messageBytes.length + ") > max payload size (" + maxPayloadSize + ")");
			return;
		}
		if (jsConnection != null) {
			CompletableFuture<PublishAck> futPubAck = jsConnection.publishAsync(natsSubject, messageBytes);
			// check every 100 messages, especially the first
			if ((pubCount % 100) == 0) {				
				try {
					PublishAck pubAck = futPubAck.get(5, TimeUnit.SECONDS);
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("->  publish stream:{}, sequence:{}", pubAck.getStream(), pubAck.getSeqno());
					}	
					checkPoint = context.getPosition();
				} catch (TimeoutException | ExecutionException | InterruptedException e) {
						LOGGER.error("Error: {} on publish: {} to jetstream subject: {}", e, pubCount, natsSubject);
						// attempt rewind to last checkPoint
						if (checkPoint != null) {
							LOGGER.warn("Rewinding to checkpoint: {}", checkPoint);
							context.setPosition(checkPoint);
						}
						throw e;
				}
			}
		} else {
			natsConnection.publish(natsSubject, messageBytes);
		}
		pubCount++;
		if (r.isTXCommit()) {
			context.setPosition(r.getNextPosition());
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  nats subject:{}, message:{}", natsSubject, value);
		}
	}
}
