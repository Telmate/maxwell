package com.zendesk.maxwell.schema.ddl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class ResolvedDatabaseCreate extends ResolvedSchemaChange {
	public String database;
	public String charset;

	static final Logger LOGGER = LoggerFactory.getLogger(ResolvedDatabaseCreate.class);

	public ResolvedDatabaseCreate() { }
	public ResolvedDatabaseCreate(String database, String charset) {
		this.database = database;
		this.charset = charset;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		if ( schema.hasDatabase(database) ) {
			//throw new InvalidSchemaError("Unexpectedly asked to create existing database " + database);
			LOGGER.warn("Unexpectedly asked to create existing database " + this.database + ". Will skip statement");
			return;
		}

		schema.addDatabase(new Database(database, charset));
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return null;
	}
}
