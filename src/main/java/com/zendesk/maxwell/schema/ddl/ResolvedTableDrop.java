package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedTableDrop extends ResolvedSchemaChange {
	public String database;
	public String table;

	static final Logger LOGGER = LoggerFactory.getLogger(ResolvedTableDrop.class);

	public ResolvedTableDrop() { }
	public ResolvedTableDrop(String database, String table) {
		this.database = database;
		this.table = table;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		try {
			Database d = schema.findDatabaseOrThrow(this.database);
			d.findTableOrThrow(this.table);

			d.removeTable(this.table);
		} catch(InvalidSchemaError ise)  {
			LOGGER.warn("Table drop problem " + ise.toString() + ". Will skip statement");
		}
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return table;
	}
}
