package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedTableCreate extends ResolvedSchemaChange {
	public String database;
	public String table;
	public Table def;

	static final Logger LOGGER = LoggerFactory.getLogger(ResolvedTableCreate.class);

	public ResolvedTableCreate() {}
	public ResolvedTableCreate(Table t) {
		this.database = t.database;
		this.table = t.name;
		this.def = t;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(this.database);

		if ( d.hasTable(this.table) ) {
			//throw new InvalidSchemaError("Unexpectedly asked to create existing table " + this.table);
			LOGGER.warn("Unexpectedly asked to create existing table " + this.database + "." + this.table + ". Will skip statement");
			return;
		}

		d.addTable(this.def);
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
