package com.opower.persistence.jpile;

import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class that collects all sql statements and executes each line.
 *
 * @author amir.raminfar
 */
public class ScriptRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptRunner.class);
    private static final String DEFAULT_DELIMITER = ";";

    private Connection connection;
    private String delimiter = DEFAULT_DELIMITER;


    public ScriptRunner(Connection connection) {
        this.connection = connection;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void runScript(Reader reader) throws IOException, SQLException {
        StringBuilder builder = new StringBuilder();
        Statement statement = this.connection.createStatement();
        for (String line : CharStreams.readLines(reader)) {
            builder.append(" ").append(line);
            if (builder.toString().endsWith(this.delimiter)) {
                LOGGER.info(builder.toString());
                statement.execute(builder.toString());
                builder.setLength(0);
            }
        }
        statement.close();
    }
}