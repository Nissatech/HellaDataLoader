package com.nissatech.helladataloader;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author aleksandar
 */
public abstract class HellaLoader
{
    protected final String file;
    protected final ITimeRounder rounder;
    protected Cluster cluster;
    protected Session session;

    public HellaLoader(String file, ITimeRounder rounder)
    {
        this.file = file;
        this.rounder = rounder;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
    }

    public abstract void load() throws IOException;

    public void addToDB(String variableType, long roundedTime, long time, Map values)
    {
        PreparedStatement statement = session.prepare("Insert into proasense.hella_variables (type_hour_stamp,variable_type,variable_timestamp, values) " + "VALUES (?, ? , ? ,?)");
        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.bind(variableType + "|" + roundedTime, variableType, new Date(time), values);
        session.execute(boundStatement);
    }

    public void close()
    {
        this.session.close();
        this.cluster.close();
    }
    
}
