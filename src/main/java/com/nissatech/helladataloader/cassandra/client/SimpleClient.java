package com.nissatech.helladataloader.cassandra.client;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 *
 * @author aleksandar
 */
public class SimpleClient
{

    private Cluster cluster;
    private Session session;

    public SimpleClient()
    {
    }

    public void connect(String address)
    {
        cluster = Cluster.builder().addContactPoint(address).build();
        session = cluster.connect();
    }

    public void queryData()
    {
        PreparedStatement statement = this.session.prepare(
                "select blobAsBigint(timestampAsBlob(variable_timestamp)) as milli, variable_timestamp, value, variable_type from proasense.aker_variables where type_hour_stamp in ('1002116|Tue Nov 19 13:00:00 CET 2013', '1002115|Tue Nov 19 13:00:00 CET 2013')order by variable_timestamp;");
        BoundStatement boundStatement = new BoundStatement(statement);
        ResultSet results = session.execute(boundStatement);
        
        for(Row row : results)
        {
            System.out.println(row.getLong("milli")+" "+row.getDate("variable_timestamp") + " " + row.getString("variable_type"));
        }
        
        
    }

    public void close()
    {
        if(session != null)
        {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

}
