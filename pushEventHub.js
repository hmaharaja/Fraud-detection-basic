const { EventHubProducerClient } = require("@azure/event-hubs");
const sql = require('mssql');
require('dotenv').config();

const config = {
  user: process.env.SQL_USERNAME,
  password: process.env.SQL_PASSWORD,
  server: process.env.SQL_SERVER, // You can use 'localhost\\instance' to connect to named instance
  database: process.env.SQL_DB,
};

// retrieve from sql
async function get() {
  try {
    console.log('yikes', config.server)
    // make sure that any items are correctly URL encoded in the connection string
    await sql.connect(config);
    const result = await sql.query`SELECT TOP (1) * FROM [dbo].[dataset2] WHERE Class = '''1'''`;
    console.log(result)
    // console.dir(result);
    return result.recordset;
  } catch (err) {
    console.log(err);
  }
}

// ob
async function publish(data) {
  const producerClient = new EventHubProducerClient(`${process.env.EVENTHUB_ENDPOINT}`, `${process.env.EVENTHUB_NAME}`);

  const eventDataBatch = await producerClient.createBatch();

  try {
    data.forEach(record => {
      let wasAdded = eventDataBatch.tryAdd({ body: record });
      if (!wasAdded) {
        throw record
      }
    });
  } catch (record) {
    console.log(record, ' was not added');
  }

  await producerClient.sendBatch(eventDataBatch);
  await producerClient.close();
}

async function main() {
  const data = await get();
  publish(data);
}