const { EventHubConsumerClient, earliestEventPosition, latestEventPosition } = require("@azure/event-hubs");
require('dotenv').config();
const axios = require('axios').default;

const ML_Post = async (data) => {
  const data_ = {
  }
  Object.keys(data).forEach((key) => {
    if (key != 'Class') {
      data_[key] = data[key];
    }
  })
  const response = await axios.post(process.env.ML_MODEL_REST_ENDPOINT, {
    data: [data_]
  })
  console.log(response.data);
  return response;
}

async function main() {
  const client = new EventHubConsumerClient(
    `${process.env.CONSUMER}`,
    `${process.env.EVENTHUB_ENDPOINT}`,
    `${process.env.EVENTHUB_NAME}`
  );

  // In this sample, we use the position of earliest available event to start from
  // Other common options to configure would be `maxBatchSize` and `maxWaitTimeInSeconds`
  const subscriptionOptions = {
    startPosition: latestEventPosition
  };

  const subscription = client.subscribe(
    {
      processEvents: async (events, context) => {
        console.log('Events: ', events);
        events.forEach(event => {
          if (event.body) {
            ML_Post(event.body)
          }
        });
      },
      processError: async (err, context) => {
        console.log('Error: ', err);
      }
    },
    subscriptionOptions
  );

  // Wait for a few seconds to receive events before closing
  // setTimeout(async () => {
  //   await subscription.close();
  //   await client.close();
  //   console.log(`Exiting sample`);
  // }, 3 * 1000);
}

// Test/Demo row of the masked data:
// ML_Post(
//   {
//     Time: '0         ',
//     V1: -1.3598071336738,
//     V2: -0.0727811733098497,
//     V3: 2.53634673796914,
//     V4: 1.37815522427443,
//     V5: -0.338320769942518,
//     V6: 0.462387777762292,
//     V7: 0.239598554061257,
//     V8: 0.0986979012610507,
//     V9: 0.363786969611213,
//     V10: 0.0907941719789316,
//     V11: -0.551599533260813,
//     V12: -0.617800855762348,
//     V13: -0.991389847235408,
//     V14: -0.311169353699879,
//     V15: 1.46817697209427,
//     V16: -0.470400525259478,
//     V17: 0.207971241929242,
//     V18: 0.0257905801985591,
//     V19: 0.403992960255733,
//     V20: 0.251412098239705,
//     V21: -0.018306777944153,
//     V22: 0.277837575558899,
//     V23: -0.110473910188767,
//     V24: 0.0669280749146731,
//     V25: 0.128539358273528,
//     V26: -0.189114843888824,
//     V27: 0.133558376740387,
//     V28: -0.0210530534538215,
//     Amount: 149.62,
//     Class: "'0'       "
//   }
// )
main();