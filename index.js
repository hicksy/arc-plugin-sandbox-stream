// let { join } = require('path')
let _arcFunctions = require('@architect/functions')
let AWS = require('aws-sdk')
let defaultPollingInterval = 10000;
let retryCountRemaining = 3;
let successCount = 0;
let shardIds = [];

module.exports = {
  // Sandbox
  sandbox: {
    // Startup operations
    start: async ({ arc, inventory, invoke }) => {
      
      // Run operations upon Sandbox startup
      const pluginProperties = arc['sandbox-table-streams'];

      if(pluginProperties.length === 0) {
        console.log(`@hicksy/arc-plugin-sandbox-table-streams: Default polling interval is set to ${defaultPollingInterval}. To change, add a polling_interval property to the @sandbox-table-streams pragma in your arc manifest. eg. \n@sandbox-table-streams \npolling_interval 5000`)
      }

      for(prop of pluginProperties) {
        if(prop[0] === 'polling_interval') {
          defaultPollingInterval = prop[1];
          console.log(`@hicksy/arc-plugin-sandbox-table-streams: Polling interval read from arc manifest. Now set to poll every ${defaultPollingInterval}.`)
        }
      }

      // if(inventory.inv.aws.region !== 'ddblocal') {
      //   console.error('@hicksy/arc-plugin-sandbox-table-streams: AWS region not set to ddblocal. Plugin @hicksy/arc-plugin-sandbox-table-streams is only compatible with ddblocal. DynamoDBLocal streams will not invoke your table stream functions.')
      //   return;
      // }

      const client = await _arcFunctions.tables();
      const dynamodb = new AWS.DynamoDB({region: inventory.inv.aws.region, endpoint: `http://localhost:${process.env.ARC_TABLES_PORT}`});  
      const dynamodb_streams = new AWS.DynamoDBStreams({region: inventory.inv.aws.region, endpoint: `http://localhost:${process.env.ARC_TABLES_PORT}`});

      const waitFor = delay => new Promise(resolve => setTimeout(resolve, delay));

      const readStreamShardData = async (shardIterator, tblName) => {
        let data = await dynamodb_streams.getRecords({ShardIterator: shardIterator}).promise();
        
        if(data.Records.length) {
          invoke({
            pragma: 'tables-streams',
            name: tblName,
            payload: data.Records,
    
          });
        }
       
        await waitFor(defaultPollingInterval);

        if(data.NextShardIterator) {
          
          successCount++;
          
          if(successCount === 2) {
            // only show polling success message after two successful returns of NextShardIterator - seems to be a delay in the stream being ready
            console.log(`@hicksy/arc-plugin-sandbox-table-streams: Stream found for table ${tableStream.table}. Polling to invoke stream function.`)
            retryCountRemaining = 3;
          }

          readStreamShardData(data.NextShardIterator, tblName);

        } else {
          console.log(`@hicksy/arc-plugin-sandbox-table-streams: Table ${tableStream.table} stream missing NextShardIterator. Will retry ${retryCountRemaining} more times.`)
          retryCountRemaining--;
          await waitFor(3000);
        }

        initLocalStreams(false);
        
      }
      
      const initLocalStreams = async (showInitLog = true) => {

        if(!showInitLog) {
          await waitFor(defaultPollingInterval);
        }

        if(inventory.inv['tables-streams']) {
        
          for(tableStream of inventory.inv['tables-streams']) {
            let generatedDynamoTableName = client.name(tableStream.table);
            let shardSuccess = [];
            try {
              
              if(showInitLog) console.log(`@hicksy/arc-plugin-sandbox-table-streams: Attempting to connect to stream for table ${tableStream.table}`);
              
              let tableMetaData = await dynamodb.describeTable({TableName: generatedDynamoTableName}).promise();
              let streamMetaData = await dynamodb_streams.describeStream({StreamArn: tableMetaData.Table.LatestStreamArn}).promise();
    
              for(shard of streamMetaData.StreamDescription.Shards) {
                
                let shardIteratorData = await dynamodb_streams.getShardIterator({StreamArn: tableMetaData.Table.LatestStreamArn, ShardIteratorType: 'LATEST',ShardId: shard.ShardId}).promise();
                
                if(shardIteratorData.ShardIterator) {
                  
                  if(!shardIds.includes(shard.ShardId)) {
                    shardIds.push(shard.ShardId);
                    readStreamShardData(shardIteratorData.ShardIterator, tableStream.table);
                  } else {
                    //console.log('shard already polling')
                  }

                  shardSuccess.push(true)
                } else {
                  shardSuccess.push(false)
                }
              
              }

              if(shardSuccess.every(s => s !== true)) {
                throw new Error('Sahrds awaiting init')
              } 
            } catch(e) {

              if(e.code === 'ResourceNotFoundException') {
                console.log(`@hicksy/arc-plugin-sandbox-table-streams: Table ${tableStream.table} does not exist. Using DynamoDB Local requires you to create the dynamodb table yourself (including 'StreamSpecification' config).`)
              }
              
              if(e.message === 'Shards awaiting init' || (e.code === 'MissingRequiredParameter' && e.message === "Missing required key 'ShardIterator' in params")) {
                
                if(retryCountRemaining > 0) {
                  
                  console.log(`@hicksy/arc-plugin-sandbox-table-streams: Table ${tableStream.table} does not have a stream enabled, or table has not finished creating / seeding. Will retry ${retryCountRemaining} more times.`)
                  retryCountRemaining--;
                  
                  await waitFor(3000);
                  initLocalStreams();
                } else {
                  console.log(`@hicksy/arc-plugin-sandbox-table-streams: Table ${tableStream.table} does not have a stream enabled.`)
                }
                
              }

              console.log(e)
            }

            
          }
        } else {
          console.error('@hicksy/arc-plugin-sandbox-table-streams: No @tables-streams pragma found in arc file. Plugin @hicksy/arc-plugin-sandbox-table-streams requires at least one @tables-streams pragma.')
        }

      }

      initLocalStreams();
     
    },

  }
}
