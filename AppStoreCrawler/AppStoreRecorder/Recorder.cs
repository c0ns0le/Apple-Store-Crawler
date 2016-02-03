using Amazon.SQS.Model;
using NLog;
using SharedLibrary;
using SharedLibrary.AWS;
using SharedLibrary.ConfigurationReader;
using SharedLibrary.Log;
using SharedLibrary.Models;
using SharedLibrary.MongoDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppStoreRecorder
{
    class Recorder
    {
        // Logging Tool
        private static Logger _logger;

        // Configuration Values
        private static string _appsDataQueueName;
        private static string _awsKey;
        private static string _awsKeySecret;
        private static int    _maxRetries;
        private static int    _maxMessagesPerDequeue;

        // Control Variables
        private static int    _hiccupTime = 1000;

        static void Main (string[] args)
        {
            // Loading Configuration
            LogSetup.InitializeLog ("Apple_Store_Recorder.log", "info");
            _logger = LogManager.GetCurrentClassLogger ();

            // Loading Config
            _logger.Info ("Loading Configurations from App.config");
            LoadConfiguration ();

            // Initializing Queue
            _logger.Info ("Initializing Queue");
            AWSSQSHelper appsDataQueue = new AWSSQSHelper (_appsDataQueueName, _maxMessagesPerDequeue, _awsKey, _awsKeySecret);
            AWSSQSHelper backup        = new AWSSQSHelper ("DeadLetter", _maxMessagesPerDequeue, _awsKey, _awsKeySecret);

            // Creating MongoDB Instance
            _logger.Info ("Loading MongoDB / Creating Instances");

            MongoDBWrapper mongoDB = new MongoDBWrapper ();
            string serverAddr      = String.Join (":", Consts.MONGO_SERVER, Consts.MONGO_PORT);
            mongoDB.ConfigureDatabase (Consts.MONGO_USER, Consts.MONGO_PASS, Consts.MONGO_AUTH_DB, serverAddr, 10000, Consts.MONGO_DATABASE, Consts.MONGO_COLLECTION);

            // Setting Error Flag to No Error ( 0 )
            System.Environment.ExitCode = 0;

            // Initialiazing Control Variables
            int fallbackWaitTime = 1;

            // Buffer of Messages to be recorder
            List<AppleStoreAppModel> recordsBuffer  = new List<AppleStoreAppModel> ();
            List<Message>            messagesBuffer = new List<Message> ();

            // Insert Batch Size
            int batchSize = 1000;

            _logger.Info ("Started Recording App Data");

            do
            {
                try
                {
                    // Dequeueing messages from the Queue
                    if (!appsDataQueue.DeQueueMessages ())
                    {
                        Thread.Sleep (_hiccupTime); // Hiccup                   
                        continue;
                    }

                    // Checking for no message received, and false positives situations
                    if (!appsDataQueue.AnyMessageReceived ())
                    {
                        // If no message was found, increases the wait time
                        int waitTime;
                        if (fallbackWaitTime <= 12)
                        {
                            // Exponential increase on the wait time, truncated after 12 retries
                            waitTime = Convert.ToInt32 (Math.Pow (2, fallbackWaitTime) * 1000);
                        }
                        else // Reseting Wait after 12 fallbacks
                        {
                            waitTime         = 2000;
                            fallbackWaitTime = 0;
                        }

                        fallbackWaitTime++;

                        // Sleeping before next try
                        Console.WriteLine ("Fallback (seconds) => " + waitTime);
                        Thread.Sleep (waitTime);
                        continue;
                    }

                    // Reseting fallback time
                    fallbackWaitTime = 1;

                    // Iterating over dequeued Messages
                    foreach (var appDataMessage in appsDataQueue.GetDequeuedMessages ())
                    {
                        try
                        {
                            // Deserializing message
                            var appData = AppleStoreAppModel.FromJson (appDataMessage.Body);

                            // Dumping "Url" to "_id"
                            appData._id = appData.url;
                                                        
                            // Adding it to the buffer of records to be recorded
                            recordsBuffer.Add (appData);

                            // Adding message to the buffer of messages to be deleted
                            messagesBuffer.Add (appDataMessage);

                            // Is it time to batch insert ?
                            if ((recordsBuffer.Count % batchSize) == 0)
                            {
                                // Batch Insertion
                                mongoDB.BatchInsert<AppleStoreAppModel> (recordsBuffer);

                                // Logging Feedback
                                _logger.Info ("\tApps Recorded : " + recordsBuffer.Count);

                                // Deleting Messages
                                messagesBuffer.ForEach ( (msg) => appsDataQueue.DeleteMessage (msg));

                                _logger.Info ("\tMessages Deleted: " + messagesBuffer.Count);

                                // Clearing Buffers
                                recordsBuffer.Clear ();
                                messagesBuffer.Clear ();
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.Error (ex);
                        }
                        finally
                        {
                            // Deleting the message
                            appsDataQueue.DeleteMessage (appDataMessage);
                            backup.EnqueueMessage (appDataMessage.Body);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error (ex);
                }

            } while (true);
        }

        private static void LoadConfiguration ()
        {
            // SQS Settings
            _maxRetries             = ConfigurationReader.LoadConfigurationSetting<int>    ("MaxRetries"           , 0);
            _maxMessagesPerDequeue  = ConfigurationReader.LoadConfigurationSetting<int>    ("MaxMessagesPerDequeue", 10);
            _appsDataQueueName      = ConfigurationReader.LoadConfigurationSetting<String> ("AWSAppsDataQueue"     , String.Empty);
            _awsKey                 = ConfigurationReader.LoadConfigurationSetting<String> ("AWSKey"               , String.Empty);
            _awsKeySecret           = ConfigurationReader.LoadConfigurationSetting<String> ("AWSKeySecret"         , String.Empty);
        }
    }
}
