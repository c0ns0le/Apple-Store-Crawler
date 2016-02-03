using NLog;
using SharedLibrary;
using SharedLibrary.AWS;
using SharedLibrary.ConfigurationReader;
using SharedLibrary.Log;
using SharedLibrary.Models;
using SharedLibrary.Parsing;
using SharedLibrary.Proxies;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppStoreAppUrlsWorker
{
    class UrlsWorker
    {
        // Logging Tool
        private static Logger _logger;

        // Configuration Values
        private static string _appUrlsQueueName;
        private static string _appsDataQueueName;
        private static string _awsKey;
        private static string _awsKeySecret;
        private static int    _maxRetries;
        private static int    _maxMessagesPerDequeue;

        // Control Variables
        private static int    _hiccupTime = 1000;

        static void Main (string[] args)
        {

            // Creating Needed Instances
            RequestsHandler httpClient = new RequestsHandler ();
            AppStoreParser  parser     = new AppStoreParser ();

            // Loading Configuration
            LogSetup.InitializeLog ("Apple_Store_Urls_Worker.log", "info");
            _logger = LogManager.GetCurrentClassLogger ();

            // Loading Config
            _logger.Info ("Loading Configurations from App.config");
            LoadConfiguration ();

            // Control Variable (Bool - Should the process use proxies? )
            bool shouldUseProxies = false;

            // Checking for the need to use proxies
            if (args != null && args.Length == 1)
            {
                // Setting flag to true
                shouldUseProxies = true;

                // Loading proxies from .txt received as argument
                String fPath = args[0];

                // Sanity Check
                if (!File.Exists (fPath))
                {
                    _logger.Fatal ("Couldnt find proxies on path : " + fPath);
                    System.Environment.Exit (-100);
                }

                // Reading Proxies from File
                string[] fLines = File.ReadAllLines (fPath, Encoding.GetEncoding ("UTF-8"));

                try
                {
                    // Actual Load of Proxies
                    ProxiesLoader.Load (fLines.ToList ());
                }
                catch (Exception ex)
                {
                    _logger.Fatal (ex);
                    System.Environment.Exit (-101);
                }
            }

            // AWS Queue Handler
            _logger.Info ("Initializing Queues");
            AWSSQSHelper appsUrlQueue  = new AWSSQSHelper (_appUrlsQueueName , _maxMessagesPerDequeue, _awsKey, _awsKeySecret);
            AWSSQSHelper appsDataQueue = new AWSSQSHelper (_appsDataQueueName, _maxMessagesPerDequeue, _awsKey, _awsKeySecret);
            
            // Setting Error Flag to No Error ( 0 )
            System.Environment.ExitCode = 0;

            // Initialiazing Control Variables
            int fallbackWaitTime = 1;

            _logger.Info ("Started Processing Individual Apps Urls");

            do
            {
                try
                {
                    // Dequeueing messages from the Queue
                    if (!appsUrlQueue.DeQueueMessages ())
                    {
                        Thread.Sleep (_hiccupTime); // Hiccup                   
                        continue;
                    }

                    // Checking for no message received, and false positives situations
                    if (!appsUrlQueue.AnyMessageReceived ())
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
                    foreach (var appUrl in appsUrlQueue.GetDequeuedMessages ())
                    {
                        if (appUrl.Body.IndexOf ("https://itunes.apple.com/us/app") < 0 && appUrl.Body.IndexOf ("https://itunes.apple.com/app/") < 0)
                        {
                            _logger.Info ("Invalid Message. Deleting [{0}]", appUrl.Body);
                            appsUrlQueue.DeleteMessage (appUrl);
                            continue;
                        }

                        bool processingWorked = true;

                        try
                        {
                            // Retries Counter
                            int retries = 0;
                            string htmlResponse;

                            // Retrying if necessary
                            do
                            {
                                // Executing Http Request for the Category Url
                                htmlResponse = httpClient.Get (appUrl.Body, shouldUseProxies);

                                if (String.IsNullOrEmpty (htmlResponse))
                                {
                                    // Extending Fallback time
                                    retries++;
                                    int sleepTime = retries * _hiccupTime <= 30000 ? retries * _hiccupTime : 30000;

                                    _logger.Info ("Retrying Request for App Page [ " + sleepTime / 1000 + " ]");
                                    
                                    Thread.Sleep (sleepTime);
                                }

                            } while (String.IsNullOrWhiteSpace (htmlResponse) && retries <= _maxRetries);

                            // Checking if retries failed
                            if (String.IsNullOrWhiteSpace (htmlResponse))
                            {
                                continue;
                            }

                            // Feedback
                            _logger.Info ("Current page " + appUrl.Body, "Parsing App Data");

                            // Parsing Data out of the Html Page
                            AppleStoreAppModel parsedApp = parser.ParseAppPage (htmlResponse);
                            parsedApp.url                = appUrl.Body;

                            // Enqueueing App Data
                            appsDataQueue.EnqueueMessage (parsedApp.ToJson ());

                            // Little Hiccup
                            Thread.Sleep (_hiccupTime);

                        }
                        catch (Exception ex)
                        {
                            _logger.Error (ex);

                            // Setting Flag to "False"
                            processingWorked = false;
                        }
                        finally
                        {
                             //Deleting the message - Only if the processing worked
                            if (processingWorked)
                            {
                                appsUrlQueue.DeleteMessage (appUrl);
                            }
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
            _maxRetries             = ConfigurationReader.LoadConfigurationSetting<int>    ("MaxRetries"           , 0);
            _maxMessagesPerDequeue  = ConfigurationReader.LoadConfigurationSetting<int>    ("MaxMessagesPerDequeue", 10);
            _appUrlsQueueName       = ConfigurationReader.LoadConfigurationSetting<String> ("AWSAppUrlsQueue"      , String.Empty);
            _appsDataQueueName      = ConfigurationReader.LoadConfigurationSetting<String> ("AWSAppsDataQueue"  , String.Empty);
            _awsKey                 = ConfigurationReader.LoadConfigurationSetting<String> ("AWSKey"               , String.Empty);
            _awsKeySecret           = ConfigurationReader.LoadConfigurationSetting<String> ("AWSKeySecret"         , String.Empty);
        }
    }
}
