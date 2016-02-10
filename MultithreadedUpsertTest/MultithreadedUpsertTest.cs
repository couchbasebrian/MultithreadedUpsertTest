using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Couchbase;
using Couchbase.Configuration.Client;


// Target Framework 
// .NET Framework 4.5

namespace MultithreadedUpsertTest
{

    class MyThreadFactory
    {

        static int threadCounter = 0;

        // Create a new MyThread and give it a unique id number
        public static MyThread createANewThread()
        {
            threadCounter++;
            return new MyThread(threadCounter);
        }
    }

    class MyThread
    {
        // Private variables
        private int mySerialNumber = 0;
        private bool keepRunning = true;

        public void stopRunning()
        {
            keepRunning = false;
        }

        // This constructor takes a serial number as an argument 
        // which is provided by the factory
        public MyThread(int myNum)
        {
            mySerialNumber = myNum;
        }

        // The main loop where periodic work is done
        public void doSomething()
        {
            string timeAndDateNow;
            string documentBody;
            int randomSleepTimeMS = 0;

            Random rnd;

            while (keepRunning)
            {
                rnd = new Random();

                timeAndDateNow = DateTime.Now.ToString();
                doLog("Hello there! " + timeAndDateNow);
                documentBody = "{ \"data\" : \"" + timeAndDateNow + " by thread #" + mySerialNumber + "\" }";
                CouchbaseHelper.upsert("documentKey", documentBody);
                randomSleepTimeMS = rnd.Next(1, 10);
                Thread.Sleep(randomSleepTimeMS); // Sleep for x milliseconds
            }
        }

        void doLog(string s)
        {
            Console.WriteLine("[ THREAD " + mySerialNumber + " ] " + s);
        }
    }


    class CouchbaseHelper
    {

        private static string hostName;
        private static string bucketName;
        private static bool initDone = false;
        private static CouchbaseBucket defaultBucket = null;
        private static TimeSpan defaultTimeSpan;
        private static int exceptionCounter = 0;

        public static void init(string hn, string bn)
        {
            defaultTimeSpan = new TimeSpan(0, 0, 0, 30);

            string myurl = "http://" + hn + ":8091";

            Console.WriteLine("About to init Cluster Helper: " + myurl);
            ClusterHelper.Initialize(new ClientConfiguration
            {
                Servers = new List<Uri>
                {
                    new Uri(myurl)
                }
            });

            Console.WriteLine("About to get bucket: " + bn);

            defaultBucket = (CouchbaseBucket) ClusterHelper.GetBucket(bn);

            initDone = true;
        }

        public static bool upsert(string key, string value)
        {
            return upsert(key, value, defaultTimeSpan);
        }

        public static bool upsert(string key, string value, TimeSpan ts)
        {

            var result = defaultBucket.Upsert(key, value, ts);

            bool isSuccess = result.Success;

            if (isSuccess == false)
            {
                Console.WriteLine("Upsert failed!  Message: " + result.Message);
                exceptionCounter++;
            }
        
            return isSuccess;
        }

    }



    class MultithreadedUpsertTestProgram
    {
        static void Main(string[] args)
        {

            CouchbaseHelper.init("10.4.2.121", "BUCKETNAME");

            const int numberOfThreads = 10;

            Thread[] threadArray = new Thread[numberOfThreads];

            Console.WriteLine("Creating " + numberOfThreads + " threads now...");

            for (int i = 0; i < numberOfThreads; i++)
            {
                MyThread myThreadObject = MyThreadFactory.createANewThread();
                threadArray[i] = new Thread(new ThreadStart(myThreadObject.doSomething));
            }

            Console.WriteLine("Starting threads now...");

            for (int i = 0; i < numberOfThreads; i++)
            {
                threadArray[i].Start();
            }

            Console.WriteLine("After starting threads");

            // The program will run until you Control-C
        }
    }
}

// EOF