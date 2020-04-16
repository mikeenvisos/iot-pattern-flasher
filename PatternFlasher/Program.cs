using System;
using System.Collections;
using System.Collections.Generic;
using System.Device.Gpio;                           // Uses Microsoft GPIO device to control pins on RPi
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Azure.EventHubs;                    // Uses Microsoft IoT as messaging API
using Newtonsoft.Json;

namespace PatternFlasher
{
    internal class Program
    {

        private readonly static bool consoleOut = true;               // Controls if the program outputs log messagess to console
        private readonly static int consoleLogLevel = 10;             // Controls the level of detail of output log messages

        // Connection information to connect to Azure IoT hub
        private readonly static string s_eventHubsCompatibleEndpoint = "sb://iothub-ns-dubai-iot-3138699-8137b73f5a.servicebus.windows.net/";
        private readonly static string s_eventHubsCompatiblePath = "dubai-iot-hub";
        private readonly static string s_iotHubSasKey = "fzccW2aDUZpHzW35NHZZpWhFyd5W9ZLiOQaEQhV5plE=";
        private readonly static string s_iotHubSasKeyName = "service";
        private static EventHubClient s_eventHubClient;

        class LightShow
        {
            public XmlDocument ShowXML { get; set; }
            public Shows Shows = new Shows();
            public LightShow(XmlDocument ShowXMLData)
            {

                ConsoleOut("Loading inbound XML data", 0);
                XmlNodeList itemNodes = ShowXMLData.SelectNodes("//lightShow/shows/*");
                foreach (XmlNode showNode in itemNodes)
                {
                    Show thisShow = new Show
                    {
                        ShowID = Convert.ToInt32(showNode.Attributes["id"].Value),
                        ShowName = showNode.Attributes["name"].Value,
                        Iterations = Convert.ToInt32(showNode.Attributes["iterations"].Value),
                        Speed = Convert.ToInt32(showNode.Attributes["speed"].Value),
                        IsDynamic = false
                    };

                    if (showNode.Attributes["dynamic"] != null)
                    {
                        thisShow.IsDynamic = true;
                        thisShow.DynamicRangeStart = Convert.ToInt32(showNode.Attributes["rangeStart"].Value);
                        thisShow.DynamicRangeEnd = Convert.ToInt32(showNode.Attributes["rangeEnd"].Value);
                        thisShow.Step = Convert.ToInt32(showNode.Attributes["step"].Value);
                    }
                    else
                    {
                        foreach (XmlNode patternNode in showNode.SelectNodes("pattern"))
                        {
                            byte bytePattern = Convert.ToByte(patternNode.InnerText, 2);
                            thisShow.ShowBytes.Add(bytePattern);
                        }
                    }
                    ConsoleOut(String.Format("Found node {0} called '{1}'", thisShow.ShowID, thisShow.ShowName), 1);
                    Shows.Add(thisShow);
                }
            }
        }

        class Shows : List<Show> { }

        class Show
        {
            public string ShowName { get; set; }
            public int ShowID { get; set; }
            public bool IsDynamic { get; set; }
            public int DynamicRangeStart { get; set; }
            public int DynamicRangeEnd { get; set; }
            public int Speed { get; set; }
            public int Iterations { get; set; }

            public int Step { get; set; }

            public List<byte> ShowBytes { get; set; }

            public Show()
            {
                ShowBytes = new List<byte>();
            }

        }

        static void WriteRegister(byte writeValue)
        {
            //  This method simulates writing data to the RPi 4 pins connected to the shift register
            //  In the actual code this method is replaced with pin actuation which controls the shift register
            
            ConsoleOut(String.Format("Outputing byte to shift register : {0}",
                        Convert.ToString(writeValue, toBase: 2)),
                        11);

            for (int i = 0; i < 8; i++)
            {
                var temp = writeValue & 0x80;
                string willOutput;
                if (temp == 0x80)
                {
                    willOutput = "High";
                }
                else
                {
                    willOutput = "Low";
                }

                ConsoleOut(String.Format("{0} : {1} : {2}", i, writeValue, willOutput),12);
                
                writeValue <<= 0x01;
            }
        }

        static void RunShow(Show show)
        {
            if (show.IsDynamic)
            {
                ConsoleOut(String.Format("Running dynamic show {0} with start {1} and end {2} step {3} at {4}ms",
                    show.ShowName,
                    show.DynamicRangeStart,
                    show.DynamicRangeEnd,
                    show.Step,
                    show.Speed), 1);
                for (int i = 0; i < show.Iterations; i++)
                {
                    int bytePattern = show.DynamicRangeStart;
                    while (bytePattern <= show.DynamicRangeEnd)
                    {
                        WriteRegister(Convert.ToByte(bytePattern));
                        bytePattern += show.Step;
                    }
                }
            }
            else
            {
                ConsoleOut(String.Format("Running static show {0} with {1} patterns at {2}ms", show.ShowName, show.ShowBytes.Count, show.Speed), 1);
                for (int i = 0; i < show.Iterations; i++)
                {
                    foreach (byte bytePattern in show.ShowBytes)
                    {
                        WriteRegister(bytePattern);
                        Thread.Sleep(show.Speed);
                    }
                }
            }

        }

        static void ConsoleOut(string consoleMessage, int messageLoggingLevel)
        {
            if (consoleOut && messageLoggingLevel < consoleLogLevel)
            {
                string padding = string.Concat(Enumerable.Repeat(" ", messageLoggingLevel));
                Console.WriteLine(string.Concat(padding, consoleMessage));
            }
        }

        public static async Task ReceiveMessagesFromDeviceAsync(string partition, CancellationToken ct)
        {
            // Create the receiver using the default consumer group.
            // For the purposes of this sample, read only messages sent since 
            // the time the receiver is created. Typically, you don't want to skip any messages.
            var eventHubReceiver = s_eventHubClient.CreateReceiver("$Default", partition, EventPosition.FromEnqueuedTime(DateTime.Now));
            Console.WriteLine("Create receiver on partition: " + partition);
            while (true)
            {
                if (ct.IsCancellationRequested) break;
                
                Console.WriteLine("Listening for messages on: " + partition);
                var events = await eventHubReceiver.ReceiveAsync(100);

                // If there is data in the batch, process it.
                if (events == null) continue;

                foreach (EventData eventData in events)
                {
                    string data = Encoding.UTF8.GetString(eventData.Body.Array);
                    Console.WriteLine("Message received on partition {0}:", partition);
                    XmlDocument eventProcessResult = ProcessIoTEvent(eventData);
                    if (eventProcessResult != null) {
                        LightShow lightShow = new LightShow(eventProcessResult);
                        foreach (Show show in lightShow.Shows) {
                            RunShow(show);
                        }
                    }
                }
            }
        }

        public static XmlDocument ProcessIoTEvent(EventData eventToProcess) {

            bool IsFlasherMessage = false;
            foreach (var prop in eventToProcess.Properties)
            {
                try { 
                    IsFlasherMessage = (prop.Key == "iot-pattern-flasher" && Convert.ToBoolean(prop.Value));
                }
                catch { }
            }
            if (IsFlasherMessage) {
                ConsoleOut("Message for iot-pattern-flasher detected", 1);
                string eventRawData = Encoding.UTF8.GetString(eventToProcess.Body.Array);
                try
                {
                    XmlDocument inboundXML = JsonConvert.DeserializeXmlNode(eventRawData);
                    ConsoleOut("Message parses as XML", 1);
                    return inboundXML;
                }
                catch
                {
                    ConsoleOut("Error parsing XML", 1);
                    return null;
                }
            }
            else { return null; }
        }

        private static async Task Main(string[] args)
        {

            // This is an example code block to show how the program will collect, parse and display byte patterns
            // This will be removed in favour of asynchronously parsing and displaying byte patterns received from IoT messages 
            // This code instantiates a new light show, populating display data from local pattern XML file

            //ConsoleOut("Starting programme", 0);
            //XmlDocument ShowXML = new XmlDocument();
            //ShowXML.Load(dataFilePath);
            //LightShow lightShow = new LightShow(ShowXML);
            //// Execute the byte patterns loaded from XML
            //ConsoleOut("Running lightshow", 0);
            //foreach (Show show in lightShow.Shows) {
            //    RunShow(show);
            //}
                        
            // For reference, the IoT code comes from here:
            // https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-raspberry-pi-kit-c-get-started


            // Listens for events on IoT Hub
            ConsoleOut(String.Format("Subscribing to IoT hub : '{0}' Press CTRL-C to Stop", s_eventHubsCompatiblePath), 0);
            var connectionString = new EventHubsConnectionStringBuilder(new Uri(s_eventHubsCompatibleEndpoint), s_eventHubsCompatiblePath, s_iotHubSasKeyName, s_iotHubSasKey);
            s_eventHubClient = EventHubClient.CreateFromConnectionString(connectionString.ToString());
            var runtimeInfo = await s_eventHubClient.GetRuntimeInformationAsync();
            var d2cPartitions = runtimeInfo.PartitionIds;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                Console.WriteLine("Exiting... please wait ...");
            };

            var tasks = new List<Task>();
            foreach (string partition in d2cPartitions)
            {
                tasks.Add(ReceiveMessagesFromDeviceAsync(partition, cts.Token));
            }

            Task.WaitAll(tasks.ToArray());
            ConsoleOut("Terminating program", 0);

        }

    }
}
