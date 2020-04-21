using System;
using System.Collections;
using System.Collections.Generic;
using System.Device.Gpio;                           // Uses Microsoft GPIO device to control pins on RPi
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
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

        public static GpioController gpioController;

        // const string sourceMethod = "FromIOT";  // source lightshow data from IOT
        const string sourceMethod = "FromWebController"; // source lightshow data from static web site

        const string dataPinSpec = "data     = GPIO14 = PIN08 = Register DS / SER";
        const string outputPinSpec = "out    = GPIO15 = PIN10 = Register OE";
        const string latchPinSpec = "stclock = GPIO18 = PIN12 = Register ST_CP";
        const string clockPinSpec = "shclock = GPIO23 = PIN16 = Register SH_CP";
        const string clearPinSpec = "clear   = GPIO24 = PIN18 = Register MR";

        const int dataPin = 14;
        const int outputPin = 15;
        const int latchPin = 18;
        const int clockPin = 23;
        const int clearPin = 24;

        const int pinSetSleepTimeMs = 1;

        private readonly static bool consoleOut = true;               // Controls if the program outputs log messagess to console
        private readonly static int consoleLogLevel = 4;             // Controls the level of detail of output log messages

        // Connection information to connect to Azure IoT hub
        private readonly static string s_eventHubsCompatibleEndpoint = "sb://iothub-ns-dubai-iot-3138699-8137b73f5a.servicebus.windows.net/";
        private readonly static string s_eventHubsCompatiblePath = "dubai-iot-hub";
        private readonly static string s_iotHubSasKey = "fzccW2aDUZpHzW35NHZZpWhFyd5W9ZLiOQaEQhV5plE=";
        private readonly static string s_iotHubSasKeyName = "service";
        private static EventHubClient s_eventHubClient;

        // Connection informaiton to connect to Web Controller site
        private readonly static string webControllerURL = "https://ve8tn.sse.codesandbox.io/";
        private readonly static string webControllerAPI = "api/lightshows";
        private readonly static int controllerReadInterval = 1;  // seconds between reads

        static void ClearRegister()
        {
            ConsoleOut("Clearing register", 3);
            gpioController.Write(clearPin, PinValue.Low);
            Thread.Sleep(pinSetSleepTimeMs * 100);
            gpioController.Write(clearPin, PinValue.High);
        }

        static void PulsePin(int targetPin, int targetValue)
        {
            PinValue pinTargetValue = PinValue.Low;
            PinValue pinRestValue = PinValue.High;
            if (targetValue == 1) { pinTargetValue = PinValue.High; pinRestValue = PinValue.Low; };
            gpioController.Write(targetPin, pinTargetValue);
            Thread.Sleep(pinSetSleepTimeMs);
            gpioController.Write(targetPin, pinRestValue);
            Thread.Sleep(pinSetSleepTimeMs);
        }

        static void WriteRegister(byte writeValue)
        {
            for (int i = 0; i < 8; i++)
            {
                var temp = writeValue & 0x80;
                if (temp == 0x80)
                {
                    gpioController.Write(dataPin, PinValue.High);
                }
                else
                {
                    gpioController.Write(dataPin, PinValue.Low);
                }
                PulsePin(clockPin, 1);
                writeValue <<= 0x01;
            }
            PulsePin(latchPin, 1);

        }

        static void SetupGPIO()
        {
            ConsoleOut("Setting up GPIO environment ...", 0);
            ConsoleOut("Pin specs are ...", 4);
            ConsoleOut(dataPinSpec, 4);
            ConsoleOut(outputPinSpec, 4);
            ConsoleOut(latchPinSpec, 4);
            ConsoleOut(clockPinSpec, 4);
            ConsoleOut(clearPinSpec, 4);

            // Setup pins for output
            gpioController.OpenPin(dataPin, PinMode.Output);
            gpioController.OpenPin(outputPin, PinMode.Output);
            gpioController.OpenPin(latchPin, PinMode.Output);
            gpioController.OpenPin(clockPin, PinMode.Output);
            gpioController.OpenPin(clearPin, PinMode.Output);

            // Setup default pin values
            gpioController.Write(dataPin, PinValue.Low);
            gpioController.Write(latchPin, PinValue.Low);
            gpioController.Write(clockPin, PinValue.Low);
            // Leave output turned on at setup
            gpioController.Write(clockPin, PinValue.Low);

            // Clear register contents
            ClearRegister();
            ConsoleOut("Setup of GPIO environment complete", 0);
        }

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
                        foreach (XmlNode byteNode in showNode.SelectNodes("byte"))
                        {
                            byte bytePattern = (byte)Convert.ToInt32(byteNode.InnerText);
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

        static void WriteRegisterFake(byte writeValue)
        {
            //  This method simulates writing data to the RPi 4 pins connected to the shift register
            
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

            var eventHubReceiver = s_eventHubClient.CreateReceiver("$Default", partition, EventPosition.FromEnqueuedTime(DateTime.Now));
            Console.WriteLine("Create receiver on partition: " + partition);
            while (true)
            {
                if (ct.IsCancellationRequested) break;
                
                Console.WriteLine("Listening for messages on: " + partition);
                var events = await eventHubReceiver.ReceiveAsync(100);

                // If there is data in the batch, process it.
                if (events == null) {
                    ClearRegister();
                    continue;
                }
                

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

        static void ListenToEventStream(string eventURI)
        {
            if (eventURI == null || eventURI.Length == 0)
            {
                throw new ApplicationException("Specify the URI of the resource to retrieve.");
            }
            HttpWebRequest request = null;
            while (true)
            {
                try
                {
                    request = WebRequest.CreateHttp(eventURI);
                    WebResponse response = request.GetResponse();
                    Stream stream = response.GetResponseStream();
                    StreamReader reader = new StreamReader(stream);
                    string line = null;
                    while (null != (line = reader.ReadLine()))
                    {
                        ConsoleOut("Received data", 1);
                        if (line.Equals(string.Empty))
                        {
                            ConsoleOut("Empty data", 10);
                        }
                        else
                        {
                            XmlDocument controllerXML = new XmlDocument();
                            try
                            {
                                controllerXML.LoadXml(line);
                                ConsoleOut(controllerXML.OuterXml, 8);
                            }
                            catch (Exception ex)
                            {
                                ConsoleOut("Error loading XML", 0);
                                ConsoleOut(ex.GetType().ToString(), 0);
                                ConsoleOut(ex.ToString(), 10);
                                controllerXML = null;
                            }
                            LightShow lightShow = null;
                            try
                            {
                                lightShow = new LightShow(controllerXML);
                            }
                            catch
                            {
                                ConsoleOut("Error parsing XML", 0);
                                controllerXML = null;
                            }
                            if (lightShow != null)
                            {
                                gpioController = new GpioController();
                                ConsoleOut("Setup GPIO", 0);
                                SetupGPIO();
                                foreach (Show show in lightShow.Shows)
                                {
                                    RunShow(show);
                                }
                                ConsoleOut("Clean up GPIO", 0);
                                gpioController.Dispose();
                            }
                        }
                    }
                    if (null == line)
                    {
                        Console.WriteLine("Response stream ended.");
                    }
                    else
                    {
                        Console.WriteLine("Listener stop request received.");
                        break;
                    }
                }
                catch (Exception ex) {
                    ConsoleOut("Listener Exception : " + ex.GetType().ToString(), 0);
                    ConsoleOut(ex.ToString(), 10);
                }
                finally
                {
                    if (null != request)
                    {
                        request.Abort();
                    }
                }
            }
            Console.WriteLine("Listener stopped.");
            Console.ReadLine();
        }

        private static async Task Main(string[] args)
        {
            ConsoleOut("Starting program", 0);

            if (sourceMethod == "FromIOT")
            {
                gpioController = new GpioController();
                ConsoleOut("Setup GPIO", 0);
                SetupGPIO();

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

                ConsoleOut("Clean up GPIO", 0);
                gpioController.Dispose();
            }
            else {
               
                ListenToEventStream(webControllerURL+"events");

            }
            
            ConsoleOut("Terminating program", 0);

        }

    }
}
