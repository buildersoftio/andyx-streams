using Andy.X.Client;
using Andy.X.Client.Abstractions;
using Andy.X.Client.Configurations;
using Andy.X.Streams.Abstractions;
using Microsoft.Extensions.Logging;
using System;

namespace Andy.X.Streams.Builders
{
    public class StreamBuilder : IStreamBuilder, IStreamDesigner, IFlowDesigner, ISinkDesigner
    {
        private readonly ILogger<StreamBuilder> _logger;
        private readonly XClient _xClient;
        private readonly string _streamName;

        private Producer<object> producerStream;
        private Consumer<object> consumerStream;

        private Func<object, object> _funcMapper;



        private StreamBuilder(XClient xClient, string streamName)
        {
            _xClient = xClient;
            _streamName = streamName;
            _logger = xClient.GetClientConfiguration().Logging.GetLoggerFactory().CreateLogger<StreamBuilder>();
        }

        public static StreamBuilder CreateNewStreamBuilder(IXClientFactory xClientFactory, string streamName)
        {
            return new StreamBuilder(xClientFactory.CreateClient(), streamName);
        }
        public static StreamBuilder CreateNewStreamBuilder(XClient xClient, string streamName)
        {
            return new StreamBuilder(xClient, streamName);
        }

        public IFlowDesigner Stream<TIn>(string component, string topic)
        {
            consumerStream = new Consumer<object>(_xClient)
                .Name(_streamName)
                .Component(component)
                .Topic(topic)
                .InitialPosition(InitialPosition.Earliest)
                .SubscriptionType(SubscriptionType.Shared)
                .Build();

            consumerStream.MessageReceived += ConsumerStream_MessageReceived;


            return this;
        }

        public ISinkDesigner Map<TIn, TOut>(Func<TIn, TOut> filterFunction)
        {
            _funcMapper = filterFunction as Func<object, object>;
            return this;
        }

        public IStreamBuilder To<TOut>(string component, string topic)
        {
            producerStream = new Producer<object>(_xClient)
                .Name(_streamName)
                .Component(component)
                .Topic(topic)
                .RetryProducing(true)
                .AddDefaultHeader("stream-version", "andyx-streams v2.1")
                .Build();

            return this;
        }

        private bool ConsumerStream_MessageReceived(object sender, Client.Events.Consumers.MessageReceivedArgs<object> e)
        {
            try
            {
                var mapped = _funcMapper(e.GenericPayload);
                producerStream.Produce(mapped);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void Build()
        {
            producerStream.OpenAsync().Wait();
            consumerStream.SubscribeAsync().Wait();
        }
    }
}
