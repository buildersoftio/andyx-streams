using Andy.X.Client;
using Andy.X.Client.Abstractions;
using Andy.X.Client.Abstractions.Consumers;
using Andy.X.Client.Abstractions.XClients;
using Andy.X.Client.Models;
using Andy.X.Streams.Abstractions;
using Andy.X.Streams.Models.Internal;
using Andy.X.Streams.Settings;
using MessagePack;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace Andy.X.Streams.Builders
{
    public class StreamBuilder<TIn, TOut> : IStreamBuilder<TIn, TOut>, IStreamDesigner<TIn, TOut>, IFlowDesigner<TIn, TOut>, ISinkDesigner<TIn, TOut>, ISinkDesignerIfElse<TIn, TOut>
    {
        private readonly ILogger<StreamBuilder<TIn, TOut>> _logger;
        private readonly IXClient _xClient;
        private readonly string _streamName;
        private readonly StreamSettings _streamSettings;

        private Producer<object, TOut> producerStream;
        private Producer<object, TOut> producerStreamElse;
        private IConsumer<object, TIn> consumerStream;

        private Func<Message<TIn>, TOut> _funcMapper;
        private Func<Message<TIn>, TOut> _funcMapperIfTrue;
        private Func<Message<TIn>, TOut> _funcMapperIfFalse;

        private IFlowDesigner<TIn, TOut>.Condition _condition;

        private MapFunctionCall mapFunctionCall;
        private StreamType streamType;

        private StreamBuilder(IXClient xClient, string streamName, StreamSettings streamSettings)
        {
            _xClient = xClient;
            _streamName = streamName;
            _streamSettings = streamSettings;

            _logger = xClient
                .GetClientConfiguration().Settings.Logging
                .GetLoggerFactory()
                .CreateLogger<StreamBuilder<TIn, TOut>>();

            streamType = StreamType.None;
        }

        public static StreamBuilder<TIn, TOut> CreateNewStream(IXClientFactory xClientFactory, string streamName, StreamSettings streamSettings = null)
        {
            if (streamSettings == null)
                return new StreamBuilder<TIn, TOut>(xClientFactory.CreateClient(), streamName, new StreamSettings());
            else
                return new StreamBuilder<TIn, TOut>(xClientFactory.CreateClient(), streamName, streamSettings);
        }
        public static StreamBuilder<TIn, TOut> CreateNewStream(IXClient xClient, string streamName, StreamSettings streamSettings = null)
        {
            if (streamSettings == null)
                return new StreamBuilder<TIn, TOut>(xClient, streamName, new StreamSettings());
            else
                return new StreamBuilder<TIn, TOut>(xClient, streamName, streamSettings);
        }

        public IFlowDesigner<TIn, TOut> Stream(SourceTopic sourceTopic)
        {
            consumerStream = Consumer<object, TIn>
                .CreateNewConsumer(_xClient)
                .ForComponent(sourceTopic.Component)
                .AndTopic(sourceTopic.Topic)
                .WithName(_streamName)
                .AndSubscription(subscription =>
                {
                    subscription.Name = _streamName;
                    subscription.Type = _streamSettings.ConsumptionInstanceType;
                    subscription.Mode = _streamSettings.ConsumptionMode;
                    subscription.InitialPosition = _streamSettings.ConsumptionInitialPosition;
                })
                .Build();

            consumerStream.MessageReceivedHandler((key, message) =>
            {
                try
                {
                    if (streamType == StreamType.Map)
                    {
                        switch (mapFunctionCall)
                        {
                            case MapFunctionCall.Map:
                                ExecuteMapFunction(key, message);
                                break;
                            case MapFunctionCall.MapIf:
                                ExecuteMapIfFunction(key, message);

                                break;
                            case MapFunctionCall.MapIfElse:
                                ExecuteMapIfElseFunction(key, message);
                                break;
                            default:
                                break;
                        }
                    }
                    else
                    {
                        // stream type is Filter
                        ExecuteFilterFunction(key, message);
                    }

                }
                catch (Exception)
                {
                    (consumerStream as Consumer<object, TIn>).UnacknowledgeMessage(message);
                }
            });

            return this;
        }

        public ISinkDesigner<TIn, TOut> Map(Func<Message<TIn>, TOut> filterFunction)
        {
            mapFunctionCall = MapFunctionCall.Map;
            streamType = StreamType.Map;

            _funcMapper = filterFunction;
            return this;
        }

        public ISinkDesigner<TIn, TOut> MapIf(IFlowDesigner<TIn, TOut>.Condition condition, Func<Message<TIn>, TOut> filterFunction)
        {
            mapFunctionCall = MapFunctionCall.MapIf;
            streamType = StreamType.Map;

            _funcMapperIfTrue = filterFunction;
            _condition = condition;

            return this;
        }

        public ISinkDesignerIfElse<TIn, TOut> MapIf(IFlowDesigner<TIn, TOut>.Condition condition, Func<Message<TIn>, TOut> filterFunctionIfTrue, Func<Message<TIn>, TOut> filterFunctionIfFalse)
        {
            mapFunctionCall = MapFunctionCall.MapIfElse;
            streamType = StreamType.Map;

            _funcMapperIfTrue = filterFunctionIfTrue;
            _funcMapperIfFalse = filterFunctionIfFalse;

            _condition = condition;

            return this;
        }

        public IStreamBuilder<TIn, TOut> To(SinkTopic sinkTopic)
        {
            producerStream = Producer<object, TOut>
                .CreateNewProducer(_xClient)
                .ForComponent(sinkTopic.Component)
                .AndTopic(sinkTopic.Topic)
                .WithName(_streamName)
                .WithSettings(settings =>
                {
                    settings.RequireCallback = _streamSettings.RequireCallbackInSink;
                })
                .AddDefaultHeader("stream-version", "andyx-streams v3.1.0")
                .Build();

            return this;
        }

        public IStreamBuilder<TIn, TOut> To(SinkTopic sinkTopicTrue, SinkTopic sinkTopicElse)
        {
            producerStream = Producer<object, TOut>
                .CreateNewProducer(_xClient)
                .ForComponent(sinkTopicTrue.Component)
                .AndTopic(sinkTopicTrue.Topic)
                .WithName(_streamName)
                .WithSettings(settings =>
                {
                    settings.RequireCallback = _streamSettings.RequireCallbackInSink;
                })
                .AddDefaultHeader("stream-version", "andyx-streams v3.1.0")
                .Build();

            producerStreamElse = Producer<object, TOut>
                .CreateNewProducer(_xClient)
                .ForComponent(sinkTopicElse.Component)
                .AndTopic(sinkTopicElse.Topic)
                .WithName(_streamName)
                .WithSettings(settings =>
                {
                    settings.RequireCallback = _streamSettings.RequireCallbackInSink;
                })
                .AddDefaultHeader("stream-version", "andyx-streams v3.1.0")
                .Build();

            return this;
        }

        public Stream<TIn, TOut> Run()
        {
            // if is filter
            if (streamType == StreamType.Filter)
            {
                if (typeof(TIn) != typeof(TOut))
                {
                    throw new Exception("Input type and Output type should be the same.");
                }
            }

            producerStream
                .OpenAsync()
                .Wait();

            Thread.Sleep(500);

            if (producerStreamElse != null)
            {
                producerStreamElse
                    .OpenAsync()
                    .Wait();
                Thread.Sleep(500);
            }

            consumerStream
                .SubscribeAsync()
                .Wait();

            Thread.Sleep(500);

            return new Stream<TIn, TOut>();
        }


        private void ExecuteMapFunction(object key, Message<TIn> message)
        {
            var mapped = _funcMapper(message);
            producerStream.SendAsync(key, mapped);
            (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
        }

        private void ExecuteMapIfFunction(object key, Message<TIn> message)
        {
            if (_condition(message.Payload) == true)
            {
                var mapped = _funcMapperIfTrue(message);
                producerStream.SendAsync(key, mapped);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
            else
            {
                (consumerStream as Consumer<object, TIn>).SkipMessage(message);
            }
        }
        private void ExecuteMapIfElseFunction(object key, Message<TIn> message)
        {
            if (_condition(message.Payload) == true)
            {
                var mapped = _funcMapperIfTrue(message);
                producerStream.SendAsync(key, mapped);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
            else
            {
                var mapped = _funcMapperIfFalse(message);
                producerStream.SendAsync(key, mapped);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
        }

        private void ExecuteFilterFunction(object key, Message<TIn> message)
        {
            if (_condition(message.Payload) == true)
            {
                producerStream.SendAsync(key, (message as Message<TOut>).Payload);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
        }

        public ISinkDesigner<TIn, TOut> Filter(IFlowDesigner<TIn, TOut>.Condition filterCondition)
        {
            streamType = StreamType.Filter;
            _condition = filterCondition;

            return this;
        }
    }
}
