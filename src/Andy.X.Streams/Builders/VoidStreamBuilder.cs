using Andy.X.Client;
using Andy.X.Client.Abstractions;
using Andy.X.Client.Abstractions.Consumers;
using Andy.X.Client.Abstractions.XClients;
using Andy.X.Client.Models;
using Andy.X.Streams.Abstractions;
using Andy.X.Streams.Models.Internal;
using Andy.X.Streams.Settings;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace Andy.X.Streams.Builders
{
    public class StreamBuilder<TIn> : IVoidStreamBuilder<TIn>, IVoidStreamDesigner<TIn>, IVoidFlowDesigner<TIn>, IVoidSinkDesigner<TIn>
    {
        private readonly ILogger<StreamBuilder<TIn>> _logger;
        private readonly IXClient _xClient;
        private readonly string _streamName;
        private readonly StreamSettings _streamSettings;

        private IConsumer<object, TIn> consumerStream;

        private Action<Message<TIn>> _funcMapper;
        private Action<Message<TIn>> _funcMapperIfTrue;
        private Action<Message<TIn>> _funcMapperIfFalse;

        private IVoidFlowDesigner<TIn>.Condition _condition;

        private MapFunctionCall mapFunctionCall;

        public StreamBuilder(IXClient xClient, string streamName, StreamSettings streamSettings)
        {
            _xClient = xClient;
            _streamName = streamName;
            _streamSettings = streamSettings;

            _logger = xClient
                .GetClientConfiguration().Settings.Logging
                .GetLoggerFactory()
                .CreateLogger<StreamBuilder<TIn>>();
        }
        public static StreamBuilder<TIn> CreateNewStream(IXClientFactory xClientFactory, string streamName, StreamSettings streamSettings = null)
        {
            if (streamSettings == null)
                return new StreamBuilder<TIn>(xClientFactory.CreateClient(), streamName, new StreamSettings());
            else
                return new StreamBuilder<TIn>(xClientFactory.CreateClient(), streamName, streamSettings);
        }
        public static StreamBuilder<TIn> CreateNewStream(IXClient xClient, string streamName, StreamSettings streamSettings = null)
        {
            if (streamSettings == null)
                return new StreamBuilder<TIn>(xClient, streamName, new StreamSettings());
            else
                return new StreamBuilder<TIn>(xClient, streamName, streamSettings);
        }

        public IVoidFlowDesigner<TIn> Stream(SourceTopic sourceTopic)
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
                catch (Exception)
                {
                    (consumerStream as Consumer<object, TIn>).UnacknowledgeMessage(message);
                }
            });

            return this;
        }

        public IVoidSinkDesigner<TIn> Map(Action<Message<TIn>> action)
        {
            mapFunctionCall = MapFunctionCall.Map;

            _funcMapper = action;
            return this;
        }
        public IVoidSinkDesigner<TIn> MapIf(IVoidFlowDesigner<TIn>.Condition condition, Action<Message<TIn>> action)
        {
            mapFunctionCall = MapFunctionCall.MapIf;

            _funcMapperIfTrue = action;
            _condition = condition;

            return this;
        }
        public IVoidSinkDesigner<TIn> MapIf(IVoidFlowDesigner<TIn>.Condition condition, Action<Message<TIn>> actionIfTrue, Action<Message<TIn>> actionIfFalse)
        {
            mapFunctionCall = MapFunctionCall.MapIfElse;

            _funcMapperIfTrue = actionIfTrue;
            _funcMapperIfFalse = actionIfFalse;

            _condition = condition;

            return this;
        }

        public IVoidStreamBuilder<TIn> To()
        {
            // Ignore Sink.
            return this;
        }

        public Stream<TIn> Run()
        {
            consumerStream
                .SubscribeAsync()
                .Wait();

            Thread.Sleep(500);

            return new Stream<TIn>();
        }

        private void ExecuteMapFunction(object key, Message<TIn> message)
        {
            _funcMapper(message);
            (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
        }
        private void ExecuteMapIfFunction(object key, Message<TIn> message)
        {
            if (_condition(message.Payload) == true)
            {
                _funcMapperIfTrue(message);
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
                _funcMapperIfTrue(message);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
            else
            {
                _funcMapperIfFalse(message);
                (consumerStream as Consumer<object, TIn>).AcknowledgeMessage(message);
            }
        }
    }
}
