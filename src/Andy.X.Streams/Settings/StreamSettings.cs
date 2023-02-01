using Andy.X.Client.Abstractions.Serializers;
using Andy.X.Client.Configurations;

namespace Andy.X.Streams.Settings
{
    public sealed class StreamSettings
    {
        public SubscriptionMode ConsumptionMode { get; set; }
        public SubscriptionType ConsumptionInstanceType { get; set; }
        public InitialPosition ConsumptionInitialPosition { get; set; }
        public bool RequireCallbackInSink { get; set; }

        public IMessageSerializer CustomMessageSerializer { get; private set; } = null;

        public StreamSettings()
        {
            ConsumptionMode = SubscriptionMode.Resilient;
            ConsumptionInstanceType = SubscriptionType.Shared;
            ConsumptionInitialPosition = InitialPosition.Earliest;
            RequireCallbackInSink = true;
        }

        public void AddCustomMessageSerializer(IMessageSerializer messageSerializer)
        {
            CustomMessageSerializer = messageSerializer;
        }
    }
}

