namespace Andy.X.Streams
{
    public class SinkTopic
    {
        public string Component { get; private set; }
        public string Topic { get; private set; }

        public SinkTopic(string component, string topic)
        {
            Component = component;
            Topic = topic;
        }
    }
}
