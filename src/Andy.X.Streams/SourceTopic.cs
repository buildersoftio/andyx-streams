namespace Andy.X.Streams
{
    public class SourceTopic
    {
        public string Component { get; private set; }
        public string Topic { get; private set; }

        public SourceTopic(string component, string topic)
        {
            Component = component;
            Topic = topic;
        }
    }
}
