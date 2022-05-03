namespace Andy.X.Streams.Abstractions
{
    public interface ISinkDesigner
    {
        public IStreamBuilder To<TOut>(string component, string topic);
    }
}
