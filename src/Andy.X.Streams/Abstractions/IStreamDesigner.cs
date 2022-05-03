namespace Andy.X.Streams.Abstractions
{
    public interface IStreamDesigner
    {
        public IFlowDesigner Stream<TIn>(string component, string topic);
    }
}
