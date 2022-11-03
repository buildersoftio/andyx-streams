namespace Andy.X.Streams.Abstractions
{
    public interface IStreamDesigner<TIn, TOut>
    {
        public IFlowDesigner<TIn, TOut> Stream(SourceTopic sourceTopic);
    }
}
