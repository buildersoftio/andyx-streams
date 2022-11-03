namespace Andy.X.Streams.Abstractions
{
    public interface IVoidStreamDesigner<TIn>
    {
        public IVoidFlowDesigner<TIn> Stream(SourceTopic sourceTopic);
    }
}
