namespace Andy.X.Streams.Abstractions
{
    public interface ISinkDesigner<TIn, TOut>
    {
        public IStreamBuilder<TIn, TOut> To(SinkTopic sinkTopic);
    }

    public interface ISinkDesignerIfElse<TIn, TOut>
    {
        public IStreamBuilder<TIn, TOut> To(SinkTopic sinkTopicTrue, SinkTopic sinkTopicElse);
    }
}
