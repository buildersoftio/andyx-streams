namespace Andy.X.Streams.Abstractions
{
    public interface IStreamBuilder<TIn, TOut>
    {
        public Stream<TIn, TOut> Run();
    }
}
