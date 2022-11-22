namespace Andy.X.Streams.Abstractions
{
    public interface IVoidStreamBuilder<TIn>
    {
        public Stream<TIn> Run();
    }
}
