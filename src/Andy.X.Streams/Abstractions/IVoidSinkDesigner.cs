namespace Andy.X.Streams.Abstractions
{
    public interface IVoidSinkDesigner<TIn>
    {
        public IVoidStreamBuilder<TIn> To();
    }
}
