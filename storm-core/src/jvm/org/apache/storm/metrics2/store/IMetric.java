public interface IMetric <TKey, TValue>{
    byte[] getKey();

    byte[] getValue();
}
