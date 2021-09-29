namespace ParquetFileViewer.CustomGridTypes
{
    /// <summary>
    /// I wish we could use an Interface but then the DataGridView doesn't execute .ToString()
    /// on the data. So all the cells end up being blank. Thus, we're using an abstract class.
    /// </summary>
    public abstract class ValueBase
    {
        public abstract object Value { get; }

        public abstract bool IsDBNull();
    }
}
