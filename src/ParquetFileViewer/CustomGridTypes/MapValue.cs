using System.Text;

namespace ParquetFileViewer.CustomGridTypes
{
    public class MapValue : ValueBase
    {
        public ValueBase key { get; }
        public ValueBase value { get; }

        /// <summary>
        /// This is the display value to be used in the DataGridView
        /// </summary>
        public override object Value => this.IsDBNull() ? (object)System.DBNull.Value : (object)this.ToString();

        public MapValue(ValueBase key, ValueBase value)
        {
            this.key = key;
            this.value = value;
        }

        public override bool IsDBNull()
        {
            return this.key.IsDBNull();
        }

        public ValueBase GetMapKey()
        {
            return this.key;
        }

        public ValueBase GetMapValue()
        {
            return this.value;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("(");

            sb.Append(this.key?.ToString() ?? string.Empty);
            sb.Append(",");
            sb.Append(this.value?.ToString() ?? string.Empty);
            sb.Append(")");

            return sb.ToString();
        }
    }
}
