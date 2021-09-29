using System;

namespace ParquetFileViewer.CustomGridTypes
{
    public class SimpleValue : ValueBase
    {
        private object value;

        public override object Value => this.value;

        public SimpleValue(object value)
        {
            if (value == null)
                this.value = DBNull.Value;
            else
                this.value = value;
        }

        public override string ToString()
        {
            return this.value?.ToString();
        }

        public override bool IsDBNull()
        {
            return this.value == DBNull.Value || this.value == null;
        }
    }
}
