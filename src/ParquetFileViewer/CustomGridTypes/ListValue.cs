using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using ParquetFileViewer.Helpers;

namespace ParquetFileViewer.CustomGridTypes
{
    public class ListValue : ValueBase, IEnumerable<ValueBase>
    {
        private List<ValueBase> value;
        private ListValue parent;

        public int Level { get; private set; }

        public override object Value => this.IsDBNull() ? (object)System.DBNull.Value : (object)this.ToString();

        public ListValue()
        {
            this.value = new List<ValueBase>();
            this.Level = 1;
        }

        public ListValue IncreaseLevel()
        {
            var list = new ListValue
            {
                parent = this,
                Level = this.Level + 1
            };
            this.value.Add(list);
            return list;
        }

        public ListValue DecreaseLevel()
        {
            return this.parent;
        }

        public ListValue GetTopLevel()
        {
            var list = this;

            while (list.parent != null)
                list = list.parent;

            return list;
        }

        public override bool IsDBNull()
        {
            //If theres only one value in the list and it's null, i think that 
            //means the value is actually null. Is there a better way?
            return this.value.Count == 1 && this.value[0].IsDBNull();
        }

        public void AddValue(ValueBase value)
        {
            this.value.Add(value);
        }


        public int GetCount()
        {
            return this.value.Count;
        }

        public DataTable GetDataTable(string columnName)
        {
            var dt = new DataTable();
            dt.Columns.Add(new DataColumn(columnName, typeof(ValueBase)));

            foreach (var item in this.value)
            {
                var row = dt.NewRow();
                row[0] = item;
                dt.Rows.Add(row);
            }
            return dt;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("[");

            bool isFirst = true;
            foreach (var data in this.value)
            {
                if (!isFirst)
                    sb.Append(",");

                sb.Append(data?.ToString() ?? string.Empty);

                isFirst = false;
            }

            sb.Append("]");
            return sb.ToString();
        }

        public IEnumerator<ValueBase> GetEnumerator()
        {
            var list = this.value;
            foreach (var value in list)
            {
                var internalValue = value;
                if (internalValue is ListValue lv)
                {
                    foreach (var v in lv.RecursiveSelect((v) =>
                    {
                        if (v is ListValue lv1)
                            return lv1;
                        else
                            return new List<ValueBase>();
                    }))
                        yield return v;
                }
                else
                    yield return internalValue;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}