using Parquet;
using ParquetFileViewer.CustomGridTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

namespace ParquetFileViewer.Helpers
{
    public static class UtilityMethods
    {
        public static DataTable ParquetReaderToDataTable(ParquetReader parquetReader, List<string> selectedFields, int offset, int recordCount, CancellationToken cancellationToken)
        {
            //Get list of data fields and construct the DataTable
            DataTable dataTable = new DataTable();
            List<Parquet.Data.Field> fields = new List<Parquet.Data.Field>();
            foreach (string selectedField in selectedFields)
            {
                var field = parquetReader.Schema.Fields.FirstOrDefault(f => f.Name.Equals(selectedField, StringComparison.InvariantCultureIgnoreCase));
                if (field != null)
                {
                    fields.Add(field);
                    DataColumn newColumn = new DataColumn(field.Name, typeof(ValueBase));
                    dataTable.Columns.Add(newColumn);
                }
                else
                    throw new Exception(string.Format("Field '{0}' does not exist", selectedField));
            }

            //Read column by column to generate each row in the datatable
            int totalRecordCountSoFar = 0;
            int rowsLeftToRead = recordCount;
            for (int i = 0; i < parquetReader.RowGroupCount; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                {
                    if (groupReader.RowCount > int.MaxValue)
                        throw new ArgumentOutOfRangeException(string.Format("Cannot handle row group sizes greater than {0}", groupReader.RowCount));

                    int rowsPassedUntilThisRowGroup = totalRecordCountSoFar;
                    totalRecordCountSoFar += (int)groupReader.RowCount;

                    if (offset >= totalRecordCountSoFar)
                        continue;

                    if (rowsLeftToRead > 0)
                    {
                        int numberOfRecordsToReadFromThisRowGroup = Math.Min(Math.Min(totalRecordCountSoFar - offset, rowsLeftToRead), (int)groupReader.RowCount);
                        rowsLeftToRead -= numberOfRecordsToReadFromThisRowGroup;

                        int recordsToSkipInThisRowGroup = Math.Max(offset - rowsPassedUntilThisRowGroup, 0);

                        ProcessRowGroup(dataTable, groupReader, fields, recordsToSkipInThisRowGroup, numberOfRecordsToReadFromThisRowGroup, cancellationToken);
                    }
                    else
                        break;
                }
            }

            return dataTable;
        }

        private static void ProcessRowGroup(DataTable dataTable, ParquetRowGroupReader groupReader, List<Parquet.Data.Field> fields,
            int skipRecords, int readRecords, CancellationToken cancellationToken)
        {
            int rowBeginIndex = dataTable.Rows.Count;
            bool isFirstColumn = true;

            foreach (var field in fields)
            {
                int rowIndex = rowBeginIndex;
                if (cancellationToken.IsCancellationRequested)
                    break;

                var test = new ParquetFieldEnumerator(field, groupReader);
                foreach (var row in test)
                {
                    if (isFirstColumn)
                    {
                        var newRow = dataTable.NewRow();
                        dataTable.Rows.Add(newRow);
                    }

                    dataTable.Rows[rowIndex][field.Name] = row;
                    rowIndex++;
                }
                isFirstColumn = false;

                /*
                int rowIndex = rowBeginIndex;
                int skippedRecords = 0;
                var column = groupReader.ReadColumn(dataField);

                if (column.HasRepetitions) //List or Array field (Are nested lists a thing? If they are... we're not handling them)
                {
                    #region helper function
                    Action<ArrayList> saveRow = (ArrayList rowToSave) => {
                        if (isFirstColumn)
                        {
                            var newRow = dataTable.NewRow();
                            dataTable.Rows.Add(newRow);
                        }

                        var listType = ParquetNetTypeToCSharpType(column.Field);
                        if (rowToSave.Count == 1 && (rowToSave[0] == null || rowToSave[0] == DBNull.Value))
                            dataTable.Rows[rowIndex][field.Name] = new ListValue(Array.CreateInstance(listType, 0)); //single null element means empty array
                        else
                            dataTable.Rows[rowIndex][field.Name] = new ListValue((ArrayList)rowToSave.Clone());

                        rowIndex++;
                    };
                    #endregion

                    int elementIndex = 0;
                    var row = new ArrayList();
                    foreach (var repititionLevel in column.RepetitionLevels)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        if (repititionLevel == 0) //new row
                        {
                            if (elementIndex != 0)
                            {
                                if (skipRecords > skippedRecords)
                                    skippedRecords++; //skip this row
                                else if (rowIndex - rowBeginIndex >= readRecords)
                                {
                                    row = null;
                                    break; //we have all the rows we need
                                }
                                else
                                    saveRow(row);
                            }

                            if (rowIndex - rowBeginIndex >= readRecords) //check if NOW we have all we need
                            {
                                row = null;
                                break;
                            }
                            else
                            {
                                row.Clear();
                                row.Add(ProcessParquetValue(column.Data.GetValue(elementIndex), column.Field.DataType));
                            }
                        }
                        else //same row
                        {
                            row.Add(ProcessParquetValue(column.Data.GetValue(elementIndex), column.Field.DataType));
                        }

                        elementIndex++;
                    }

                    //process final row
                    if (row != null)
                        saveRow(row);
                }
                else
                {
                    foreach (var value in column.Data)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        if (skipRecords > skippedRecords)
                        {
                            skippedRecords++;
                            continue;
                        }

                        if (rowIndex - rowBeginIndex >= readRecords)
                            break;

                        if (isFirstColumn)
                        {
                            var newRow = dataTable.NewRow();
                            dataTable.Rows.Add(newRow);
                        }

                        dataTable.Rows[rowIndex][field.Name] = ProcessParquetValue(value, dataField.DataType);
                        rowIndex++;
                    }
                }

                isFirstColumn = false;
                */
            }
        }

        public static string CleanCSVValue(string value, bool alwaysEncloseInQuotes = false)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                //In RFC 4180 we escape quotes with double quotes
                string formattedValue = value.Replace("\"", "\"\"");

                //Enclose value with quotes if it contains commas,line feeds or other quotes
                if (formattedValue.Contains(",") || formattedValue.Contains("\r") || formattedValue.Contains("\n") || formattedValue.Contains("\"\"") || alwaysEncloseInQuotes)
                    formattedValue = string.Concat("\"", formattedValue, "\"");

                return formattedValue;
            }
            else
                return string.Empty;
        }

        public static IEnumerable<ICollection<T>> Split<T>(IEnumerable<T> src, int maxItems)
        {
            var list = new List<T>();
            foreach (var t in src)
            {
                list.Add(t);
                if (list.Count == maxItems)
                {
                    yield return list;
                    list = new List<T>();
                }
            }

            if (list.Count > 0)

                yield return list;
        }

        public static DataTable MergeTables(IEnumerable<DataTable> additionalTables)
        {
            // Build combined table columns
            DataTable merged = null;
            foreach (DataTable dt in additionalTables)
            {
                if (merged == null)
                    merged = dt;
                else
                    merged = AddTable(merged, dt);
            }
            return merged ?? new DataTable();
        }

        private static DataTable AddTable(DataTable baseTable, DataTable additionalTable)
        {
            // Build combined table columns
            DataTable merged = baseTable.Clone(); // Include all columns from base table in result.
            foreach (DataColumn col in additionalTable.Columns)
            {
                string newColumnName = col.ColumnName;
                merged.Columns.Add(newColumnName, col.DataType);
            }
            // Add all rows from both tables
            var bt = baseTable.AsEnumerable();
            var at = additionalTable.AsEnumerable();
            var mergedRows = bt.Zip(at, (r1, r2) => r1.ItemArray.Concat(r2.ItemArray).ToArray());
            foreach (object[] rowFields in mergedRows)
            {
                merged.Rows.Add(rowFields);
            }
            return merged;
        }

        private static object ProcessParquetValue(object value, Parquet.Data.DataType dataType)
        {
            if (value == null)
                return DBNull.Value;
            else if (dataType == Parquet.Data.DataType.DateTimeOffset)
                return ((DateTimeOffset)value).DateTime; //converts to local time!
            else
                return value;
        }
    }
}
