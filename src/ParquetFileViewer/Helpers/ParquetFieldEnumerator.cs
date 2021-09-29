using Parquet;
using Parquet.Data;
using ParquetFileViewer.CustomGridTypes;
using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;

namespace ParquetFileViewer.Helpers
{
    /// <summary>
    /// Welcome to recursion hell! Population: YOU
    /// It isn't easy enumerating a Parquet file with nested structures on a 
    /// traditional row-by-row basis. But I feel like I'm close...
    /// </summary>
    public class ParquetFieldEnumerator : IEnumerable<ValueBase>
    {
        public Field Field { get; }
        public ParquetRowGroupReader Reader { get; }

        private int lastValueSkipCount;

        public ParquetFieldEnumerator(Field field, ParquetRowGroupReader reader)
        {
            this.Field = field;
            this.Reader = reader;
        }

        public IEnumerator<ValueBase> GetEnumerator()
        {
            foreach (var value in HandleField(this.Field))
                yield return value;
        }

        private IEnumerable<ValueBase> HandleField(Field field)
        {
            if (field.SchemaType == SchemaType.Data)
            {
                if (field is DataField dataField)
                {
                    var dataColumn = this.Reader.ReadColumn(dataField);
                    foreach (var value in ReadDataColumn(dataColumn))
                    {
                        yield return value;

                        //if (value is ValueBase)
                        //{

                        //}
                        //else if (value is ArrayList al)
                        //{
                        //    var list = new ListValue();
                        //    foreach (var innerValue in al)
                        //    {
                        //        if (innerValue == null || innerValue == DBNull.Value)
                        //            list.Value.Add(new SimpleValue(null));
                        //        else if (innerValue.GetType().IsSubclassOf(typeof(Field)))
                        //        {
                        //            foreach (var innerInnerValue in HandleField((Field)innerValue))
                        //                list.Value.Add(innerInnerValue);
                        //        }
                        //        else
                        //            list.Value.Add(new SimpleValue(innerValue));
                        //    }
                        //    yield return list;
                        //}
                        //else
                        //    throw new Exception("oh no");
                    }
                }
                else
                    throw new Exception($"Something is wrong with DataField: {this.Field?.Name}");
            }
            else if (field.SchemaType == SchemaType.List)
            {
                if (field is ListField listField)
                {
                    foreach (var value in HandleField(listField.Item))
                    {
                        yield return value;
                    }
                }
                else
                    throw new Exception($"Something is wrong with ListField: {this.Field?.Name}");
            }
            else if (field.SchemaType == SchemaType.Map)
            {
                if (field is MapField mapField)
                {
                    if (mapField.Key == null)
                        throw new Exception($"Map field should not be null: {mapField.Name}");
                    else if (mapField.Key.GetType().IsSubclassOf(typeof(Field))
                        && mapField.Value.GetType().IsSubclassOf(typeof(Field)))
                    {
                        IEnumerator<ValueBase> keys = HandleField(mapField.Key).GetEnumerator();
                        IEnumerator<ValueBase> values = HandleField(mapField.Value).GetEnumerator();

                        while (keys.MoveNext())
                        {
                            var key = keys.Current;
                            
                            //Skip already processed stuff (keys are progressing but the values enumeration resets after each "yield return mapList;" below
                            while(lastValueSkipCount - 1 > 0)
                            {
                                lastValueSkipCount--;
                                values.MoveNext();
                            }

                            while(values.MoveNext())
                            {
                                var value = values.Current;
                                if (key is ListValue k && value is ListValue v)
                                {
                                    if (k.Count() > 1) 
                                    {
                                        var list = new ListValue();

                                        foreach (var map in k.Zip(v, (keyValue, valueValue) => new MapValue(keyValue, valueValue)))
                                            list.AddValue(map);

                                        yield return list;
                                    }
                                    else 
                                    {
                                        var map = new MapValue(k.First(), v.Count() > 1 ? v : v.First());

                                        //top level maps are always in a list (i think)
                                        var mapList = new ListValue();
                                        mapList.AddValue(map);

                                        //Now advance the key value so we can go to the next row (hopefully this is right?)
                                        //if (keys.MoveNext())
                                        //    key = keys.Current;

                                        //Need to some how remember how many levels i need to skip in the values for the next key that will be called later
                                        lastValueSkipCount++; //<============     but this isn't working here.....
                                        yield return mapList;
                                        break; //We're done with the values enumeration. It will reset for the next key and we will skip until we get back to where we were...Hope that makes sense.
                                    }
                                }
                                else 
                                    throw new Exception($"Unsupported Map field: {field.Name}");
                            }
                        }
                    }
                    else
                        throw new Exception($"Unsupported variation of MapField: {mapField.Name}");
                }
                else
                    throw new Exception($"Something is wrong with MapField: {this.Field?.Name}");
            }
            else
                throw new Exception("Unsupported Schema Type");
        }

        /// <summary>
        /// Based on the logic provided here: https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
        /// </summary>
        private IEnumerable<ValueBase> ReadDataColumn(DataColumn dataColumn)
        {
            if (!dataColumn.HasRepetitions) //Simple type
            {
                foreach (var value in dataColumn.Data)
                    yield return new SimpleValue(value);
            }
            else //Complex type
            {
                int elementIndex = 0;
                var list = new ListValue();
                bool wasLastValueNull = false;
                foreach (var repititionLevel in dataColumn.RepetitionLevels)
                {
                    if (repititionLevel == 0) //new row
                    {
                        if (elementIndex != 0)
                        {
                            //row is ready, go to the top level
                            yield return list.GetTopLevel();

                            //prepare for next row
                            list = new ListValue();
                        }

                        //create nested lists up to MaxRepititionLevel (excluding the one we already created above)
                        for (int i = 0; i < dataColumn.Field.MaxRepetitionLevel - 1; i++)
                        {
                            list = list.IncreaseLevel();
                        }

                        list.AddValue(new SimpleValue(dataColumn.Data.GetValue(elementIndex)));
                    }
                    else //same row
                    {
                        if (repititionLevel < list.Level)
                        {
                            while (repititionLevel < list.Level)
                            {
                                list = list.DecreaseLevel();
                            }

                            //Need to start a new level at the end of decreasing (that's the rule :shrug:)
                            list = list.IncreaseLevel();
                            wasLastValueNull = false; //just in case
                        }
                        else if (repititionLevel > list.Level)
                        {
                            while (repititionLevel > list.Level)
                            {
                                list = list.IncreaseLevel();
                            }
                        }
                        else if (repititionLevel == list.Level && wasLastValueNull
                            && list.Level != dataColumn.Field.MaxRepetitionLevel)
                        {
                            //i don't know if this is correct... it works on the two sample files I have
                            list = list.IncreaseLevel();
                            wasLastValueNull = false;
                        }

                        var value = new SimpleValue(dataColumn.Data.GetValue(elementIndex));
                        list.AddValue(value);
                        wasLastValueNull = value.IsDBNull();
                    }

                    elementIndex++;
                }

                //Return final value
                yield return list.GetTopLevel();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
