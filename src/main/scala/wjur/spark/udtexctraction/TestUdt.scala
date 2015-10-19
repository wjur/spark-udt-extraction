package wjur.spark.udtexctraction

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._



class TestUdt extends UserDefinedType[TestUdtValue] {
  override def sqlType: DataType = StructType(Seq(
    StructField("i", StringType),
    StructField("d", StringType)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case TestUdtValue(i, d) =>
        val row = new GenericMutableRow(2)
        row.setInt(0, i)
        row.setDouble(1, d)
        row
    }
  }

  override def userClass: Class[TestUdtValue] = classOf[TestUdtValue]

  override def deserialize(datum: Any): TestUdtValue = {
    datum match {
      case r: InternalRow =>
        TestUdtValue(
          r.getInt(0),
          r.getDouble(1)
        )
    }
  }
}
