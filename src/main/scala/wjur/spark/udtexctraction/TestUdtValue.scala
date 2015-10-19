package wjur.spark.udtexctraction

import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * @author Wojciech Jurczyk <wojtek.jurczyk@gmail.com>.
 */
@SQLUserDefinedType(udt = classOf[TestUdt])
case class TestUdtValue(i: Int, d: Double) extends Serializable
