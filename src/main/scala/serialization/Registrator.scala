package serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) = {
    kryo.register(classOf[Set[String]])
    kryo.register(classOf[String])
  }
}