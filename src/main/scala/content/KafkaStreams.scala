package content

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{
  GlobalKTable,
  JoinWindows,
  TimeWindows,
  Windowed
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(
        orderId: OrderId,
        user: UserId,
        products: List[Product],
        amount: Double
    )
    case class Discount(
        profile: Profile,
        amount: Double
    ) // in percentage points
    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  def main(args: Array[String]): Unit = {

    //source = emjts elements
    //flows = transforms elemtns along the way (e.g map)
    // sink = "ingests" elements
    import Topics._
    import Domain._

    implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] = {
      val serializer = (a: A) => a.asJson.noSpaces.getBytes()
      val deserializer = (bytes: Array[Byte]) => {
        val json = new String(bytes)
        decode[A](json).toOption
      }
      Serdes.fromFn[A](serializer, deserializer)
    }

    // topology
    val builder = new StreamsBuilder()

    // KStream
    val usersOrdersStream: KStream[UserId, Order] =
      builder.stream[UserId, Order](OrdersByUserTopic)

    //KTable
    val userProfilesTable: KTable[UserId, Profile] =
      builder.table[UserId, Profile](DiscountProfilesByUserTopic)

    //GlobalKTable
    val discountsTable: GlobalKTable[Profile, Discount] =
      builder.globalTable[Profile, Discount](DiscountsTopic)

    val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

    val listsOfProducts: KStream[UserId, List[Product]] =
      expensiveOrders.mapValues { order =>
        order.products
      }

    val productsStream: KStream[UserId, Product] =
      usersOrdersStream.flatMapValues { order =>
        order.products
      }

    //join
    val ordersWithUserProfiles = usersOrdersStream.join(userProfilesTable) {
      (order, profile) =>
        (order, profile)
    }

    val discountedOrdersStream: KStream[UserId, Order] =
      ordersWithUserProfiles.join[Profile, Discount, Order](discountsTable)(
        { case (userId, (order, profile)) => profile }, // Joining key
        { case ((order, profile), discount) =>
          order.copy(amount = order.amount * discount.amount)
        }
      )

    //pick another identifier
    val ordersStream =
      discountedOrdersStream.selectKey((userId, order) => order.orderId)
    val paymentsStream = builder.stream[OrderId, Payment](PaymentsTopic)

    val joinWidow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    val ordersPaid: KStream[OrderId, Order] = {
      val joinOrdersAndPayments = (order: Order, payment: Payment) =>
        if (payment.status == "PAID") Option(order) else Option.empty[Order]
      val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
      ordersStream
        .join[Payment, Option[Order]](paymentsStream)(
          joinOrdersAndPayments,
          joinWindow
        )
        .flatMapValues(maybeOrder => maybeOrder.toIterable)
    }

    ordersPaid.to(PaidOrdersTopic)

    val topology = builder.build();

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()
  }
}
