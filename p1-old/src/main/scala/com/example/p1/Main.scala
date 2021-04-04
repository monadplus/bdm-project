package com.example.p1

import io.circe._
import io.circe.generic.auto._
import io.circe.jawn._
import java.io.File

sealed trait PropertyType
case object Flat extends PropertyType
case object Duplex extends PropertyType
case object Chalet extends PropertyType
case object PentHouse extends PropertyType
case object CountryHouse extends PropertyType
case object Studio extends PropertyType

object CoproductDecoder {
  def apply[A](f: String => Either[Unit, A]): Decoder[A] =
    new Decoder[A] {
      final def apply(c: HCursor): Decoder.Result[A] =
        for {
          str <- c.as[String]
          r <- f(str) match {
            case Right(r) => Right(r)
            case Left(_)  => Left(DecodingFailure("Unexpected: " ++ str, List()))
          }
        } yield r
    }
}

object PropertyType {
  implicit val decodePropertyType: Decoder[PropertyType] =
    CoproductDecoder {
      _ match {
        case "flat"         => Right(Flat)
        case "duplex"       => Right(Duplex)
        case "chalet"       => Right(Chalet)
        case "penthouse"    => Right(PentHouse)
        case "studio"       => Right(Studio)
        case "countryHouse" => Right(CountryHouse)
        case _              => Left(())
      }
    }
}

sealed trait Operation
case object Sale extends Operation
case object Rent extends Operation

object Operation {
  implicit val decodeOperation: Decoder[Operation] =
    CoproductDecoder {
      _ match {
        case "sale" => Right(Sale)
        case "rent" => Right(Rent)
        case _      => Left(())
      }
    }
}

case class Address(name: String)
object Address {
  implicit val decodeAddress: Decoder[Address] =
    Decoder[String].map(Address.apply)
}

case class Meters(meters: Int)
object Meters {
  implicit val decodeMeters: Decoder[Meters] =
    Decoder[Int].map(Meters.apply)
}

case class SMeters(squaredMeters: Double)
object SMeters {
  implicit val decodeSMeters: Decoder[SMeters] =
    Decoder[Double].map(SMeters.apply)
}

case class Housing(
    neighborhood: String,
    district: String,
    price: Double,
    propertyCode: String,
    thumbnail: Option[String],
    numPhotos: Int,
    floor: Option[String],
    propertyType: PropertyType,
    operation: Operation,
    size: SMeters,
    exterior: Boolean,
    rooms: Int,
    bathrooms: Int,
    address: Option[Address],
    province: String,
    municipality: String,
    country: String,
    latitude: Double,
    longitude: Double,
    showAddress: Boolean,
    url: String,
    distance: Meters,
    hasVideo: Boolean,
    status: Option[String],
    newDevelopment: Boolean,
    hasLift: Option[Boolean],
    priceByArea: Double,
    detailedType: JsonObject,
    suggestedTexts: JsonObject,
    hasPlan: Boolean,
    has3DTour: Boolean,
    has360: Boolean,
    hasStaging: Boolean,
    topNewDevelopment: Boolean
)

object Main extends App {
  val idealista = new File("../datasets/idealista").listFiles
    .filter(_.getName.endsWith(".json"))

  idealista.foreach { file =>
    println(file)
    decodeFile[List[Json]](file) match {
      case Left(err) =>
        println(err)
        System.exit(-1)
      case Right(jsons) =>
        val housings = jsons.flatMap { json =>
          json.as[Housing] match {
            case Left(_)        => None
            case Right(housing) => Some(housing)
          }
        }
        housings.foreach(x => println(x.district ++ " - " ++ x.neighborhood))
    }
  }
}
