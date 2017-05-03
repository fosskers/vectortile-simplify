package vp

import geotrellis.proj4.WebMercator
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerReader, FileLayerWriter}
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Feature, MultiPolygon, Polygon}
import geotrellis.vectortile._
import geotrellis.vectortile.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD

// --- //

object IO extends App {
  override def main(arg: Array[String]): Unit = {

    implicit val sc: SparkContext =
      SparkUtils.createLocalSparkContext("local[*]", "vp-simplify")

    /* Silence the damn INFO logger */
    Logger.getRootLogger().setLevel(Level.ERROR)

    /* For writing a compressed Tile Layer */
    val catalog: String = "/home/colin/tiles/"
    val store  = FileAttributeStore(catalog)
    val reader = FileLayerReader(store)
    val writer = FileLayerWriter(store)

    /*
    val rawcatalog: SpatialKey => String = SaveToHadoop.spatialKeyToPath(
      LayerId("vancouver-lite", 15),
      "/home/colin/rawtiles/{name}/{z}/{x}/{y}.mvt"
    )
     */

    val layout: LayoutDefinition =
      ZoomedLayoutScheme.layoutForZoom(15, WebMercator.worldExtent, 512)

    val manyTiles: RDD[(SpatialKey, VectorTile)] with Metadata[LayerMetadata[SpatialKey]] =
      reader.read(LayerId("north-van", 15))

    // TODO: Shouldn't be necessary, but it seems to be.
    val tiles = manyTiles.distinct

    println("Simplifying tiles...")

    val lighter: RDD[(SpatialKey, VectorTile)] = tiles.mapValues({v =>

      /* If the original Tile area had no Polygons in it, there would be no
       * `polygon` Layer present in the VT.
       */
      v.layers.get("polygons").map({ polyLayer =>
        val polys: Seq[Feature[Polygon, Map[String, Value]]] =
          polyLayer
            .polygons
            .filter(f => f.data.get("building").map(v => v == VString("yes")).getOrElse(false))
            .map(f => f.copy(data = f.data.filterKeys(k => !k.contains('軈'))))

        val mpolys: Seq[Feature[MultiPolygon, Map[String, Value]]] =
          polyLayer
            .multiPolygons
            .filter(f => f.data.get("building").map(v => v == VString("yes")).getOrElse(false))
            .map(f => f.copy(data = f.data.filterKeys(k => !k.contains('軈'))))

        val liteLayer: StrictLayer = StrictLayer(
          polyLayer.name,
          polyLayer.tileWidth,
          polyLayer.version,
          polyLayer.tileExtent,
          Seq.empty,  /* Points */
          Seq.empty,  /* MultiPoints */
          Seq.empty,  /* Lines */
          Seq.empty,  /* MultiLines */
          polys,
          mpolys
        )

        VectorTile(Map("buildings" -> liteLayer), v.tileExtent)
      }).getOrElse(VectorTile(Map.empty, v.tileExtent))
    })

    /* Write uncompressed VTs */
//    lighter.saveToHadoop(rawcatalog)({ (k,v) => v.toBytes })

    println("Writing compressed tiles...")

    /* Write a GeoTrellis layer of VTs */
    writer.write(LayerId("north-van-lite", 15), ContextRDD(lighter, manyTiles.metadata), ZCurveKeyIndexMethod)

    /* We're done */
    sc.stop()
    println("Done.")
  }
}
