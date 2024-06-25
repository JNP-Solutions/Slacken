package com.jnpersson.slacken

import com.jnpersson.discount
import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.AnyMinSplitter
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{classify, getTaxonLabels}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.BitSet


object TaxonFragment {

  //  def fromSeqTaxon(fragment: InputFragment, taxon: Taxon) =
  //    TaxonFragment(taxon, fragment.nucleotides, fragment.header + fragment.location)


}

final case class TaxonFragment(taxon: Taxon, nucleotides: NTSeq, id: String) {

  // returns all distinct minimizers in the nucleotide sequence
  def distinctMinimizers(splitter: AnyMinSplitter)=
    splitter.superkmerPositions(nucleotides, addRC = false).map(_._2).toArray.distinct.iterator.map(_.data)

  def generateReads(seq: NTSeq, readLen: Int): Iterator[NTSeq] = {

    for {
      i <- Iterator.range(0, seq.length - readLen + 1)
      read = seq.substring(i, i + readLen)
    } yield read

  }
  /**
   * Generate reads from the fragment then classify them according to the lca's.
   * @param minimizers
   * @param lcas
   * @return
   */
  def readClassifications(minimizers:Array[Array[Long]], lcas: Array[Taxon], readLen: Int):Iterator[(Taxon,Long)] = {

    // generate reads from fragment: Get taxon genome -->
    // Break genome up into read fragments and convert to Dataset[InputFragment] -->
    // pass to classify with the minimizers
    val reads = generateReads(nucleotides, readLen)
    ???
  }

}

class BrackenWeights(keyValueIndex: KeyValueIndex)(implicit val spark: SparkSession) {

  import spark.sqlContext.implicits._

  def brackenReport = ???

  def buildWeights(library: GenomeLibrary, taxa: BitSet) = {

    val titlesTaxa = getTaxonLabels(library.labelFile).toDF("header", "taxon")
    val idSeqDF = library.inputs.getInputFragments(withRC = false)
    val fragments = idSeqDF.join(titlesTaxa, idSeqDF("header") === titlesTaxa("header")).
      select("taxon", "nucleotides", "location", "header").as[(Taxon, NTSeq, SeqLocation, SeqTitle)].
      map(x => TaxonFragment(x._1, x._2, x._4 + x._3))

  }
}

