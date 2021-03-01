package encoder

import org.apache.hadoop.fs.{FileSystem, Path}

class DNAEncoder(subsequenceLength: Int) extends Serializable {
  private val DNABitsToSymbolSubstitutionTable: Array[Char] = Array[Char]('A', 'C', 'G', 'T')
  private val bitsByAlphabetSize = 2 // log10(DNABitsToSymbolSubstitutionTable.length)/log10(2.0)
  private val bitsMask = (1 << bitsByAlphabetSize) - 1 // 3 0x0000000000000011

  private def getBitsFromChar(symbol: Char): Int = symbol match {
    case 'A' | 'a' => 0
    case 'C' | 'c' => 1
    case 'G' | 'g' => 2
    case 'T' | 't' => 3
    case _ => 0
  }

  def encodeSubsequenceToInteger(subSymbolList: String): Int = {
    var encoded: Int = 0
    for (i <- 1 to subSymbolList.length) {
      encoded = encoded | (getBitsFromChar(subSymbolList.charAt(i - 1)) << ((subsequenceLength - i) * bitsByAlphabetSize))
    }
    encoded
  }

  def decodeIntegerToString(encoded: Int, stringLength: Int = subsequenceLength): String = {
    val sb = new StringBuilder(stringLength)
    for (pos <- 0 until stringLength) {
      val posInInt = subsequenceLength - pos
      val shift = posInInt * bitsByAlphabetSize
      val value = encoded >> (shift - bitsByAlphabetSize)
      sb.append(DNABitsToSymbolSubstitutionTable((value & bitsMask).toInt))
    }
    sb.toString
  }

}

object DNAEncoder {
  def apply(subsequenceLength: Int): DNAEncoder = new DNAEncoder(subsequenceLength)

  def main(args: Array[String]): Unit = {
    val encoder = DNAEncoder(11)

    val sequence = "AGCTTTTCATT"
    val encoded = encoder.encodeSubsequenceToInteger(sequence)
    val decoded = encoder.decodeIntegerToString(encoded)

    println(s"Sequence: $sequence\nEncoded: $encoded\nDecoded: $decoded")
  }
}