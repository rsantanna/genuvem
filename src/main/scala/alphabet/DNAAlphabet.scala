package alphabet

import scala.collection.mutable

object DNAAlphabet {

  val size = 4

  val a = 'a'
  val c = 'c'
  val g = 'g'
  val t = 't'

  private val lLetters = Array('a', 'c', 'g', 't').sorted
  private val uLetters = Array('A', 'C', 'G', 'T').sorted

  private val lSpecialLetters = Array('r', 'y', 'k', 'm', 's', 'w', 'b', 'd', 'h', 'v', 'n', 'x').sorted
  private val uSpecialLetters = Array('R', 'Y', 'K', 'M', 'S', 'W', 'B', 'D', 'H', 'V', 'N', 'X').sorted

  private val allLetters = Array('a', 'c', 'g', 't', 'A', 'C', 'G', 'T', 'r', 'y', 'k', 'm', 's',
    'w', 'b', 'd', 'h', 'v', 'n', 'x', 'R', 'Y', 'K', 'M', 'S', 'W', 'B', 'D', 'H', 'V', 'N', 'X').sorted

  private val lLettersBitSet = toBitSet(lLetters)
  private val uLettersBitSet = toBitSet(uLetters)

  private val lSpecialLettersBitSet = toBitSet(lSpecialLetters)
  private val uSpecialLettersBitSet = toBitSet(uSpecialLetters)

  private val allLettersBitSet = toBitSet(allLetters)


  private def toBitSet(arr: Array[Char]): mutable.BitSet = {
    var bitSet = mutable.BitSet.empty
    arr.foreach(c => bitSet += c.toInt)
    bitSet
  }

}
