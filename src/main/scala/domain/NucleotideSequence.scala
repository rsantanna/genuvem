package domain

class NucleotideSequence {
  private var header: String = _
  private var sequence: String = _

  override def toString: String = {
    val subsequence = if (sequence == null) "" else ": " + sequence.substring(0, Math.min(25, sequence.length))
    s"$header $subsequence"
  }
}

object NucleotideSequence {
  def apply(header: String, sequence: String): NucleotideSequence = {
    var n = new NucleotideSequence
    n.header = header
    n.sequence = sequence
    n
  }
}