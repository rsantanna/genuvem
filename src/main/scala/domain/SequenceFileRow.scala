package domain

class SequenceFileRow {
  private var fileId: String = _
  private var lineNumber: Long = _
  private var text: String = _

  override def toString: String = s"$fileId, $lineNumber -> $text"
}

object SequenceFileRow{
  def apply(id : String, position: Long, subsequence: String): SequenceFileRow = {
    val sub = new SequenceFileRow
    sub.fileId = id
    sub.lineNumber = position
    sub.text = subsequence
    sub
  }
}