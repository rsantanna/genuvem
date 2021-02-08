package model

case class StoredSequence(id: Int, typ: String, gi: String, name: String, accession: String, description: String, encodedSequence: Array[Byte])

case class StoredSequenceInfo(id: Int, offset: Int, length: Int)

case class StoredDatabank(subsequenceLength: Int, mask: String, lowComplexityFilter: Int, sequencesQnt: Int, basesQnt: Int, sequencesInfo: Array[StoredSequenceInfo])
