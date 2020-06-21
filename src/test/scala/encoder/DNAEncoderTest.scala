package encoder

import org.scalatest.funsuite.AnyFunSuite

class DNAEncoderTest extends AnyFunSuite {

  test("Encode DNA Subsequence") {
    val encoder = DNAEncoder(8)

    assert(encoder.encodeSubsequenceToInteger("TCGGACTG") === 55838)
    assert(encoder.encodeSubsequenceToInteger("AACAACAA") === 1040)
    assert(encoder.encodeSubsequenceToInteger("CCCCCCCC") === 21845)
    assert(encoder.encodeSubsequenceToInteger("AAAAAAAA") === 0)
    assert(encoder.encodeSubsequenceToInteger("TTTTTTTT") === 65535)
    assert(encoder.encodeSubsequenceToInteger("ACTGGTCA") === 7860)
    assert(encoder.encodeSubsequenceToInteger("ATTTTTTT") === 16383)
    assert(encoder.encodeSubsequenceToInteger("TCTAGCCA") === 56468)
  }

  test("Decode DNA Subsequence") {
    val encoder = DNAEncoder(8)

    assert("TCGGACTG" === encoder.decodeIntegerToString(55838))
    assert("AACAACAA" === encoder.decodeIntegerToString(1040))
    assert("CCCCCCCC" === encoder.decodeIntegerToString(21845))
    assert("AAAAAAAA" === encoder.decodeIntegerToString(0))
    assert("TTTTTTTT" === encoder.decodeIntegerToString(65535))
    assert("ACTGGTCA" === encoder.decodeIntegerToString(7860))
    assert("ATTTTTTT" === encoder.decodeIntegerToString(16383))
    assert("TCTAGCCA" === encoder.decodeIntegerToString(56468))
  }
}
