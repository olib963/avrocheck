package io.github.olib963.avrocheck

import org.apache.avro.generic.GenericData
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.Parameters
import org.scalatest.prop.PropertyChecks

import scala.util.Try

class RecordWithUnionSpec extends SchemaGeneratorSpec with PropertyChecks {
  override val schemaFile: String = "record-with-unions.avsc"

  describe("Generators for record with unions of primitives"){
    val expectedForSeed1 = new GenericData.Record(schema)
    expectedForSeed1.put("nullableInt", null)
    expectedForSeed1.put("stringOrLong", "䬒갛햗鸹쳤⹡ᕫ雓ꏻꂼ濗榟秆齚⭳풺亮䱇Ⅽ둲䉄彶瘍⩠偀閣훫ꫲ썅髆짹젊틥ﮎ赂ዞ㯄ᥰ뚀ﭙ⋧䨻ꏮ኉嚳㌻厦ꦴᢴ㳆鿈꾓瘅ᰘ멪癖缑狈ᣐ끍뛠咣ꄍ鸩륃祋ᒥ矁嘶좃붡閤ჿ㸈䓾䮨霡㖐菾뀸분㌵ꆸᙣ蹙랑퟿㛉஘굃ﯾ")

    describe("generating a random record") {
      val gen = genFromSchema(schema)
      recordsShouldMatch(gen(Parameters.default, firstSeed), expectedForSeed1)
    }

    describe("generating a random record for next seed") {
      val gen = genFromSchema(schema)
      val expectedForSeed2 = new GenericData.Record(schema)
      expectedForSeed2.put("nullableInt", 2093140340)
      expectedForSeed2.put("stringOrLong", "㧁ම㳣群尹つ沨攺酻⎻忬契⼳쎒ඞ櫯‎鼰놊팿ᒲ")

      recordsShouldMatch(gen(Parameters.default, secondSeed), expectedForSeed2)
    }
    describe("generating a random record with implicit overrides") {
      val expectedWithOverrides = new GenericData.Record(expectedForSeed1, true)
      expectedWithOverrides.put("nullableInt", null)
      expectedWithOverrides.put("stringOrLong", "}^#HyNhXb\"*sZ<b]>2U8c`lu\\LiC#\"u-0gevpj1A*$2t`uD!|Jd&sTWg'HF`% SCw%P;6s(_4o`yBWk~hC^(JR,:ir}fOXuuXxK")

      implicit val printableStrings: Arbitrary[String] = Arbitrary(Gen.asciiPrintableStr)

      val gen = genFromSchema(schema)
      recordsShouldMatch(gen(Parameters.default, firstSeed), expectedWithOverrides)
    }
    describe("Invalid overrides") {
      it("should not let you select a type that doesn't exist in the union") {
        {
          implicit val overrides: Overrides = overrideKeys("nullableInt" -> "hello")
          Try(genFromSchema(schema)) should be('failure)
        }
        {
          implicit val overrides: Overrides = overrideKeys("stringOrLong" -> false)
          Try(genFromSchema(schema)) should be('failure)
        }
      }
    }
    describe("Valid overrides") {
      describe("selecting a value for one branch") {
        implicit val overrides: Overrides = overrideKeys("nullableInt" -> 12, "stringOrLong" -> 123L)

        val expectedUnionSelected = new GenericData.Record(schema)
        expectedUnionSelected.put("nullableInt", 12)
        expectedUnionSelected.put("stringOrLong", 123L)

        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedUnionSelected)
      }
    }
  }
}
