package io.github.olib963.avrocheck

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.Parameters

import scala.util.Try
import scala.collection.JavaConverters._

object CompositeSchemaTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {

  override val schemaFile = "record-with-composites.avsc"

  override def tests =
    Seq(
      test("Schema based generator") {
        // TODO should this just return the composite object anyway in an NRC rather than return a failure?
        forAll(compositeSchemaGen) { schema =>
          that("Because it should reject all composite schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        }
      },
      fieldSuite
    )

  private def fieldSuite = {
    val expectedForSeed1 = new GenericData.Record(schema)
    expectedForSeed1.put("enum", new GenericData.EnumSymbol(schema.getField("enum").schema(), "b"))
    expectedForSeed1.put("fixed", new GenericData.Fixed(
      schema.getField("fixed").schema(),
      Array[Byte](0, 99, 0, 1, 97, 127, 57, 127, 0, 97, 1, 1, 127, 127, -45)))
    expectedForSeed1.put("longArray",
      List(-5539706659784598441L, -1L, 3253015383694729175L, -8349649725000873569L, 1L, -9223372036854775808L, 7086352390877697882L,
        -1025855910169801415L, 5340980081771819891L, -1L, 9223372036854775807L, -9223372036854775808L, -1494231739350021511L,
        -9051056836655791438L, -5832904433934041295L, 2089374971598801494L, -9223372036854775808L, 9223372036854775807L, 9223372036854775807L,
        0L, 0L, 4055968460948124786L, 1L, 0L, 1216524570094546806L, -7647139346699854323L, 1L, -1L, -3873487284825341888L,
        9223372036854775807L, -3238205379105500491L, -7401279930943474020L, 0L, 3709083005474636613L, 1L, 315120932515649650L,
        5501182890336406114L, -1L, -1L, 553492218871735700L, -1L, 0L, -9223372036854775808L, 7052106758320544478L, 0L, -9223372036854775808L,
        3986519995097373636L, 8717583357824354672L, 1L, -9223372036854775808L).asJava)
    expectedForSeed1.put("stringMap",
      Map(
        "⋧䨻ꏮ኉嚳㌻厦ꦴᢴ㳆鿈꾓瘅ᰘ멪癖缑狈ᣐ끍뛠咣ꄍ鸩륃祋ᒥ矁嘶좃붡閤ჿ" -> "鑅盔ࢅΨ欚例掎酦쀳㵒囀姎閛홣죿嵓튦守熀⢚蘫ଙ멿㟂侨ႇꌊ耧黅冰咣ꔍ삝螈⿍恅䳹햩ᡬ居ꦰL쌏푠",
        "ᵺꨌ噚훰픥嵊꠩禪␒큠ꂶ⯘埰侶捭欕纶㺟嘇担ᡧꄼᦄ雪⎳稘㚫૎땪훂ñ륊䫶䥺질蠆촟㗩哹峭܇鮶猵ؗ䱺䊙㭦笮楍৭梁祈繌哝䵲" -> "⽔鎉곡盔妭 ⾃登䎅槄勨伊ઉ଼歐悤숕Ɯ솦ɣҮ梬녷猱⮭﹉扠锕㚑巴羕틁ᚌ엙喝㉃യ磓",
        "딆獄赩ㄗﳫ䫩ꐇ笿襃翗缁岊ꄚԷ縉䗨龼䓁⚯웝䠁뻲譕敓磃덲̤㮔䥚ꆉ힚唛뭃⸣ѵ꘽㫕淋琚诹僩鿜頀奜닠ᘭྩ闉ⱪ쁄犺吖皅" -> "ࡄ惔⛷譀ꖔ쓛矮荱듟닗栰賂駕鉖蕕䐍윶售湏뮧⬦諏ﱰ녵䛟궝뻦撣㶖ጠ㢮묿鶿룕ٷ잘",
        "偘鎮歨거蕼쒹﹐쐁智㛳ℚ饔⧩唜⭚ퟹ꿻펃㾧耥洔⹋꿙翞쿳늴얘ĵ㏤샗詈譟褣㊺祎ᤜ⭖" -> "鋦㊘℀솅껠챸Ἡ漐⋨↡蝷ꆓᜨ彧멕౉䓙ୣ鲆툗瞞"
      ).asJava)

    suite("Generators for records with composite fields",
      test("generating a random record with the appropriate types on each field") {
        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedForSeed1)
      },
      test("generating a random record with implicit overrides") {
        val expectedWithOverrides = new GenericData.Record(expectedForSeed1, true)
        expectedWithOverrides.put("longArray", List[Long](-61, -33, -76, -84, -25, -45, -61, -57, -70, -82, -18, -95, -7, -94, -9, -66, -49, -40, -11, -3, -2, -30, -71, -87, -81, -6, -100, -9, -39, -68, -28, -32, -14, -97, -28, -75, -94, -63, -15, -27, -56, -85, -80, -49, -5, -83, -45, -12, -98, -3).asJava)
        expectedWithOverrides.put("stringMap", Map(
          "lmruumoujmfeiffolfuyqpbjhkpgvdlExrOepuakjqcftkcvbldxbrYlqnccIuuSdoqmxydUwmjlkjqRffblwhvfWxl" -> "rQgetxbuefhxlfskecmkemzotvsuPguotJwnmkxnrspffmjlroxafgqvhtjIyxnuqdbUqevgyfwxubcrhgiezYifmxzclwlwls",
          "qkTvrbabcktHcKirefNm" -> "vIpEucOuxOaxccTztggdxGodgxdeiwaNpmjgjmyseuxiuqBtudslfqYoocyctzeOoiwtCnfoAakigrdrwtcebnoftPdfxAjyfhve",
          "WncH" -> "xzEjnoaqppyyGclvzavszuepwcysoxuixepTzpcmjeribmurpdraVojtkqXoxezjHzbizidojevlvhoWAozxiitrltcnkYmhYo",
          "vte" -> "zcupSfpM",
          "zcbRpIbgWfrxskb" -> "ojvpuzyrdsuhtklozyxrejClPvrbHVsZccrqssmlbepvkp",
          "jUmontjyDplohzrFdsupjnqmhwkuztublqtzwxzugqfTVeuboqmqbijgHg" -> "wodroqbetCtnHqafdokkzzslbwXfvfaobjhkohmevbfraxikcWppdnlnsk",
          "shapgegjetdnsdiwpgfOlktMwguycotfafbRbuva" -> "tijhlnbeqyrhaijBhudoqvvieneyodrgzykb",
          "lxsdcqsopbykjbaaQaogzqkekptjbkfzxgmgizTnnhylwxercdneYtulamqpejy" -> "tdliryhwakwvkkzzxcKqglvutfelQjtyfnttugkucifBbenkRyzghvxvUfymtphlhweNclagnifqtlOr",
          "fslfnlmnuatopqufeyzXuQpVldaqsQphtegpooctkkWqqSsryztjCqnbzix" -> "kauswCupqjxwpxk",
          "qwiiWerkzmxapvfelbauqqblZcbkykkugnztiitzqdcbjlcwvkznhafsbxdnvmmozzipdbmtapzitpduwgbxvwxfqaa" -> "yqjiKoldoUpjxelwDMVkawlsjuFranuwqcplilzniriwDmsmyrofnThYceDpRcoznpThDBzcgbif",
          "aXRbmbWcdhxfjdmodbfrdtgpkorbjuamotjxzxpfcWvcfBywdHiFndngsuwpnfChyogSUkyhjarIxRwhGrxvaMstm" -> "bkbprerfHcGikxylfzvtovpkchsbgrGotmpbMwjpCdiwPgvAvObqmDhaglq",
          "akuysJiRqrAnpaghtwYmu" -> "grfteEuteyvyjhzxzxnmlpujyaqhcucebgbbrdnanqqxoJmbomahsfnfwfdaxgripPhZLhUiuqoysd",
          "upnpopnuimmccWaupsfeMsrawcmurqgfbntkwchcadKafjlzxPcwodwdfhaupjnhp" -> "OnPhrzczTilq",
          "vuqglnmapunfDjkQzmjnywnfiXAxrjyqctchqsFnzRyvxrzzYbbOegjnvsmyahflajfl" -> "rwlugpepmvtsugixiwdyctvjgdpgbwywhhfaghqaigIfiwzgzhgm",
          "xjgXrwlnoRamqdikhueryajttpkwcfsukdwSxeddo" -> "haenhjsjpbjlicnlcvcpphecwedjwtaokzhvbmymzpzntakgzziswxUnuqafrbSvcjrnUye",
          "xsuHeerkofkl" -> "JgosneOeIwibweqifyElgnihxVblkyaxbrcerhvnsdzbAptqRpsbnmUxmoejoiuGiqseufbkuqrfhjffzbvxmxguo",
          "oedbKktyimzYfhlslifnh" -> "ogjcqahintRzmnrlmykurdqsdhYmTnanAkapgfIxmawjrktBvSsethqcanfxXeuymiyafrliiaetaoyju",
          "eofmDCjffiskiaiwGtsjq" -> "cqpetdnrvdpctfkwpkkbhzigqa",
          "piYJnihldiqxfsjkhwpkahxsyrkcmafyynyhdtvbwpreyjglQczonqvqzckewyupescoqrJmtms" -> "CuuisyueDocplwbgdXnjodyGxSvsBhejwFfThVtb",
          "pmbluvbwQwqlequQaguedklxgekedXpdbgrpxlwqdgPnfprdbwxBlhHDzlwdjocjbnunrqqevfvtavemkatxoekvveomwjv" -> "tnyQbnyibh",
          "WqwvsygwltldVbgmxjebottoredlorervpdzywejgoooihmznvporwutgzwWndim" -> "wcrsrmhyHVeBQsawplJajvmdnkukxhxjvatdoszasekwvjaymrtmydnytg",
          "eugloxdInbbyljzanzkhzMylxNjzvsuggjfmsqfrkjhqsxrZJxosnhxyw" -> "zjeqsyjggvmgjlcmcqtBoosbuhChregjjbdzgltpdcjhqkwuruEoqfDVDaxPnptodncymwayj",
          "kmbgvMgzqoyfqsehgptpxzzoylwhhQockbcGqovklluvsmlZkem" -> "RywlcmjwnpdtJdphmigwdegaVrwmpMpibwpIwlhnaCdbtuacmZwpkscovkpvetskfm",
          "khblfkLmbrjVktqurrbwSvdiAohjdawjvgnscozcqluczbfztvkcocemiljvxpgqykVmCgndvsdziulgzxsruiTwcgacspRupu" -> "hydsQEilahcGgrzzttvfsoGMmoyEgdrwxswxdiwzufmhmADwkohazrxtnxjtcdpoLvaeUjrnoyvbgxyjfprmVgoxdcqh",
          "pedjSregtlufhskervibxupwhqgreozpdSgyzfrMzognkxqdzfxfsSfwvqgqjgtdxryYUmuKfcJohukpzulcvtroyJrbop" -> "pjgxhueunuUxchuzmnBykjcyqodevvglmkifYirhiuBpfwiTwx",
          "vjfwoaxjejsdxjpo" -> "lrqigpeXukqwvjreUjxodn",
          "vjrnblk" -> "doqstmmpjavgoDtihzabffpugqgwsyuzyhkxrywqgoregtejrmlvbvpcjWJdcbzppmubylvmhgmxyxdvNiyrBpufxhkbrvwutubv",
          "kqplgxzcddiykz" -> "kETfiuxyqldcNgsbsvyxnweJakEyyvhhobcxf",
          "bkvlrpezhObydisgjjhaCiyilkgvqynLywZbdmiihkhfepqvwzbMagszaaylnhnywertnbmeTmhvuynuZjDbxMqfdwrtxmmzmj" -> "jaefznmpmudjayNrzozxrtiliypnxuzgpjqCrKffrhdbuyfSgoavjaleksonzzvqfzkkrogwzoShopiqXiidahpriLXvyF",
          "ZnclkrycqrxszhynlfGemlldycaaoholkHenaeeuSqhgfcclcsxAfohvpscfgntoApNrLkysrycl" -> "nguqomrYfv",
          "jvrbhbznnhyemrfaaotBJke" -> "swtrtkylPyGtvntidGuosmohbtPztzhoarhgjaljxfzqTiuIcnwyigMehfbrxztwHkmkGtm",
          "rjgqmuzTunitadhgjrhyhyqibmygigihPOiwgfcfuyXrWfcraqkcU" -> "ajxevnyidphzeziScewmztqugvteaijanenydKxvhyejlqoLpqJHahrgdzPQgeQpaofiGbdoxvqcrxaeqkznolsdjyibpsxz",
          "wyxzwbkfwomyFhhubochjdLfvdkgBjhhiioarrytemapnfiqneOcomoHptxljsgfvmgdNaaHcwggrquQliuhissdxdj" -> "oocrthottnzkcpcvaKOjchslbtrnOByQyjxypdwiyustklselwsSbmhyfyrpohcncymlsqgygsbaCgaVi",
          "cvsbubkeyogyrmanlkbdnnnqmcsdzGeawouvroolkjawiqgmJa" -> "XogEkgccuprQbWegsjbwbncdfOxuyfhhuhgfufkgRacvkcpwrpknxSFsozawydmcsqctavdnnSBuvrmoqnOHaukYk",
          "exoitezjMzcYqkrtptkkaLyhdypwujd" -> "uiqwaAGtmvutuxyoqDfzXvItnbdmqBsGkiicchcup",
          "bshlvrhdsg" -> "ozsdLoytdvdxwmdmufrpaBouevrOb",
          "tFfpxmUkezsbgobwswbijzwanshqhu" -> "lrictYfntXpboUthycaofEnhepfinbrijtszxgnurkdqenBaxVXsB",
          "ukiuwzwbukzwvgqcfqmvxzexzisdqymllieecuwhxbexqpdqudlQodhmaghKkRxiddcepiorZqk" -> "gvblmcYlttenxobiGuRgtyqnmxsrIeuTtvk",
          "qBqcatausFuxbOjlgxga" -> "IQuiwvyotjfjzirajbsxdqSazr",
          "hvxostqhrhyukn" -> "xMvlmbghajLqemksdrbfhosYelvhoivntkrrpoiucpdwsIzazsgWlozebsYjnlfdamqLedhvAgwozAwxskolNlnukGjqo",
          "bmaydniiNjosqsIaqe" -> "guovgtoeoRvmmsyevnebbuLzRwboledmcnvdcqjokreuIzqalsZsmqsdpowsejEbyxutitifhsdejsxfwcxdvthz",
          "gfRlrzmPvlsfelbrsoobhdcrnknljegaspJxpfqmptvmaqjrtkaplzuhofpphldyzfyejlinuoiwkmaykpbUbvzMk" -> "pfmvtnnyyngqoqeuwsqrgnRascvqmkcdnJklciacf",
          "qqtqaplpgtlinebuMcdipzclukjysaefgdaykprtGzwfaouzehc" -> "idpitdvmVyoinPufjfkfwVmbYvesdMezCyxovfwbqLupvtxfdavnwxunkhJrosmOpfxso",
          "tixfxckggzjChatixhssllcdvkqzgkfckv" -> "gtmfpkCGotpd",
          "wjnfcllufrgblKblmMzHcFbvoENJpipvsdazsawiylhmk" -> "hbgeteiayilzfaldzUlqugujCevVuudehgfdpmYozMKfulkwfqcVrwluoBqdwimiaqooaIwLazfrqjzbaviJgTtcniElXwrqaph",
          "lgfjpqefzheftbzrtBsncwvivxTagtbuQdtShjKdelwtnwvkoseczrhLluQfbinpxvxwjutepu" -> "xXnQKWuMxdtzeydmcdomuuakeUdoxhaaewgeyqssscFyqjPossmziybFErlxloks",
          "kJqbriiHobgzltKpnygWghbsjitqovddonKpqcreqerfyhxczxvHqpeo" -> "ouazprikaudfjsjtschdfdYzpmjyBsdxdnckokaubzpdlahxvnwxgocFaucmf",
          "pkvtesrkJvmuoa" -> "qkzimkrqHl",
          "mjvDaobdvOwhiojsyezpNukQvztbwgfoigqzhnvsmyjtyhSeqwpkaevoqhdbadvjmytnwbdGxaugfutqOcbogkCatFpwh" -> "Xzzzb",
          "yFtfapkFujrbfq" -> "kcgfabtdwDstuluorwmeqahJpqgemxbi"
        ).asJava)

        // Changes the Map[String, String] and List[Long] generators
        implicit val alphaString: Arbitrary[String] = Arbitrary(Gen.alphaStr)
        implicit val negativeLongs: Arbitrary[Long] = Arbitrary(Gen.negNum[Long])

        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedWithOverrides)
      },
      suite("Invalid Overrides",
        test("should not allow explicit enum symbol overrides") {
          val enumSymbol = new GenericData.EnumSymbol(schema.getField("enum").schema(), "b")
          implicit val overrides: Overrides = overrideKeys("enum" -> enumSymbol)
          that(Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        },
        test("should not allow you to select an enum that doesn't exist") {
          implicit val overrides: Overrides = overrideKeys("enum" -> "d")
          that(Try(genFromSchema(schema)) , isFailure[Gen[GenericRecord]])
        },
        test("should not allow a fixed override if the byte array is of the wrong size") {
          val smallAssertion = {
            // Too small
            implicit val overrides: Overrides = overrideKeys("fixed" -> Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
            that(Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
          }
          val bigAssertion = {
            // Too big
            implicit val overrides: Overrides = overrideKeys("fixed" -> Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16))
            that(Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
          }
          smallAssertion.and(bigAssertion)
        },
      ),
      suite("Valid Overrides",
        test("should let you select a specific enum") {
          val enumValue = "c"
          implicit val overrides: Overrides = overrideKeys("enum" -> enumValue)
          val gen = genFromSchema(schema)
          val generated = gen(Parameters.default, firstSeed).get
          that(generated.get("enum"), isEqualTo[AnyRef](new GenericData.EnumSymbol(schema.getField("enum").schema(), enumValue)))
        },
        test("should let you override a fixed byte array with the correct size") {
          val bytes = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
          implicit val overrides: Overrides = overrideKeys("fixed" -> bytes)
          val gen = genFromSchema(schema)
          val generated = gen(Parameters.default, firstSeed).get
          that(generated.get("fixed"), isEqualTo[AnyRef](new GenericData.Fixed(schema.getField("fixed").schema(), bytes)))
        }
      )
    )
  }

}
