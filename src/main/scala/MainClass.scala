


import java.lang.IllegalStateException

import DataTransform._
import FeatureExtraction.{ArtistsFrequency, GenreFrequency, ReadabilityScore, RhymeScheme, Utility}
import MachineLearning.Word2Vectorizer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression

import scala.util.Try

object MainClass {
  val lyricsModelDirectoryPath = "/tmp/spark-logistic-regression-model"

  /*
  Id    Genre
  0.0 - Pop
  1.0 - Rock
  2.0 - Electronic
  3.0 - Hip-Hop
  4.0 - Metal
  5.0 - Jazz
  6.0 - Folk
  7.0 - NA
  8.0 - RB
  9.0 - Other
  10.0 - Country
  11.0 - Indie
   */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GenrePredictorFromLyrics")
      .master("local")
      .getOrCreate()


    val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"


   // train(spark)
   // predict(spark, unknownlyrics)

    testtrain(spark)
    testpredict(spark, unknownlyrics)
    //predict(spark, unknownlyrics)


  }


  def train(spark: SparkSession) ={
    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\subset.csv")// Give correct path here.
    //Rohan's path :E:\C drive\NEU\Scala\Final\datasets\kaggle\


    val cleanedData = DataCleaner.cleanTrain(df)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("clean_lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")






    val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)


    val genresFrequency =GenreFrequency.filterByGenreFrequency(swRemovedWordTokenizer)

    artistsFrequency.show(false)
    genresFrequency.show(false)


  //  val artistsGroup = ArtistsFrequency.groupLyrics(swRemovedWordTokenizer)
  //  val genresGroup = GenreFrequency.groupLyrics(swRemovedWordTokenizer)


    //artistsGroup.
   // GenreFrequency.addGenre(genresGroup)


    //val word2VecModelEntire = Word2Vectorizer.vectorize(swRemovedWordTokenizer)

 //   val word2VecModelGenres = Word2Vectorizer.vectorizeGenres(genresGroup)
   // val word2VecModelArtists = Word2Vectorizer.vectorizeArtists(artistsGroup)

  //  Word2Vectorizer.saveArtists(word2VecModelArtists)
  //  Word2Vectorizer.saveGenres(word2VecModelGenres)

   //
   // Word2Vectorizer.cosineArtists("")

 //   println("saved")


    // val w2vp = new W2VPipeline(spark).train()



    /*

    val cleanedData = DataCleaner.clean(df)
    val wordTokenizer = WordTokenizer.tokenize(cleanedData, "tokenized_words")
   // wordTokenizer.show(10, false)
    val nonStpWordData = SWRemover.removeStopWords(wordTokenizer.where(wordTokenizer("lyrics").isNotNull))
    val swRemovedWordTokenizer = WordTokenizer.tokenize(nonStpWordData, "words")

    val word2VecModel = Word2Vectorizer.vectorize(swRemovedWordTokenizer)


    word2VecModel.findSynonyms("cold", 1).show(false)
 //   val artistsFrequency = ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
 //   val artistsGroupedLyrics = ArtistsFrequency.groupLyrics(swRemovedWordTokenizer)
  //  val genreGroupedLyrics = GenreFrequency.groupLyrics(swRemovedWordTokenizer)


  //  artistsGroupedLyrics.show(false)
  //  genreGroupedLyrics.show(false)



    //val lyrics_ds = spark.createDataset(lyrics)





    //val model = new CrossValidatorModel()


  //  val crossValidatorModel = new CrossValidatorModel().classify()


    //val result = word2VecModel.transform(documentDF)
    //result.show(false)






  /*
    val documentDF = spark.createDataFrame(Seq(
      random.split(" ")

    ).map(Tuple1.apply)).toDF("tokenized_words")

    val result = word2VecModel.transform(documentDF)
    result.select("features").show(false)
  */

    //val artistsFrequency =ArtistsFrequency.filterByArtistFrequency(swRemovedWordTokenizer)
   // val genresFrequency =GenreFrequency.filterByGenreFrequency(swRemovedWordTokenizer)
   // genresFrequency.show(false)



  */
  }



  def dfToRDD(df: DataFrame): Unit = {
    val rootRDD = df.rdd.map( row =>
      ( Try(row.getString(0).toInt),
        Try(row.getString(1)),
        Try(row.getString(2).toInt),
        Try(row.getString(3)),
        Try(row.getString(4)),
        Try(row.getString(5)) ))

    // transform with Readability Score feature
    val transformRDD1 = (rootRDD zip ReadabilityScore.transformWithScore(rootRDD)) map (Utility.flattenNestedTuple6)

    // transform with Rhyme Scheme feature
    val transformRDD2 = (transformRDD1 zip RhymeScheme.transformWithRhymeScheme(rootRDD)) map (Utility.flattenNestedTuple7)
  }




  def predict(spark: SparkSession, unknownlyrics: String) = {
    val splitSentences = unknownlyrics.split("\\r?\\n{1,}")
    val splitWords = unknownlyrics.split("\\s+")

    val word2VecModelGenres = Word2Vectorizer.loadGenres()
    val word2VecModelArtists = Word2Vectorizer.loadArtists()

    def Sentences =
      for {
        sentence <- splitSentences
      } yield (println(sentence))



    val cleanedData = DataCleaner.cleanPredict(spark.createDataFrame(Seq(
      (0, unknownlyrics)
    )).toDF("id", "lyrics"))

    cleanedData.show(false)

    for(word <- splitWords) {
      println("-------------------------------------------------------------------------")
      println(word)
      try {
        word2VecModelGenres.findSynonyms(word.toLowerCase, 1).show(false)
      }catch {
        case e: IllegalStateException => print("")
      }
      println("-------------------------------------------------------------------------")
    }



/*
      for(words <- splitWords) {
        println(words)
        println("testing breaker!!")
      }

*/


    //println(split)


  //  val word2VecModelGenres = Word2Vectorizer.loadGenres()
  //  val word2VecModelArtists = Word2Vectorizer.loadArtists()


    //word2VecModelGenres.findSynonyms(split, 1).show(false)
    //word2VecModelArtists.findSynonyms(split, 1).show(false)
  }



  def testtrain(spark:SparkSession) = {

    /*
    val training = spark.createDataFrame(Seq(
      (0L, "Oh baby, how you doing? You know I'm gonna cut right to the chase Some women were made but me, myself I like to think that I was created for a special purpose You know, what's more special than you? You feel me It's on baby, let's get lost You don't need to call into work 'cause you're the boss For real, want you to show me how you feel I consider myself lucky, that's a big deal Why? Well, you got the key to my heart But you ain't gonna need it, I'd rather you open up my body And show me secrets, you didn't know was inside No need for me to lie It's too big, it's too wide It's too strong, it won't fit It's too much, it's too tough He talk like this 'cause he can back it up He got a big ego, such a huge ego I love his big ego, it's too much He walk like this 'cause he can back it up Usually I'm humble, right now I don't choose You can leave with me or you could have the blues Some call it arrogant, I call it confident You decide when you find on what I'm working with Damn I know I'm killing you with them legs Better yet them thighs Matter a fact it's my smile or maybe my eyes Boy you a site to see, kind of something like me It's too big, it's too wide It's too strong, it won't fit It's too much, it's too tough I talk like this 'cause I can back it up I got a big ego, such a huge ego But he love my big ego, it's too much I walk like this 'cause I can back it up I, I walk like this 'cause I can back it up I, I talk like this 'cause I can back it up I, I can back it up, I can back it up I walk like this 'cause I can back it up It's too big, it's too wide It's too strong, it won't fit It's too much, it's too tough He talk like this 'cause he can back it up He got a big ego, such a huge ego, such a huge ego I love his big ego, it's too much He walk like this 'cause he can back it up Ego so big, you must admit I got every reason to feel like I'm that bitch Ego so strong, if you ain't know I don't need no beat, I can sing it with piano", 1.0),
      (1L, "playin' everything so easy, it's like you seem so sure. still your ways, you dont see i'm not sure if they're for me. then things come right along our way, though  we didn't truly ask. it seems as if they're gonna linger with every delight they ", 1.0),
      (2L, "If you search For tenderness It isn't hard to find You can have the love You need to live But if you look For truthfulness You might just As well be blind It always seems to be So hard to give Chorus: Honesty Is such a lonely word Everyone is so ", 1.0),
      (3L, "Oh oh oh I, oh oh oh I [Verse 1:] If I wrote a book about where we stand Then the title of my book would be \"Life with Superman\" That's how you make me feel I count you as a privilege This love is so ideal I'm honored to be in it I know you ", 1.0),
      (4L, "Party the people, the people the party it's popping no sitting around, I see you looking you looking I see you I look at what you started now Let's hook up little one on one up come on give some of the that stuff, Let me freshin' it with no ruff (let's go) Let's hook up when we start we won't stop, Baby, baby don't stop come on give me some of that stuff (let's go) I am, Black Culture I am, Black Culture I am, Black Culture I am, Black Culture Party the people, the people the party, it's popping no sitting around, I see you looking you looking I see you I look at what you started now Let's hook up come one give some of the that stuff, Let me freshin' it up little one on one with no ruff (let's go) Let's hook up, when we start we won't stop, Baby, baby don't stop come on give me some of that stuff (let's go) You are, Black Culture You are, Black Culture You are, Black Culture You are, Black Culture Let's hook up come on give some of the that stuff, Let me freshin' it up little one on one with no ruff (let's go) Let's hook up, when we start we won't stop, Baby, baby don't stop come on give me some of that stuff (let's go) Let's hook up come on give some of the that stuff, Let me freshin' it up little one on one with no ruff (let's go) Let's hook up, when we start we won't stop, Baby, baby don't stop come on give me some of that stuff (let's go) We are, Black Culture We are, Black Culture We are, Black Culture We are, Black Culture We are, Black Culture We are, Black Culture We are, Black Culture We are, Black Culture", 1.0),

      (5L, "Where should I begin cripplin' all you villains in Never injuring the civilians that are innocent So sick of listening to a bunch of synonyms Same band every man but aint no need to mention them You actors had me crackin' up with laughter Now you gonna have to hear another cracker rapper Still hearing all of these people callin' me a wannabe But I'm gonna keep it going 'cuz you heard them all from me Chorus You can cover your ears, but the noise won't stop, Your still gonna hear the white trash hip-rock. You can cover your ears, but the noise won't stop, You know your still gonna hear the white trash hip-rock, You can cover your ears, but the noise won't stop! See everything's a challenge when you're chemically imbalanced So I thought a single soul would never see my talent Y'all figured by now that I would throw the towel in Tellin' me repeatedly the genre's overcrowded But some of these cats are done with the past And I wonder if they ever had a love for the craft No you'll never see my turntablist in the back, pretending to scratch Cuz anyone who did it is wack. Chorus To all the Blinks, Eddie Vedders and the Kurt Cobains It's a shame you impersonators sound the same You can't resurrect a legend and emulate the veteran Man I would love to level the devil who let them in Instead of breaking out a switchblade and carving it in I'll address the enemy and stab his heart with a pen You can't wait for this hip rock style to end I'm sorry my friend, this shit has just started again! Chorus 2x", 2.0),
      (6L, "Enough of all that, let's switch up the format, And talk some more trash, I'm getting bored fast Ain't gonna talk gats, just hand-to-hand combat, Man-to-man, full contact, on the floor mat, Your weak blows are deflectable, I'll go toe-to-toe with you, at your own dojo, Want none of you bozos at my show, where some silly hoe's shakin' her ass up in my video I didn't kick flows in the cold until my lips froze, So folk can always tell me how my shit goes I grab a pencil like it's a pistol But this way it isn't problem if it hits you You won't see what you want from me, So don't come to me well \"what's it all mean?\" cause' It don't mean a thing Your lies is all I despise, Been fightin' guys like you all my life and They don't mean a thing. My best shit ain't even on my set list, Cus half the listeners won't even get this, So what! you sold records, you ain't respected, You went too far and tried too hard to be accepted, This can never be a thing of jealousy, Just don't be tellin' me to check the melody, I got the better beats, got the better rhymes, I made sure I bettered mine ahead of time So what's up kid? Your big head's been busted, Don't get disgusted, I'm just servin' justice. You won't see what you want from me, So don't come to me well \"what's it all mean?\" cause' It don't mean a thing Your lies is all I despise, Been fightin' guys like you all my life and They don't mean a thing. So don't attempt, to beg for my pardon, Cause' I'll keep talkin' til' my arteries harden, I've takin' losses, told off some bosses, Sick of all them jobs that all make me nauseous, It ain't worth it, workin' for no purpose, For cash I'm hurtin', future's uncertain, So I keep searching, livin' and learnin', Earnest to earn everything I'm deservin' If I can't be on the stage with my band jammin', I'll be standin' on the street corner panhandlin'! You won't see what you want from me, So don't come to me well \"what's it all mean?\" cause' It don't mean a thing Your lies is all I despise, Been fightin' guys like you all my life and They don't mean a thing. You won't see what you want from me, So don't come to me well \"what's it all mean?\" cause' It don't mean a thing Your lies is all I despise, Been fightin' guys like you all my life and They don't mean a thing.", 2.0),
      (7L, "They say the end's near But I'll approach it with no fear When the smoke clears I'll find my spirit is still here May take years to find it the prophets are real Too much damage to inhabit the planet, that's severe I talk shit you heartless paupers can't feel With concepts the smartest robber can't steal Your fears will cause all ya'll to pour tears Cause the bomb that I'm droppin' is landing, so stand clear Are there any queers here in the theater tonight? We'll get 'em up against the wall and stick 'em with dynamite Cause there's something going on in my head that ain't right But I'm getting ready to take flight, so hang tight It don't matter if you're standing tall If you're thinking small, we all rise and fall Don't gonna try to abide by the law No one alive is surviving the holocaust It don't matter if you're standing tall If your thinking small, we all rise and fall Don't gonna try to abide by the law No one alive is surviving the holocaust Fuck partying like it's 1999 Ain't watching the ball fall, ain't calculating time Watching polar caps melt, and the coastline rise See the ocean divide, and nine planets align Sun shining too bright, igniting the Alpines With petrified eyes were watching at ringside Can't run, you can't hide, panicking worldwide When straight out the sky, falls great balls of fire I'll sneak on the spot where the shuttle is launched Strap me and a broad onto a rocket and blast off We'll be following the way of the Force like Star Wars And I'll start a new fam' when I land on Mars It don't matter if you're standing tall If you're thinking small, we all rise and fall Don't gonna try to abide by the law No one alive is surviving the holocaust It don't matter if you're standing tall If you're thinking small, we all rise and fall Don't gonna try to abide by the law No one alive is surviving the holocaust Now when the end comes, and your time here is done Will you look back and say, overall, it was all fun? Well life's been good to me so far like Joe Walsh But it won't break my heart if it all falls apart It don't matter if you're standing tall If you're thinking small, we all rise and fall Don't gonna try to abide by the law No one alive is surviving the holocaust", 2.0),
      (8L, "Listen son I'm gonna tell you a story About how your grandfather died He got eaten up by a psycho lorry Out on interstate five Roadkills But we'll get back at them for what they've done You just wait and see We'll build and army and destroy them Before they get you and me ", 2.0),

      (9L, "Gods Of The Mountains Sky, Forest And Seas Lands Of Fire, Ice And The Northern Deeps Cold As A Storm From The Raging Sea Soon Their Winds To Rise Gods Of The Mountains Sky, Forest And Seas Lands Of Fire, Ice And The Northern Deeps Cold As A Storm From The Raging Sea Soon Their Winds To Rise", 0.0),
      (10L, "Rise gods of the fierous black burning skies Raise hammers spears and swords high on your ride Along the great legends born of fire and ice Warriors on horses attack from the mountainous sides Fires shall flame to the skies all battalions stride Alliances stand line by line Hordes of men armoured to die Fight warriors of norse while death glances in your eyes Swing axes and blades unto the final ride Attrition holds forces under the great fires Now heathenous shall be the victors with swords forged by fire A warcry shall echo the rise Loud from the fierce mountainside All set their lives on the line To death which is for you all to sign Ride Under The Fire Under The Great Fires Man Of The Mountains Ride Under The Fire Under The Great Fires No Man Of The Mountains Ride Under The Fire Under The Great Fires Man Of The Mountains Ride Under The Fire Under The Great Fires Man Of The Mountains Ride with the wind in your eyes break through the enemies lines Dark is the power we rise here under the great fires Victory stands on this days bring to all warriors their fate Ride our enemies down bring them all to the ground March under the gods of our fathers unite March under the flag of the norse \"Oden", 0.0),
      (11L, "A Dying Skyline Cold From Wind And Rain Autumn Of Darkness End In Fire And Flames North Blows The Wind And My Sight Is Afar Along The Coming Of The Northern Stars Fires Burnt Under These Skies Here Ancient Norsemen Once Brought Us To Pride A Heathen Force Is To Rise On These Days The Sun Still Fanding Soon Clouds Of Black Rage We're All Blackened Skies We're All Blackened Skies We're Under All Blackened Skies The Heir Of Gods To Arise We're Under All Blckened Skies The Heir Of Our Ancient Pride We're All Blackened Skies We're All Blackened Skies We're All Blackened Skies We're All Blackened Skies", 0.0),
      (12L, "Fiends of the gods to war we ride Over the black mountain far over ice Fields of war before us now ride by my side Ride by my side fire Warriors of the north hold your swords high Ride forth the stallions black all free under sky You", 0.0),
      (13L, "Verse One Waiting all your life To talk about (talk about) the birds and the bees, yeah Waiting all your life To do the things (2x) the birds and the bees do Growing up you learn to wait a while (a while) Or take it slow awhile (a while) Cause youre learning from the right role models Cinderella, Snow White you know the rest They made us all wanna choose perfection (Doohoo Doo, ru, doohoohoohoe yeah) Growing up you think youve found the One (the One) But you get hurt over and over again Growing up you learn (you learn) to take control (control) Or hurt them back (2x) or stop this loving thing cause its not for you Then one day you meet this guy He seem so right Are you mad to try again But you do, you try again If people wanna know why this is my answer Chorus: Im just a girl (Im just a girl) Who took a chance (who took a chance) On someone extraordinary A girl (Im just a girl) Who took a chance (who took a chance) On someone extraordinary His eyes make me believe in love His heart made me try again His life made me reach for life Verse Two Waiting all your life (your life) to live your life (life) now youre caught up in group talk (talk) Waiting all your life (your life) to stand up tall (tall) now youre too shy to let them see you (see you) All grown up and it doesnt make sense how they still hold you down (down) And they still make you doubt (doubt) Cinderella, Snow White, you know the rest They might be real but focus on your own love story (Doohoo Doo, ru, doohoohoohoe yeah) You know you wanna tell them about his dreams (dreams) About his visions (visions) and how he made them real (real) You know you wanna tell them amongst these things (things) He still makes you feel like number one But theres one thing, theyll find his flaws Are you mad thats what theyll say But you know and God knows he is human If people wanna know why (why) Im holding on I have to say that Chorus: Im just a girl (Im just a girl) Who took a chance (who took a chance) On someone extraordinary A girl (Im just a girl) Who took a chance (who took a chance) On someone extraordinary His eyes make me believe in love His heart made me try again His life made me reach for life Written by: Christi Warner (2008) Afrochica Entertainment ", 0.0),
      (14L, "Intro Tonight, Im dancing around you babe Tonight, Im dancing around you babe Tonight, Im dancing around you babe Tonight, Im dancing around you babe Pie-yaari, pie-yaari, pie-yaari, Pam pam, pam, pam yeah ah ha ha 4x Verse One Dancing, dancing, all night long yeah yeah Dancing, dancing all night long Ive been thinking That I wanna get down with you The night is young And the beat keeps pumping (pumping) Come on over. Lets get this rhythm going Tell me, how long do you wanna fight me (so come on) Chorus: Pie-yaari, pie-yaari, pie-yaari, Pam pam, pam, pam yeah ah ha ha 4x Verse Two Dancing, dancing, all night long yeah yeah Come on dance with me dance all night long Dont just look at me (me), but come and dance with me (me) If you come over here (here). You could be doing more (more) Then just looking and (and) And, and wishing that (that), That you were dancing here with me So feel my rhythm, Cause I can feel yours I can see you looking, cause Im looking too But we gotta stop this game playing and get it on So come on Chorus: Pie-yaari, pie-yaari, pie-yaari, Pam pam, pam, pam yeah ah ha ha 4x Breakdown Written by: Christi Warner (2008) Afrochica Entertainment ", 0.0),
      (15L, "Verse One: No more castles in the sky No more building on rocks Do you hear the whispers flowing by? Almost feel like angels in the sky Talking about angels: last night I felt an angel by my side And when he spoke? He told me I would be the one for you I knew that was the truth ?cause An angel never lies ? angels never lie Pre-chorus: Ah ha aha aha (aha ha) Ah ha aha aha ? aha aha Chorus: I?ll fly away with you today I?ll fly away with you today Take my hand lets be bold and find eternity 2x Verse Two: The preacher came and we signed our love And they know we are meant to be (oh oh eah yeah) They say our eyes speaks a love so true And I see it too; oh I feel it strong (oh oh eah, yeah) I know I can trust this heart of mine I see true love in your eyes Thanks to you I know the true meaning of love Pre-chorus: Ah ha aha aha (aha ha) Ah ha aha aha ? aha aha Chorus: I?ll fly away with you today I?ll fly away with you today Take my hand lets be bold and find eternity (till fade) Written by: Christi Warner (2004) Ã‚Â© Afrochica Entertainment ", 0.0),
      (16L, "Verse One: Why am I killing myself? Some believe I got the lazy curse Been declared worse you know Even heard them say growing ups a pain Guess they dont understand (2x) Chorus: I got music in my soul Im a slave to the bug Cant escape this world of mine Cause the radio will die 2x Verse Two: Im a poet of society Realitys got me chasing rhymes Late night youll see my lights on I got my flow working over time Guess they dont understand (2x) Chorus: I got music in my soul Im a slave to the bug Cant escape this world of mine Cause the radio will die 2x Verse Three: When Im in the studio I work all day, work all night See time is not the essence Only sometimes its a bother Cause my cash cant keep me flowing, working, performing No, no, no no. Chorus: I got music in my soul Im a slave to the bug Cant escape this world of mine Cause the radio will die (Chorus till fade) Written by: Christi Warner (2008) Afrochica Entertainment ", 0.0),
      (17L, "hadoop software", 0.0)

    )).toDF("id", "text", "label")
*/



    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("multiLine", true)
      .load("E:\\C drive\\NEU\\Scala\\Final\\datasets\\kaggle\\original.csv")// Give correct path here.


    val clean_lyrics = DataCleaner.cleanTrain(df.limit(20000).toDF())
    val clean = DataCleaner.cleanGenre(clean_lyrics)


    val indexer = new StringIndexer().setInputCol("genre").setOutputCol("genre_code")
    val indexed = indexer.fit(clean).transform(clean)
   // indexed.show(false)



    println("--------------------------------------------------------------------------------------------------------------------------------------------------------")
    //clean.show(false)
    println("--------------------------------------------------------------------------------------------------------------------------------------------------------")

    val unknownlyrics = "This is me for forever\nOne of the lost ones\nThe one without a name\nWithout an honest heart as compass\n\nThis is me for forever\nOne without a name\nThese lines the last endeavor\nTo find the missing lifeline\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\n\nMy flower, withered between\nThe pages two and three\nThe once and forever bloom gone with my sins\nWalk the dark path\nSleep with angels\nCall the past for help\nTouch me with your love\nAnd reveal to me my true name\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nNemo sailing home\nNemo letting go\n\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nMy loving heart\nLost in the dark\nFor hope I'd give my everything\nOh how I wish\nFor soothing rain\nAll I wish is to dream again\nOnce and for all\nAnd all for once\nNemo my name forevermore\n\nName for evermore"



    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("clean_lyrics")
      .setOutputCol("tokenized_words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(1).setLabelCol("genre_code")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val reg = new RegressionEvaluator()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(reg.setLabelCol("genre_code"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(indexed)
/*

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (6L, unknownlyrics)
    )).toDF("id", "clean_lyrics")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test)
      .select("id", "clean_lyrics", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
*/


    val cvModelSaveDir = "/tmp/spark-logistic-regression-model"+"/word2vec/" + "/word2vec/cvmodel/"
    cvModel.write.overwrite().save(cvModelSaveDir)

  }


  def testpredict(spark:SparkSession, unknownlyrics: String ) = {
    val test = spark.createDataFrame(Seq(
      (6L, unknownlyrics)
    )).toDF("id", "clean_lyrics")

    val cvModelSaveDir = "/tmp/spark-logistic-regression-model"+"/word2vec/" + "/word2vec/cvmodel/"

    val cvModel = CrossValidatorModel.load("/tmp/spark-logistic-regression-model"+"/word2vec/" + "/word2vec/cvmodel/")
    cvModel.transform(test)
      .select("id", "clean_lyrics", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }
}

